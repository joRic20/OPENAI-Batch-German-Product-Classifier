# build_requests_jsonl.py
# Purpose: Build chunked .jsonl request files for OpenAI Batch API classification (50k per chunk)

import json
import pandas as pd
import math
import os
from dotenv import load_dotenv

load_dotenv()

# ====== CONFIG ======
INPUT_EXCEL = "missing_products_for_reprocessing.xlsx"
OUTPUT_DIR = "batch_chunks"
MODEL = "gpt-4o-mini"
MAX_ROWS = None  # Process all 10,000 rows
CHUNK_SIZE = 50000  # 50k rows per chunk  
BATCH_SIZE = 44  # 10 API calls total
MAX_COMPLETION_TOKENS = 16000  # ← FIXED! (Model's maximum)
# ====================

# Updated system prompt based on our successful test
SYSTEM_PROMPT = """You are an expert German e-commerce product categorization specialist with 15+ years of experience in industrial tools, hardware, and technical equipment classification.

Your task: Classify each item into the most precise professional German product type for e-commerce platforms.

Input: JSON array of {sku, product_title_de}
Rules:
- Use ONLY product_title_de for classification.
- No generic terms like Zubehör, Teil, Maschinenteil, Produkt.
- Output simple, core German product type (e.g., Winkelschleifer, Bohrer, Schutzhaube).
- STRICTLY ignore brand names, sizes, dimensions, quantities, measurements, model numbers.
- Remove ALL technical specifications and material types including:
  * Material descriptors: HSS, HSS-R, HSS-Co, Hartmetall, Diamant, Keramik, Carbide
  * Manufacturing methods: geschliffen, gedreht, gepresst, gerollt
  * Surface treatments: TiN, TiAlN, beschichtet, poliert
  * Technical grades: DIN, ISO, ANSI specifications
  * Performance descriptors: Professional, Premium, Heavy Duty, Extra, Super
  * Numbers: 2-Wege, 3-Wege, 2K-, 6-kant, 4in1, etc.
  * Technical acronyms: HACCP, FAZ, LOCKTIX, etc.
  * Brand codes: Eins.LOGS, ZYLKOschraube, etc.
- Remove ALL size indicators: mm, cm, m, Zoll, x, measurements, diameters.
- Remove functional descriptors like "abgerundet", "ohne Deckblech", "mit Gewinde", "für Winkelschleifer".
- Focus ONLY on the basic tool/product category.
- Use German terms when available, BUT keep established English product type names that are standard in German markets (e.g., Multi-Tool, Impact Driver).
- Avoid English for generic descriptors.

Examples:
- "Metabo HSS-R-Bohrer 19,0 x 198 mm" → "Bohrer"
- "2-Komponentenkleber Professional 50ml" → "Kleber"
- "3-Wege-Kupplung DN 25" → "Kupplung"
- "HACCP-Mantel Größe L" → "Mantel"
- "6-kant-Steckschlüssel-Set" → "Steckschlüssel"
- "Bosch Hartmetall-Trennscheibe 125mm Professional" → "Trennscheibe"
- "Diamant-Sägeblatt Expert 190mm" → "Diamant-Sägeblatt" 
- "Spatmeißel abgerundet 400 x 135 mm" → "Spatmeißel"
- "Schutzhaube ohne Deckblech 100 mm" → "Schutzhaube"
- "Multi-Tool Professional 250W" → "Multi-Tool"
- "HSS-Spiralbohrer-Set DIN 338" → "Spiralbohrer-Set"
- "2K-Epoxydkleber 25ml" → "Kleber"
- "FAZ II Dämpfer M12" → "Dämpfer"
- "Eins.LOGS Holzverbinder" → "Holzverbinder"
- "LOCKTIX-Scheiben M8" → "Scheibe"
- "2-Takt-Motorenöl 1L" → "Motorenöl"
- "H-Filter HEPA 14" → "Filter"

Use your expertise to ensure consistent, professional categorization that serves e-commerce customers effectively.

IMPORTANT: Return ONLY the raw JSON array, no markdown formatting, no code blocks, no extra text.
Format: [{"sku":"<sku>","product_type_de":"<type>"}]
"""

def load_and_prepare_data():
    """Load Excel data and prepare for processing"""
    print("Loading Excel file...")
    df = pd.read_excel(INPUT_EXCEL)
    
    # Handle different title column names
    title_col = None
    possible_title_cols = ["product_title-de", "Product Title (DE)", "product_title_de"]
    for col in possible_title_cols:
        if col in df.columns:
            title_col = col
            break
    
    if title_col is None:
        raise ValueError(f"Could not find title column. Available columns: {list(df.columns)}")
    
    # Check for sku column
    if "sku" not in df.columns:
        raise ValueError(f"Could not find 'sku' column. Available columns: {list(df.columns)}")
    
    # Standardize column names and select only needed columns
    df = df.rename(columns={title_col: "product_title_de"})
    df = df[["sku", "product_title_de"]]
    
    if MAX_ROWS:
        df = df.head(MAX_ROWS)
    
    print(f"Loaded {len(df):,} rows")
    return df

def create_batched_requests(chunk_df, chunk_id):
    """Create batched requests for a chunk of data"""
    requests = []
    total_batches = math.ceil(len(chunk_df) / BATCH_SIZE)
    
    for batch_idx in range(total_batches):
        start_idx = batch_idx * BATCH_SIZE
        end_idx = min(start_idx + BATCH_SIZE, len(chunk_df))
        batch_data = chunk_df.iloc[start_idx:end_idx]
        
        # Create items for this batch
        items = []
        for _, row in batch_data.iterrows():
            items.append({
                "sku": str(row["sku"]),
                "product_title_de": str(row["product_title_de"])
            })
        
        # Create batch request
        request = {
            "custom_id": f"chunk_{chunk_id}_batch_{batch_idx}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": MODEL,
                "temperature": 0.2,
                "max_completion_tokens": MAX_COMPLETION_TOKENS,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": json.dumps(items, ensure_ascii=False)}
                ]
            }
        }
        requests.append(request)
    
    return requests

def main():
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Load data
    df = load_and_prepare_data()
    
    # Calculate number of chunks
    total_chunks = math.ceil(len(df) / CHUNK_SIZE)
    print(f"Will create {total_chunks} chunks of max {CHUNK_SIZE:,} rows each")
    
    # Process each chunk
    for chunk_idx in range(total_chunks):
        start_idx = chunk_idx * CHUNK_SIZE
        end_idx = min(start_idx + CHUNK_SIZE, len(df))
        chunk_df = df.iloc[start_idx:end_idx]
        
        print(f"\nProcessing chunk {chunk_idx + 1}/{total_chunks} ({len(chunk_df):,} rows)")
        
        # Create requests for this chunk
        requests = create_batched_requests(chunk_df, chunk_idx)
        
        # Write JSONL file for this chunk
        output_file = os.path.join(OUTPUT_DIR, f"requests_chunk_{chunk_idx}.jsonl")
        with open(output_file, "w", encoding="utf-8") as f:
            for request in requests:
                f.write(json.dumps(request, ensure_ascii=False) + "\n")
        
        print(f"✅ Wrote {len(requests)} batch requests to {output_file}")
        print(f"   Processing {len(chunk_df):,} items in {len(requests)} API calls")
    
    print(f"\n🎉 Successfully created {total_chunks} chunk files in {OUTPUT_DIR}/")
    print(f"Total rows to process: {len(df):,}")
    
    # Calculate actual API calls
    total_api_calls = 0
    for chunk_idx in range(total_chunks):
        start_idx = chunk_idx * CHUNK_SIZE
        end_idx = min(start_idx + CHUNK_SIZE, len(df))
        chunk_size = end_idx - start_idx
        chunk_api_calls = math.ceil(chunk_size / BATCH_SIZE)
        total_api_calls += chunk_api_calls
    
    print(f"Total API calls across all chunks: {total_api_calls}")

if __name__ == "__main__":
    main()
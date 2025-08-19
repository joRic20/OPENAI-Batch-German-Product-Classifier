# run_batch_and_export.py
# Purpose: Submit OpenAI Batch jobs for chunks, download results, and export to Excel

import json
import pandas as pd
import os
import time
from openai import OpenAI
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# ====== CONFIG ======
BATCH_CHUNKS_DIR = "batch_chunks"
RESULTS_DIR = "batch_results"
EXCEL_OUTPUT_DIR = "excel_outputs"
CLIENT = OpenAI()
POLL_INTERVAL = 300  # Check batch status every 5 minutes
# ====================

def clean_response_content(content):
    """Clean response content using the same method from our successful test"""
    content = content.strip()
    
    # Remove markdown code blocks
    if content.startswith('```json'):
        content = content[7:]
    elif content.startswith('```'):
        content = content[3:]
    
    if content.endswith('```'):
        content = content[:-3]
    
    # Remove any remaining ``` anywhere
    content = content.replace('```', '')
    content = content.strip()
    
    return content

def extract_json_array(content):
    """Extract JSON array using simple string operations"""
    # Find the first [ and last ]
    start_pos = content.find('[')
    end_pos = content.rfind(']')
    
    if start_pos != -1 and end_pos != -1 and start_pos < end_pos:
        json_str = content[start_pos:end_pos + 1]
        return json_str
    return None

def parse_response_content(content, custom_id):
    """Parse response content with robust error handling"""
    try:
        # Clean the content first
        cleaned_content = clean_response_content(content)
        
        if not cleaned_content:
            return [{"sku": f"ERROR_EMPTY_{custom_id}", "product_type_de": "EmptyContent"}]
        
        # Try direct parsing first
        try:
            data = json.loads(cleaned_content)
        except json.JSONDecodeError:
            # Try to extract JSON array using simple string operations
            json_str = extract_json_array(cleaned_content)
            if json_str:
                data = json.loads(json_str)
            else:
                return [{"sku": f"ERROR_NO_JSON_{custom_id}", "product_type_de": "NoJSONFound"}]
        
        if isinstance(data, dict):
            data = [data]
        
        # Validate structure
        valid_data = []
        for item in data:
            if isinstance(item, dict) and "sku" in item and "product_type_de" in item:
                valid_data.append(item)
            else:
                print(f"Invalid item structure in {custom_id}: {item}")
        
        return valid_data if valid_data else [{"sku": f"ERROR_INVALID_{custom_id}", "product_type_de": "InvalidStructure"}]
        
    except Exception as e:
        print(f"Error parsing response for {custom_id}: {e}")
        return [{"sku": f"ERROR_PARSE_{custom_id}", "product_type_de": "ParseError"}]

def submit_batch_job(jsonl_file):
    """Submit a single batch job"""
    print(f"Uploading {jsonl_file}...")
    
    with open(jsonl_file, "rb") as f:
        uploaded = CLIENT.files.create(file=f, purpose="batch")
    
    print(f"📤 Uploaded file ID: {uploaded.id}")
    
    # Create batch job
    batch = CLIENT.batches.create(
        input_file_id=uploaded.id,
        endpoint="/v1/chat/completions",
        completion_window="24h"
    )
    
    print(f"🚀 Batch job created: {batch.id}")
    return batch.id, uploaded.id

def check_batch_status(batch_id):
    """Check the status of a batch job"""
    batch = CLIENT.batches.retrieve(batch_id)
    return batch.status, batch

def download_batch_results(batch):
    """Download and parse batch results"""
    if not batch.output_file_id:
        raise Exception("No output file available")
    
    print(f"📥 Downloading results for batch {batch.id}...")
    
    # Download the results file
    result_file_name = f"results_{batch.id}.jsonl"
    result_file_path = os.path.join(RESULTS_DIR, result_file_name)
    
    content = CLIENT.files.content(batch.output_file_id)
    
    with open(result_file_path, "wb") as f:
        f.write(content.content)
    
    print(f"✅ Downloaded results to {result_file_path}")
    return result_file_path

def process_results_to_excel(results_file, chunk_id):
    """Process results JSONL file and create Excel output"""
    print(f"Processing results file: {results_file}")
    
    rows = []
    with open(results_file, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            try:
                obj = json.loads(line)
                custom_id = obj.get("custom_id", f"unknown_{line_num}")
                
                if "response" in obj and "body" in obj["response"]:
                    response_content = obj["response"]["body"]["choices"][0]["message"]["content"]
                    parsed_data = parse_response_content(response_content, custom_id)
                    rows.extend(parsed_data)
                else:
                    # Handle error responses
                    error_info = obj.get("error", {})
                    rows.append({
                        "sku": f"ERROR_{custom_id}",
                        "product_type_de": f"APIError: {error_info.get('message', 'Unknown error')}"
                    })
                    
            except Exception as e:
                print(f"Error processing line {line_num}: {e}")
                rows.append({
                    "sku": f"ERROR_LINE_{line_num}",
                    "product_type_de": f"LineProcessError: {str(e)}"
                })
    
    # Create DataFrame and save to Excel
    df_out = pd.DataFrame(rows)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(EXCEL_OUTPUT_DIR, f"product_mapping_chunk_{chunk_id}_{timestamp}.xlsx")
    
    df_out.to_excel(output_file, index=False, sheet_name="Product Mapping")
    print(f"✅ Saved {len(df_out):,} rows to {output_file}")
    print(f"   Source: {results_file} → {output_file}")  # Added traceability line
    
    return output_file, len(df_out)

def main():
    # Create output directories
    os.makedirs(RESULTS_DIR, exist_ok=True)
    os.makedirs(EXCEL_OUTPUT_DIR, exist_ok=True)
    
    # Find all chunk files
    chunk_files = [f for f in os.listdir(BATCH_CHUNKS_DIR) if f.endswith('.jsonl')]
    chunk_files.sort()  # Process in order
    
    if not chunk_files:
        print(f"No .jsonl files found in {BATCH_CHUNKS_DIR}")
        return
    
    print(f"Found {len(chunk_files)} chunk files to process")
    
    # Track all batch jobs
    active_batches = {}  # batch_id: (chunk_id, file_id)
    completed_chunks = []
    
    # Submit all batch jobs
    for chunk_file in chunk_files:
        chunk_id = chunk_file.replace('requests_chunk_', '').replace('.jsonl', '')
        file_path = os.path.join(BATCH_CHUNKS_DIR, chunk_file)
        
        try:
            batch_id, file_id = submit_batch_job(file_path)
            active_batches[batch_id] = (chunk_id, file_id)
            print(f"Submitted chunk {chunk_id} as batch {batch_id}")
            time.sleep(2)  # Brief pause between submissions
            
        except Exception as e:
            print(f"Error submitting {chunk_file}: {e}")
    
    print(f"\n🚀 Submitted {len(active_batches)} batch jobs")
    print("Now monitoring for completion...")
    
    # Monitor batch completion
    while active_batches:
        print(f"\n⏳ Checking status of {len(active_batches)} active batches...")
        
        completed_batches = []
        
        for batch_id, (chunk_id, file_id) in active_batches.items():
            try:
                status, batch = check_batch_status(batch_id)
                print(f"Batch {batch_id} (chunk {chunk_id}): {status}")
                
                if status == "completed":
                    completed_batches.append(batch_id)
                    
                    # Download and process results
                    try:
                        results_file = download_batch_results(batch)
                        excel_file, row_count = process_results_to_excel(results_file, chunk_id)
                        completed_chunks.append((chunk_id, excel_file, row_count))
                        print(f"✅ Completed chunk {chunk_id}: {row_count:,} rows -> {excel_file}")
                        
                    except Exception as e:
                        print(f"❌ Error processing results for chunk {chunk_id}: {e}")
                
                elif status == "failed":
                    completed_batches.append(batch_id)
                    print(f"❌ Batch {batch_id} (chunk {chunk_id}) failed")
                    
            except Exception as e:
                print(f"Error checking batch {batch_id}: {e}")
        
        # Remove completed batches from active list
        for batch_id in completed_batches:
            del active_batches[batch_id]
        
        if active_batches:
            print(f"Waiting {POLL_INTERVAL} seconds before next check...")
            time.sleep(POLL_INTERVAL)
    
    # Summary
    print(f"\n🎉 Processing complete!")
    print(f"Completed chunks: {len(completed_chunks)}")
    total_rows = sum(count for _, _, count in completed_chunks)
    print(f"Total rows processed: {total_rows:,}")
    
    print("\nGenerated Excel files:")
    for chunk_id, excel_file, row_count in completed_chunks:
        print(f"  Chunk {chunk_id}: {excel_file} ({row_count:,} rows)")

if __name__ == "__main__":
    main()
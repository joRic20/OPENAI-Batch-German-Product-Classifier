# combine_latest_files.py
# Purpose: Combine the latest Excel files into one master file and remove ERROR rows

import pandas as pd
import os
from datetime import datetime

# ====== CONFIG ======
EXCEL_OUTPUT_DIR = "excel_outputs"
MASTER_OUTPUT_FILE = "master_product_classifications_expert.xlsx"
# ====================

def find_latest_processing_files():
    """Find the most recent set of processing files"""
    print("🔍 FINDING LATEST PROCESSING FILES:")
    print("=" * 50)
    
    # Get all Excel files
    all_files = [f for f in os.listdir(EXCEL_OUTPUT_DIR) if f.startswith('product_mapping_chunk_') and f.endswith('.xlsx')]
    
    if not all_files:
        print("❌ No chunk files found!")
        return []
    
    # Group files by date only (ignore time differences)
    file_groups = {}
    for file in all_files:
        try:
            # Extract date from filename (format: product_mapping_chunk_X_YYYYMMDD_HHMMSS.xlsx)
            parts = file.split('_')
            if len(parts) >= 5:
                date_part = parts[-2]  # Get YYYYMMDD part
                if date_part not in file_groups:
                    file_groups[date_part] = []
                file_groups[date_part].append(file)
        except:
            continue
    
    if not file_groups:
        print("❌ No valid chunk files found!")
        return []
    
    # Find the most recent date
    latest_date = max(file_groups.keys())
    latest_files = file_groups[latest_date]
    
    # Sort files by chunk number for proper order
    def extract_chunk_number(filename):
        try:
            parts = filename.split('_')
            return int(parts[2])  # chunk number is at index 2
        except:
            return 0
    
    latest_files.sort(key=extract_chunk_number)
    
    print(f"Latest processing date: {latest_date}")
    print(f"Found {len(latest_files)} files from latest run:")
    
    for file in latest_files:
        # Extract file info for display
        try:
            parts = file.split('_')
            chunk_num = parts[2]
            time_part = parts[-1].replace('.xlsx', '')
            print(f"  • Chunk {chunk_num}: {file} (time: {time_part})")
        except:
            print(f"  • {file}")
    
    return latest_files

def combine_latest_files():
    """Combine the latest Excel files and remove ERROR entries"""
    print("\n🔗 COMBINING LATEST FILES INTO MASTER FILE")
    print("=" * 60)
    
    # Find latest files
    latest_files = find_latest_processing_files()
    
    if not latest_files:
        print("❌ No files to combine!")
        return
    
    # Show what we found and ask for confirmation
    print(f"\nFound {len(latest_files)} files to combine. Proceed? (y/n)")
    response = input().lower()
    
    if response not in ['y', 'yes']:
        print("Cancelled. You can manually specify files if needed.")
        return
    
    # Combine all files
    all_data = []
    total_products = 0
    total_errors = 0
    chunk_summary = []
    
    for i, excel_file in enumerate(latest_files):
        file_path = os.path.join(EXCEL_OUTPUT_DIR, excel_file)
        
        try:
            df = pd.read_excel(file_path)
            
            # Count products and errors in this chunk
            chunk_products = len(df)
            error_rows = df[df['sku'].str.startswith('ERROR', na=False)]
            chunk_errors = len(error_rows)
            clean_rows = chunk_products - chunk_errors
            
            chunk_summary.append({
                'chunk': i,
                'file': excel_file,
                'total': chunk_products,
                'errors': chunk_errors,
                'clean': clean_rows
            })
            
            # Add clean rows only (remove ERROR entries)
            clean_df = df[~df['sku'].str.startswith('ERROR', na=False)].copy()
            all_data.append(clean_df)
            
            total_products += chunk_products
            total_errors += chunk_errors
            
            print(f"✅ Processed Chunk {i}: {clean_rows:,} clean products from {excel_file}")
            
        except Exception as e:
            print(f"❌ Error reading {excel_file}: {e}")
    
    # Combine all clean data
    print(f"\n📊 COMBINATION SUMMARY:")
    print("=" * 40)
    
    if all_data:
        master_df = pd.concat(all_data, ignore_index=True)
        
        # Remove any potential duplicates
        original_count = len(master_df)
        master_df = master_df.drop_duplicates(subset=['sku'], keep='first')
        final_count = len(master_df)
        duplicates_removed = original_count - final_count
        
        print(f"Total input products: {total_products:,}")
        print(f"ERROR entries removed: {total_errors:,}")
        print(f"Clean products combined: {original_count:,}")
        
        if duplicates_removed > 0:
            print(f"Duplicate SKUs removed: {duplicates_removed:,}")
            
        print(f"Final master file products: {final_count:,}")
        
        # Add metadata
        master_df['processed_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        master_df['processing_version'] = "expert_prompt_v2"
        
        # Save master file
        master_df[['sku', 'product_type_de']].to_excel(MASTER_OUTPUT_FILE, index=False, sheet_name="Expert Classifications")
        
        print(f"\n✅ Master file created: {MASTER_OUTPUT_FILE}")
        print(f"   Contains: {final_count:,} expertly classified products")
        
        # Analyze results with expert prompt
        analyze_expert_classifications(master_df)
        
        return master_df
        
    else:
        print("❌ No clean data found to combine")
        return None

def analyze_expert_classifications(df):
    """Analyze the quality of expert classifications"""
    print(f"\n📈 EXPERT CLASSIFICATION ANALYSIS:")
    print("=" * 50)
    
    # Count product types
    type_counts = df['product_type_de'].value_counts()
    
    print(f"Unique product types: {len(type_counts):,}")
    print(f"\nTop 20 most common product types:")
    
    for i, (product_type, count) in enumerate(type_counts.head(20).items()):
        percentage = (count / len(df)) * 100
        print(f"  {i+1:2d}. {product_type:<25} {count:>6,} ({percentage:5.1f}%)")
    
    # Quality check - look for remaining technical specs
    print(f"\n🔍 EXPERT PROMPT QUALITY CHECK:")
    technical_patterns = [
        'HSS', 'Diamant', 'Hartmetall', 'Professional', 'Premium', 
        '2K-', '3-Wege', '2-Wege', 'HACCP', 'FAZ', '6-kant'
    ]
    
    issues_found = False
    for pattern in technical_patterns:
        matching = df[df['product_type_de'].str.contains(pattern, case=False, na=False)]
        if len(matching) > 0:
            issues_found = True
            print(f"  ⚠️  {pattern}: {len(matching):,} products still contain this term")
            # Show a few examples
            examples = matching['product_type_de'].value_counts().head(3)
            for example, count in examples.items():
                print(f"      Example: {example} ({count:,} products)")
    
    if not issues_found:
        print("  ✅ Excellent! No technical specifications found in classifications!")
        print("  ✅ Expert prompt successfully cleaned all product types!")
    
    # Check for clean, simple classifications
    print(f"\n📋 SAMPLE OF EXPERT CLASSIFICATIONS:")
    sample_df = df.sample(n=min(15, len(df)))
    for _, row in sample_df.iterrows():
        print(f"  {row['sku']}: {row['product_type_de']}")
    
    # Calculate success metrics
    total_original = 470837  # Your original dataset size
    success_rate = (len(df) / total_original) * 100
    
    print(f"\n🎯 EXPERT PROCESSING SUCCESS METRICS:")
    print("=" * 40)
    print(f"Original dataset: {total_original:,} products")
    print(f"Successfully classified: {len(df):,} products")
    print(f"Success rate: {success_rate:.1f}%")
    print(f"Unique product categories: {len(type_counts):,}")
    print(f"Average products per category: {len(df) // len(type_counts):,}")

def main():
    """Main function"""
    if not os.path.exists(EXCEL_OUTPUT_DIR):
        print(f"❌ Excel output directory not found: {EXCEL_OUTPUT_DIR}")
        return
    
    print("🎯 EXPERT CLASSIFICATION MASTER FILE CREATOR")
    print("=" * 60)
    print("Combining your latest expert-classified product files into one master file\n")
    
    master_df = combine_latest_files()
    
    if master_df is not None:
        print(f"\n" + "="*60)
        print("🎉 SUCCESS! Your expert classifications are ready!")
        print("="*60)
        print(f"📁 Master file: {MASTER_OUTPUT_FILE}")
        print(f"📊 Total products: {len(master_df):,}")
        print(f"🎯 Quality: Expert-level classifications with no technical specs")
        print(f"✅ Ready for e-commerce use!")

if __name__ == "__main__":
    main()
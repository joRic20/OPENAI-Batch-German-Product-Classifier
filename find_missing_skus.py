# find_missing_skus.py
# Purpose: Find SKUs that weren't processed and create a new dataset for reprocessing

import pandas as pd
import os

# ====== CONFIG ======
ORIGINAL_EXCEL = "dataset.xlsx"
EXCEL_OUTPUT_DIR = "excel_outputs"
MISSING_PRODUCTS_FILE = "missing_products_for_reprocessing.xlsx"
# ====================

def find_missing_products():
    """Find products that weren't processed and create file for reprocessing"""
    print("🔍 FINDING MISSING PRODUCTS FOR REPROCESSING")
    print("=" * 60)
    
    # Load original dataset
    print("Loading original dataset...")
    original_df = pd.read_excel(ORIGINAL_EXCEL)
    original_skus = set(original_df['sku'].astype(str))
    print(f"Original dataset: {len(original_df):,} products")
    print(f"Unique SKUs: {len(original_skus):,}")
    
    # Collect all processed SKUs from Excel outputs
    print("\nCollecting processed SKUs from output files...")
    processed_skus = set()
    
    excel_files = [f for f in os.listdir(EXCEL_OUTPUT_DIR) if f.startswith('product_mapping_chunk_') and f.endswith('.xlsx')]
    excel_files.sort()
    
    total_processed = 0
    for excel_file in excel_files:
        file_path = os.path.join(EXCEL_OUTPUT_DIR, excel_file)
        df = pd.read_excel(file_path)
        
        # Get SKUs from this chunk (convert to string for consistency)
        chunk_skus = set(df['sku'].astype(str))
        processed_skus.update(chunk_skus)
        
        chunk_count = len(df)
        total_processed += chunk_count
        
        print(f"  {excel_file}: {chunk_count:,} products")
    
    print(f"\nTotal processed: {total_processed:,} products")
    print(f"Unique processed SKUs: {len(processed_skus):,}")
    
    # Find missing SKUs
    missing_skus = original_skus - processed_skus
    print(f"\nMissing SKUs: {len(missing_skus):,}")
    
    if len(missing_skus) == 0:
        print("🎉 No missing products found! All SKUs were processed.")
        return
    
    # Create dataset with missing products
    print(f"\nCreating reprocessing dataset with {len(missing_skus):,} missing products...")
    
    # Filter original dataset to only missing SKUs
    missing_df = original_df[original_df['sku'].astype(str).isin(missing_skus)].copy()
    
    # Save to new Excel file
    missing_df.to_excel(MISSING_PRODUCTS_FILE, index=False)
    
    print(f"✅ Created: {MISSING_PRODUCTS_FILE}")
    print(f"   Contains: {len(missing_df):,} products to reprocess")
    
    # Show sample of missing products
    print(f"\nSample of missing products:")
    print(missing_df.head()[['sku', missing_df.columns[1]]].to_string(index=False))
    
    # Quick analysis
    missing_percentage = (len(missing_skus) / len(original_skus)) * 100
    print(f"\n📊 SUMMARY:")
    print(f"Original products: {len(original_df):,}")
    print(f"Successfully processed: {len(processed_skus):,}")
    print(f"Missing products: {len(missing_skus):,} ({missing_percentage:.1f}%)")
    
    # Check for any duplicate processing
    if total_processed > len(processed_skus):
        duplicates = total_processed - len(processed_skus)
        print(f"Duplicate processing detected: {duplicates:,} SKUs processed multiple times")
    
    print(f"\n🎯 NEXT STEPS:")
    print(f"1. Update build_requests_jsonl.py to use: INPUT_EXCEL = '{MISSING_PRODUCTS_FILE}'")
    print(f"2. Set MAX_ROWS = None (to process all missing products)")
    print(f"3. Run: python build_requests_jsonl.py")
    print(f"4. Run: python run_batch_and_export.py")
    print(f"5. You'll get additional Excel files with the missing {len(missing_skus):,} products!")

def check_for_duplicates_in_output():
    """Check if any SKUs were processed multiple times"""
    print("\n🔍 CHECKING FOR DUPLICATE PROCESSING:")
    print("=" * 50)
    
    all_processed_skus = []
    
    excel_files = [f for f in os.listdir(EXCEL_OUTPUT_DIR) if f.startswith('product_mapping_chunk_') and f.endswith('.xlsx')]
    
    for excel_file in excel_files:
        file_path = os.path.join(EXCEL_OUTPUT_DIR, excel_file)
        df = pd.read_excel(file_path)
        all_processed_skus.extend(df['sku'].astype(str).tolist())
    
    # Check for duplicates
    from collections import Counter
    sku_counts = Counter(all_processed_skus)
    duplicates = {sku: count for sku, count in sku_counts.items() if count > 1}
    
    if duplicates:
        print(f"Found {len(duplicates)} SKUs processed multiple times:")
        for sku, count in list(duplicates.items())[:10]:  # Show first 10
            print(f"  {sku}: processed {count} times")
        if len(duplicates) > 10:
            print(f"  ... and {len(duplicates) - 10} more")
    else:
        print("✅ No duplicate processing found - each SKU processed exactly once")

def main():
    """Main function"""
    if not os.path.exists(ORIGINAL_EXCEL):
        print(f"❌ Original Excel file not found: {ORIGINAL_EXCEL}")
        return
    
    if not os.path.exists(EXCEL_OUTPUT_DIR):
        print(f"❌ Excel output directory not found: {EXCEL_OUTPUT_DIR}")
        return
    
    find_missing_products()
    check_for_duplicates_in_output()

if __name__ == "__main__":
    main()
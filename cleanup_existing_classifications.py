# find_remaining_missing_skus.py
# Purpose: Find SKUs missing from the latest expert classification run and create file for reprocessing

import pandas as pd
import os

# ====== CONFIG ======
ORIGINAL_EXCEL = "dataset_product_type.xlsx"
MASTER_FILE = "master_product_classifications_expert.xlsx"
MISSING_PRODUCTS_FILE = "remaining_missing_products_expert.xlsx"
# ====================

def find_remaining_missing_products():
    """Find products that weren't processed in the latest expert run"""
    print("🔍 FINDING REMAINING MISSING PRODUCTS AFTER EXPERT PROCESSING")
    print("=" * 70)
    
    # Check if files exist
    if not os.path.exists(ORIGINAL_EXCEL):
        print(f"❌ Original Excel file not found: {ORIGINAL_EXCEL}")
        return
    
    if not os.path.exists(MASTER_FILE):
        print(f"❌ Master file not found: {MASTER_FILE}")
        print("Please run combine_latest_files.py first!")
        return
    
    # Load original dataset
    print("Loading original dataset...")
    original_df = pd.read_excel(ORIGINAL_EXCEL)
    original_skus = set(original_df['sku'].astype(str))
    print(f"Original dataset: {len(original_df):,} products")
    print(f"Unique SKUs: {len(original_skus):,}")
    
    # Load master classified file
    print("\nLoading expert-classified master file...")
    master_df = pd.read_excel(MASTER_FILE)
    processed_skus = set(master_df['sku'].astype(str))
    print(f"Expert-classified products: {len(master_df):,}")
    print(f"Unique processed SKUs: {len(processed_skus):,}")
    
    # Find missing SKUs
    missing_skus = original_skus - processed_skus
    print(f"\nRemaining missing SKUs: {len(missing_skus):,}")
    
    if len(missing_skus) == 0:
        print("🎉 PERFECT! No missing products found!")
        print("✅ All SKUs from your original dataset have been expertly classified!")
        print("🎯 Your product classification project is 100% complete!")
        return
    
    # Create dataset with remaining missing products
    print(f"\nCreating reprocessing dataset with {len(missing_skus):,} remaining products...")
    
    # Filter original dataset to only missing SKUs
    missing_df = original_df[original_df['sku'].astype(str).isin(missing_skus)].copy()
    
    # Save to new Excel file
    missing_df.to_excel(MISSING_PRODUCTS_FILE, index=False)
    
    print(f"✅ Created: {MISSING_PRODUCTS_FILE}")
    print(f"   Contains: {len(missing_df):,} products to reprocess with expert prompt")
    
    # Show sample of missing products
    print(f"\nSample of remaining missing products:")
    sample_cols = ['sku']
    # Add title column (handle different possible names)
    title_col = None
    possible_title_cols = ["product_title-de", "Product Title (DE)", "product_title_de"]
    for col in possible_title_cols:
        if col in missing_df.columns:
            title_col = col
            break
    
    if title_col:
        sample_cols.append(title_col)
        print(missing_df.head(10)[sample_cols].to_string(index=False))
    else:
        print(missing_df.head(10)[['sku']].to_string(index=False))
    
    # Calculate completion percentage
    completion_percentage = (len(processed_skus) / len(original_skus)) * 100
    missing_percentage = (len(missing_skus) / len(original_skus)) * 100
    
    print(f"\n📊 EXPERT PROCESSING COMPLETION ANALYSIS:")
    print("=" * 50)
    print(f"Original products: {len(original_df):,}")
    print(f"Successfully classified: {len(processed_skus):,} ({completion_percentage:.1f}%)")
    print(f"Remaining missing: {len(missing_skus):,} ({missing_percentage:.1f}%)")
    
    # Estimate processing for remaining
    estimated_chunks = max(1, len(missing_skus) // 50000)
    estimated_api_calls = (len(missing_skus) // 200) + 1  # Using batch size 200
    estimated_cost = estimated_api_calls * 0.003  # Rough estimate
    
    print(f"\n🎯 REPROCESSING ESTIMATES:")
    print("=" * 30)
    print(f"Expected chunks: {estimated_chunks}")
    print(f"Estimated API calls: {estimated_api_calls:,}")
    print(f"Estimated cost: ${estimated_cost:.2f}")
    print(f"Estimated time: {estimated_api_calls // 100} hours")
    
    print(f"\n🚀 NEXT STEPS TO COMPLETE 100%:")
    print("=" * 40)
    print("1. Update build_requests_jsonl.py:")
    print(f"   INPUT_EXCEL = '{MISSING_PRODUCTS_FILE}'")
    print("   MAX_ROWS = None")
    print("   BATCH_SIZE = 200  # Safe for remaining products")
    print("   MAX_COMPLETION_TOKENS = 16000")
    print()
    print("2. Run: python build_requests_jsonl.py")
    print("3. Run: python run_batch_and_export.py") 
    print("4. Combine with your master file for 100% completion!")

def analyze_what_was_missed():
    """Analyze patterns in what products were missed"""
    print("\n🔍 ANALYZING WHAT TYPES OF PRODUCTS WERE MISSED:")
    print("=" * 55)
    
    if not os.path.exists(MISSING_PRODUCTS_FILE):
        return
    
    try:
        missing_df = pd.read_excel(MISSING_PRODUCTS_FILE)
        
        # Find title column
        title_col = None
        possible_title_cols = ["product_title-de", "Product Title (DE)", "product_title_de"]
        for col in possible_title_cols:
            if col in missing_df.columns:
                title_col = col
                break
        
        if title_col:
            # Analyze patterns in missing product titles
            titles = missing_df[title_col].astype(str)
            
            # Look for common words in missing products
            all_words = []
            for title in titles.head(1000):  # Sample first 1000
                words = title.split()
                all_words.extend([word for word in words if len(word) > 3])
            
            from collections import Counter
            word_counts = Counter(all_words)
            
            print("Most common words in missing products:")
            for word, count in word_counts.most_common(15):
                print(f"  {word}: {count} occurrences")
            
            print(f"\nThis can help identify if certain product types were systematically missed.")
        
    except Exception as e:
        print(f"Could not analyze missing products: {e}")

def check_for_duplicates_in_master():
    """Check if master file has any duplicate SKUs"""
    print("\n🔍 CHECKING MASTER FILE FOR QUALITY ISSUES:")
    print("=" * 50)
    
    if not os.path.exists(MASTER_FILE):
        return
    
    try:
        master_df = pd.read_excel(MASTER_FILE)
        
        # Check for duplicates
        duplicate_skus = master_df['sku'].duplicated().sum()
        if duplicate_skus > 0:
            print(f"⚠️  Found {duplicate_skus} duplicate SKUs in master file")
            print("   Consider removing duplicates for clean data")
        else:
            print("✅ No duplicate SKUs found in master file")
        
        # Check for empty classifications
        empty_classifications = master_df['product_type_de'].isna().sum()
        if empty_classifications > 0:
            print(f"⚠️  Found {empty_classifications} empty classifications")
        else:
            print("✅ No empty classifications found")
        
        # Check for ERROR entries that might have slipped through
        error_entries = master_df[master_df['sku'].str.startswith('ERROR', na=False)]
        if len(error_entries) > 0:
            print(f"⚠️  Found {len(error_entries)} ERROR entries in master file")
        else:
            print("✅ No ERROR entries found in master file")
            
    except Exception as e:
        print(f"Could not analyze master file: {e}")

def main():
    """Main function"""
    print("🎯 EXPERT CLASSIFICATION COMPLETION CHECKER")
    print("=" * 60)
    print("Finding any remaining products that need expert classification\n")
    
    find_remaining_missing_products()
    check_for_duplicates_in_master()
    analyze_what_was_missed()

if __name__ == "__main__":
    main()
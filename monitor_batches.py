# monitor_batches.py
# Purpose: Monitor the status of running OpenAI batch jobs

import time
from openai import OpenAI
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

CLIENT = OpenAI()

def monitor_all_batches():
    """Monitor all batch jobs"""
    print("Fetching all batch jobs...")
    
    # Get all batches
    batches = CLIENT.batches.list(limit=100)
    
    if not batches.data:
        print("No batch jobs found")
        return
    
    print(f"Found {len(batches.data)} batch jobs:\n")
    
    # Group by status
    status_groups = {}
    for batch in batches.data:
        status = batch.status
        if status not in status_groups:
            status_groups[status] = []
        status_groups[status].append(batch)
    
    # Display by status
    for status, batch_list in status_groups.items():
        print(f"📊 {status.upper()}: {len(batch_list)} jobs")
        
        for batch in batch_list:
            created = datetime.fromtimestamp(batch.created_at).strftime("%Y-%m-%d %H:%M:%S")
            
            if hasattr(batch, 'request_counts') and batch.request_counts:
                completed = batch.request_counts.completed or 0
                failed = batch.request_counts.failed or 0
                total = batch.request_counts.total or 0
                progress = f"({completed}/{total} completed, {failed} failed)"
            else:
                progress = "(progress unknown)"
            
            print(f"  • {batch.id} - Created: {created} {progress}")
        
        print()

def monitor_specific_batches(batch_ids):
    """Monitor specific batch jobs"""
    print(f"Monitoring {len(batch_ids)} specific batches:\n")
    
    for batch_id in batch_ids:
        try:
            batch = CLIENT.batches.retrieve(batch_id)
            created = datetime.fromtimestamp(batch.created_at).strftime("%Y-%m-%d %H:%M:%S")
            
            if hasattr(batch, 'request_counts') and batch.request_counts:
                completed = batch.request_counts.completed or 0
                failed = batch.request_counts.failed or 0
                total = batch.request_counts.total or 0
                progress_pct = (completed / total * 100) if total > 0 else 0
                
                print(f"📋 {batch_id}")
                print(f"   Status: {batch.status}")
                print(f"   Created: {created}")
                print(f"   Progress: {completed}/{total} ({progress_pct:.1f}%)")
                print(f"   Failed: {failed}")
                
                if batch.status == "completed" and batch.output_file_id:
                    print(f"   Output file: {batch.output_file_id}")
                elif batch.status == "failed" and hasattr(batch, 'errors'):
                    print(f"   Errors: {batch.errors}")
                
            else:
                print(f"📋 {batch_id}: {batch.status} (no progress info)")
                
            print()
            
        except Exception as e:
            print(f"❌ Error checking batch {batch_id}: {e}\n")

def main():
    import sys
    
    if len(sys.argv) > 1:
        # Monitor specific batch IDs provided as arguments
        batch_ids = sys.argv[1:]
        monitor_specific_batches(batch_ids)
    else:
        # Monitor all batches
        monitor_all_batches()

if __name__ == "__main__":
    main()
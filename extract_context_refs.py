"""
Extract unique context_refs from all XBRL files in the NSE dataset
Saves all unique context_refs to a single file
"""

import requests
import pandas as pd
from lxml import etree
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random
from xbr_data_extract import extract_shareholding_pct

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Thread-safe set for unique context_refs
unique_context_refs = set()
import threading
refs_lock = threading.Lock()

def process_single_xbrl(args):
    """
    Process a single XBRL file and extract context_refs
    """
    idx, row = args
    symbol = row['symbol']
    xbrl_url = row['xbrl']
    
    try:
        # Extract context_refs from XBRL
        context_refs = extract_shareholding_pct(xbrl_url)
        
        # Add to global set (thread-safe)
        with refs_lock:
            unique_context_refs.update(context_refs)
        
        print(f"✓ {symbol}: {len(context_refs)} context_refs extracted")
        return len(context_refs)
        
    except Exception as e:
        print(f"✗ {symbol}: {type(e).__name__}")
        return 0


def extract_all_context_refs(csv_path, output_file="unique_context_refs.txt", max_workers=5, limit=None):
    """
    Extract all unique context_refs from XBRL files
    
    Args:
        csv_path (str): Path to NSE XBR data CSV
        output_file (str): File to save unique context_refs
        max_workers (int): Number of threads
        limit (int): Limit records for testing
    """
    
    # Read CSV
    df = pd.read_csv(csv_path)
    if limit:
        df = df.head(limit)
    
    print(f"Extracting context_refs from {len(df)} XBRL files with {max_workers} threads...\n")
    
    # Create tasks
    tasks = [(idx, row) for idx, row in df.iterrows()]
    
    # Process with ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_single_xbrl, task) for task in tasks]
        
        completed = 0
        for future in as_completed(futures):
            try:
                future.result()
                completed += 1
                if completed % 50 == 0:
                    print(f"[Progress] Completed {completed}/{len(tasks)}")
            except Exception as e:
                print(f"Error: {str(e)}")
    
    print(f"\nFinished! Found {len(unique_context_refs)} unique context_refs")
    
    # Save to file
    with open(output_file, 'w') as f:
        for ref in sorted(unique_context_refs):
            f.write(f"{ref}\n")
    
    print(f"Saved to {output_file}")
    return unique_context_refs


if __name__ == "__main__":
    csv_path = "nse_xbr_data.csv"
    output_file = "unique_context_refs.txt"
    
    # Extract context_refs
    refs = extract_all_context_refs(
        csv_path=csv_path,
        output_file=output_file,
        max_workers=5,
        limit=None  # Set to None for full dataset
    )
    
    print(f"\nSample of context_refs:")
    print(sorted(list(refs))[:10])

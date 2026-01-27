import requests
import xml.etree.ElementTree as ET
import pandas as pd
from lxml import etree
import urllib3
import time
from xbr_data_extract import extract_shareholding_pct
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure session with retries
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def create_session():
    """Create a requests session with retry strategy"""
    session = requests.Session()
    
    # Retry strategy: retry on timeout and connection errors
    retry_strategy = Retry(
        total=3,  # Total retries
        backoff_factor=1,  # Wait 1, 2, 4 seconds between retries
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

# Thread-safe lock for appending results
results_lock = threading.Lock()

def process_single_record(args):
    """
    Process a single record (worker function for threading)
    
    Args:
        args: tuple of (idx, row_dict)
        
    Returns:
        dict: Result containing symbol and extracted data
    """
    idx, row = args
    xbrl_url = row['xbrl']
    symbol = row['symbol']
    
    # Add random delay to avoid hammering server
    import random
    time.sleep(random.uniform(0.5, 1.5))
    
    try:
        # Extract shareholding data
        records = extract_shareholding_pct(xbrl_url)
        result = {
            'symbol': symbol,
            'context_refs': str(records),
            'num_categories': len(records)
        }
        print(f"✓ {symbol}: {len(records)} distinct contextRef found")
    except requests.exceptions.ConnectionError as e:
        print(f"✗ {symbol}: Connection error - {str(e)[:50]}")
        result = {
            'symbol': symbol,
            'context_refs': 'CONNECTION_ERROR',
            'num_categories': -1
        }
    except requests.exceptions.Timeout as e:
        print(f"✗ {symbol}: Timeout")
        result = {
            'symbol': symbol,
            'context_refs': 'TIMEOUT',
            'num_categories': -1
        }
    except Exception as e:
        print(f"✗ {symbol}: {type(e).__name__} - {str(e)[:50]}")
        result = {
            'symbol': symbol,
            'context_refs': f'{type(e).__name__}',
            'num_categories': 0
        }
    
    return result

def process_nse_xbr_data(csv_path, output_path=None, context_refs_output=None, limit=None, max_workers=10):
    """
    Process NSE XBR data CSV and extract shareholding percentages using threading
    
    Args:
        csv_path (str): Path to the CSV file
        output_path (str): Path to save the output CSV (optional)
        context_refs_output (str): Path to save unique context_refs (optional)
        limit (int): Limit number of records to process (optional, for testing)
        max_workers (int): Maximum number of threads (default: 10)
        
    Returns:
        tuple: (DataFrame with extracted data, set of unique context_refs)
    """
    
    # Read the CSV
    df = pd.read_csv(csv_path)
    
    # Limit records if specified (useful for testing)
    if limit:
        df = df.head(limit)
    
    print(f"Processing {len(df)} records with {max_workers} threads...\n")
    
    # Initialize lists to store results
    shareholding_results = []
    
    # Create list of tasks
    tasks = [(idx, row) for idx, row in df.iterrows()]
    
    # Process with ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks and get futures
        futures = [executor.submit(process_single_record, task) for task in tasks]
        
        # Collect results as they complete
        completed = 0
        for future in as_completed(futures):
            try:
                result = future.result()
                shareholding_results.append(result)
                completed += 1
                if completed % 10 == 0:
                    print(f"[Progress] Completed {completed}/{len(tasks)} records")
            except Exception as e:
                print(f"Error in thread: {str(e)}")
    
    print(f"\nFinished processing all records!")
    
    # Create DataFrame from shareholding results
    shareholding_df = pd.DataFrame(shareholding_results)
    
    # Merge with original data (maintaining order)
    shareholding_df = shareholding_df.set_index('symbol')
    df_indexed = df.set_index('symbol')
    result_df = df_indexed.join(shareholding_df).reset_index()
    
    # Save results if output path provided
    if output_path:
        result_df.to_csv(output_path, index=False)
        print(f"Data saved to {output_path}")
    
    # Extract and save unique context_refs
    unique_context_refs = set()
    for ctx_str in shareholding_df['context_refs']:
        if ctx_str and ctx_str not in ['CONNECTION_ERROR', 'TIMEOUT']:
            try:
                # Parse the string representation of list
                import ast
                ctx_list = ast.literal_eval(ctx_str)
                if isinstance(ctx_list, list):
                    unique_context_refs.update(ctx_list)
            except:
                pass
    
    # Save unique context_refs to file
    if context_refs_output:
        with open(context_refs_output, 'w') as f:
            for ctx_ref in sorted(unique_context_refs):
                f.write(f"{ctx_ref}\n")
        print(f"Unique context_refs saved to {context_refs_output} ({len(unique_context_refs)} total)")
    
    return result_df, unique_context_refs


if __name__ == "__main__":
    # Path to the CSV file
    csv_path = "nse_xbr_data_sample.csv"
    
    # Output paths
    output_path = "shareholding_extracted.csv"
    context_refs_output = "unique_context_refs.txt"
    
    # Number of worker threads
    # Use 5 threads for faster processing (balanced speed and server load)
    # Each thread includes random delays between requests
    num_threads = 5
    
    # Process all records
    result_df, unique_refs = process_nse_xbr_data(
        csv_path, 
        output_path=output_path, 
        context_refs_output=context_refs_output,
        limit=None, 
        max_workers=num_threads
    )
    
    # Display sample
    print("\nSample of extracted data:")
    print(result_df[['symbol', 'context_refs', 'num_categories']].head(10))
    
    # Display unique context refs
    print(f"\n{len(unique_refs)} unique context_refs found:")
    print(sorted(list(unique_refs))[:10], "...")  # Show first 10

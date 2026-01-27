"""
Fetch shareholding data points from XBRL files using specific context_refs
Groups and extracts data for specified context references
"""

import requests
import pandas as pd
from lxml import etree
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_session_with_retries():
    """Create a session with connection pooling and retry strategy"""
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    
    session = requests.Session()
    
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=10
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session


def fetch_data_from_xbrl(xbrl_url, context_refs):
    """
    Fetch shareholding data from XBRL file for specific context_refs
    
    Args:
        xbrl_url (str): URL to XBRL file
        context_refs (list): List of context_refs to extract data for
        
    Returns:
        dict: Data for each context_ref
    """
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/xml, text/xml, */*',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive'
    }
    
    try:
        session = get_session_with_retries()
        response = session.get(xbrl_url, headers=headers, timeout=60, verify=False)
        response.raise_for_status()
        
        xml = response.content
        root = etree.fromstring(xml)
        
        # Extract data for specified context_refs
        data = {}
        
        for n in root.xpath("//*[local-name()='ShareholdingAsAPercentageOfTotalNumberOfShares']"):
            ctx = n.attrib.get("contextRef")
            
            # Only extract if context_ref is in our list
            if ctx in context_refs:
                value = float(n.text) if n.text else None
                data[ctx] = value
        
        return data
    
    except Exception as e:
        raise Exception(f"Error fetching from {xbrl_url}: {type(e).__name__}: {str(e)}")


def process_stock_for_context_refs(args):
    """
    Process a single stock for specific context_refs
    """
    idx, row, context_refs = args
    symbol = row['symbol']
    xbrl_url = row['xbrl']
    
    # Add random delay
    time.sleep(random.uniform(0.5, 1.5))
    
    try:
        data = fetch_data_from_xbrl(xbrl_url, context_refs)
        result = {
            'symbol': symbol,
            'status': 'success',
            'data': data
        }
        print(f"✓ {symbol}: {len(data)} data points found")
        return result
        
    except Exception as e:
        print(f"✗ {symbol}: {type(e).__name__}")
        result = {
            'symbol': symbol,
            'status': 'error',
            'error': str(e),
            'data': {}
        }
        return result


def fetch_data_by_context_refs(csv_path, context_refs, output_file=None, max_workers=5, limit=None):
    """
    Fetch shareholding data for specific context_refs from all stocks
    
    Args:
        csv_path (str): Path to NSE XBR data CSV
        context_refs (list): List of context_refs to extract
        output_file (str): File to save results (CSV)
        max_workers (int): Number of threads
        limit (int): Limit records for testing
        
    Returns:
        DataFrame: Data for all stocks and specified context_refs
    """
    
    # Read CSV
    df = pd.read_csv(csv_path)
    if limit:
        df = df.head(limit)
    
    print(f"Fetching {len(context_refs)} context_refs from {len(df)} stocks with {max_workers} threads...\n")
    print(f"Context_refs: {context_refs}\n")
    
    # Create tasks
    tasks = [(idx, row, context_refs) for idx, row in df.iterrows()]
    
    all_results = []
    
    # Process with ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_stock_for_context_refs, task) for task in tasks]
        
        completed = 0
        for future in as_completed(futures):
            try:
                result = future.result()
                all_results.append(result)
                completed += 1
                if completed % 50 == 0:
                    print(f"[Progress] Completed {completed}/{len(tasks)}")
            except Exception as e:
                print(f"Error: {str(e)}")
    
    print(f"\nFinished!")
    
    # Create DataFrame
    result_data = []
    for result in all_results:
        row = {'symbol': result['symbol'], 'status': result['status']}
        if result['status'] == 'success':
            row.update(result['data'])
        else:
            row['error'] = result.get('error', '')
        result_data.append(row)
    
    result_df = pd.DataFrame(result_data)
    
    # Save if output file specified
    if output_file:
        result_df.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")
    
    return result_df


def load_context_refs_from_file(file_path):
    """Load context_refs from file"""
    with open(file_path, 'r') as f:
        refs = [line.strip() for line in f.readlines() if line.strip()]
    return refs


if __name__ == "__main__":
    csv_path = "nse_xbr_data_sample.csv"
    context_refs_file = "unique_context_refs.txt"
    output_file = "shareholding_data_by_context_refs.csv"
    
    # Load all unique context_refs (or specify manually)
    # For testing, you can specify a subset:
    # context_refs = ["CY2024Q4_Consolidated", "CY2024Q4_Standalone"]
    
    # Or load from file:
    try:
        all_refs = load_context_refs_from_file(context_refs_file)
        print(f"Loaded {len(all_refs)} context_refs from {context_refs_file}")
        
        # For demo, use first 5 context_refs
        context_refs = all_refs[:5]
        print(f"Using context_refs: {context_refs}\n")
        
    except FileNotFoundError:
        print(f"Context refs file not found. Run extract_context_refs.py first")
        exit(1)
    
    # Fetch data
    result_df = fetch_data_by_context_refs(
        csv_path=csv_path,
        context_refs=context_refs,
        output_file=output_file,
        max_workers=5,
        limit=None
    )
    
    print("\nSample of fetched data:")
    print(result_df.head())

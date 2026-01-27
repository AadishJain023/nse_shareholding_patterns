import requests
import xml.etree.ElementTree as ET
from lxml import etree
import urllib3
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_session_with_retries():
    """Create a session with connection pooling and retry strategy"""
    session = requests.Session()
    
    retry_strategy = Retry(
        total=5,  # Increased total retries
        backoff_factor=2,  # Exponential backoff: 1, 2, 4, 8, 16 seconds
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

def extract_shareholding_pct(xbrl_url):
    """
    Extract distinct contextRef names from XBRL shareholding data
    
    Args:
        xbrl_url (str): URL to the XBRL file
        
    Returns:
        list: List of distinct contextRef names for shareholding categories
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

        # Extract distinct contextRef names
        context_refs = set()

        for n in root.xpath(
            "//*[local-name()='ShareholdingAsAPercentageOfTotalNumberOfShares']"
        ):
            ctx = n.attrib.get("contextRef")
            if ctx:
                context_refs.add(ctx)

        # Return as sorted list for consistency
        return sorted(list(context_refs))
    
    except requests.exceptions.ConnectionError as e:
        raise Exception(f"Connection error: {str(e)}")
    except requests.exceptions.Timeout as e:
        raise Exception(f"Timeout error: {str(e)}")
    except Exception as e:
        raise Exception(f"Error parsing XBRL: {type(e).__name__}: {str(e)}")




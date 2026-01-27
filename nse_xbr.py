import requests
import pandas as pd
import urllib3

# Suppress the warning about unverified requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

url = "https://www.nseindia.com/api/corporate-share-holdings-master"
params = {
    "index": "equities",
    "from_date": "01-01-2018",
    "to_date": "01-01-2026"
}

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/"
}

session = requests.Session()

# Step 1: warm-up request (mandatory) - with verify=False
session.get("https://www.nseindia.com", headers=headers, timeout=10, verify=False)

# Step 2: API call - with verify=False
resp = session.get(url, headers=headers, params=params, timeout=10, verify=False)
data = resp.json()

df = pd.DataFrame(data)[[
    "symbol",
    "name",
    "date",
    "xbrl",
    "pr_and_prgrp",
    "public_val",
    "submissionDate",
    "broadcastDate"
]]

print(df)

df.to_csv("nse_xbr.csv", index=False)
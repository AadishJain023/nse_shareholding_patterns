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
from perf_constants import (
    perf_bank,
    perf_promoter,
    perf_public,
    perf_mf,
    perf_FPI,
    perf_insur,
)

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


def fetch_data_from_xbrl(xbrl_url, category_contexts):
    """
    Fetch shareholding data from XBRL file for specific context_refs
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

        root = etree.fromstring(response.content)

        totals = {cat: 0.0 for cat in category_contexts}

        for n in root.xpath("//*[local-name()='ShareholdingAsAPercentageOfTotalNumberOfShares']"):
            ctx = n.attrib.get("contextRef")
            if not ctx or not n.text:
                continue

            val = float(n.text)

            for cat, ctx_set in category_contexts.items():
                if ctx in ctx_set:
                    totals[cat] += val

        return totals

    except Exception as e:
        raise Exception(f"Error fetching from {xbrl_url}: {type(e).__name__}: {str(e)}")


def process_stock_for_context_refs(args):
    """
    Process a single stock for specific context_refs
    """
    idx, row, category_contexts = args
    symbol = row['symbol']
    xbrl_url = row['xbrl']
    date = pd.to_datetime(row["date"], dayfirst=True)

    time.sleep(random.uniform(0.5, 1.5))

    try:
        data = fetch_data_from_xbrl(xbrl_url, category_contexts)
        result = {
            'symbol': symbol,
            'date': date,
            'status': 'success',
            'data': data
        }
        print(f"✓ {symbol}: {len(data)} data points found")
        return result

    except Exception as e:
        print(f"✗ {symbol}: {type(e).__name__}")
        return {
            'symbol': symbol,
            'date': date,
            'status': 'error',
            'error': str(e),
            'data': {}
        }


if __name__ == "__main__":
    csv_path = "sample.csv"
    output_file = "sample_output.csv"

    CATEGORY_CONTEXTS = {
        "perf_bank": set(perf_bank),
        "perf_promoter": set(perf_promoter),
        "perf_public": set(perf_public),
        "perf_mf": set(perf_mf),
        "perf_FPI": set(perf_FPI),
        "perf_insur": set(perf_insur),
    }

    df = pd.read_csv(csv_path)
    df["date"] = pd.to_datetime(df["date"], dayfirst=True)

    meta_df = (
        df[[
            "symbol",
            "date",
            "pr_and_prgrp",
            "public_val",
            "broadcastDate"
        ]]
        .set_index(["symbol", "date"])
    )

    tasks = [(idx, row, CATEGORY_CONTEXTS) for idx, row in df.iterrows()]
    results = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_stock_for_context_refs, t) for t in tasks]

        for i, f in enumerate(as_completed(futures), 1):
            res = f.result()
            results.append(res)
            if i % 50 == 0:
                print(f"[Progress] {i}/{len(tasks)} done")

    rows = []
    for r in results:
        base = {
            "symbol": r["symbol"],
            "date": r["date"],
            "status": r["status"]
        }

        if r["status"] == "success":
            base.update(r["data"])
        else:
            base["error"] = r.get("error", "")

        rows.append(base)

    result_df = pd.DataFrame(rows)

    result_df = (
        result_df
        .set_index(["symbol", "date"])
        .join(meta_df, how="left")
        .reset_index()
    )

    FIELD_MAP = {
        "perf_bank": "perc_bank",
        "perf_FPI": "perc_FPI",
        "perf_insur": "perc_insur",
        "perf_mf": "perc_mf",
        "perf_promoter": "perc_promoters",
        "perf_public": "perc_public",
    }

    long_rows = []

    for _, r in result_df.iterrows():
        if r["status"] != "success":
            continue

        period = r["date"].strftime("%Y%m")

        flag = True
        for col, field in FIELD_MAP.items():
            long_rows.append({
                "Date_ReportingPeriod": period,
                "Symbol_AsOfReportingDate": r["symbol"],
                "Field": field,
                "Value": r.get(col),
                "UnitsOfValue": "%",
                "csv_pr_and_prgrp": r.get("pr_and_prgrp")if flag else None,
                "csv_public_val": r.get("public_val") if flag else None
            })
            flag = False

    final_df = pd.DataFrame(long_rows)
    final_df.to_csv(output_file, index=False)

    print(f"\nSaved → {output_file}")
    print("\nSample output:")
    print(final_df.head())

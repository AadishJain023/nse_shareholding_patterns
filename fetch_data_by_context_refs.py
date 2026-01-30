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


def fetch_data_from_xbrl(xbrl_url, category_contexts):
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
    
    # Add random delay
    time.sleep(random.uniform(0.5, 1.5))
    
    try:
        data = fetch_data_from_xbrl(xbrl_url, category_contexts)
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



if __name__ == "__main__":
    csv_path = "nse_xbr_data.csv"
    output_file = "shareholding_data_by_context_refs.csv"
  
    perf_bank = [
        "DetailsOfSharesHeldByBanks001I",
        "DetailsOfSharesHeldByBanks002I",
        "DetailsOfSharesHeldByBanks003I",
        "DetailsOfSharesHeldByBanks004I",
        "DetailsOfSharesHeldByBanks005I",
        "DetailsOfSharesHeldByBanks006I",
        "DetailsOfSharesHeldByBanks007I",
        "DetailsOfSharesHeldByBanks008I",
        "DetailsOfSharesHeldByBanks009I",
        "DetailsOfSharesHeldByBanks010I",
        "DetailsOfSharesHeldByBanks011I",
        "DetailsOfSharesHeldByBanks012I",
        "DetailsOfSharesHeldByBanks_Context15",
        "DetailsOfSharesHeldByBanks_Context16",
        "DetailsOfSharesHeldByBanks_Context17",
        "DetailsOfSharesHeldByBanks_Context18",
        "DetailsOfSharesHeldByBanks_Context19",
        "DetailsOfSharesHeldByBanks_Context20",
        "DetailsOfSharesHeldByBanks_Context21",
        "BanksI",
        "Banks_ContextI",
        "DetailsOfSharesHeldByFinancialInstitutionOrBanks001I",
        "DetailsOfSharesHeldByFinancialInstitutionOrBanks002I",
        "DetailsOfSharesHeldByFinancialInstitutionOrBanks003I",
        "DetailsOfSharesHeldByFinancialInstitutionOrBanks004I",
        "DetailsOfSharesHeldByFinancialInstitutionOrBanks005I",
        "DetailsOfSharesHeldByFinancialInstitutionOrBanks006I",
        "DetailsOfSharesHeldByFinancialInstitutionOrBanks007I",
        "DetailsOfSharesHeldByFinancialInstitutionOrBanks008I",
        "DetailsOfSharesHeldByFinancialInstitutionOrBanks009I",
        "DetailsOfSharesHeldByFinancialInstitutionOrBanks010I",
        "DetailsOfSharesHeldByFinancialInstitutionOrBanks011I",
        "DetailsOfSharesHeldByFinancialInstitutionOrBanks012I",
        "DetailsOfSharesHeldByFinancialInstitutionOrBanks013I",
        "DetailsOfSharesHeldByFinancialInstitutionOrBanks014I",
        "DetailsOfSharesHeldByFinancialInstitutionOrBanks015I",
        "DetailsOfSharesHeldByFinancialInstitutionOrBanks016I",
        "DetailsOfSharesHeldByFinancialInstitutionOrBanks017I",
        "DetailsOfSharesHeldByFinancialInstitutionOrBanks018I",
        "DetailsOfSharesHeldByIndianFinancialInstitutionsOrBanks001I",
        "DetailsOfSharesHeldByIndianFinancialInstitutionsOrBanks002I",
        "DetailsOfSharesHeldByIndianFinancialInstitutionsOrBanks003I",
        "DetailsOfSharesHeldByIndianFinancialInstitutionsOrBanks004I",
        "DetailsOfSharesHeldByIndianFinancialInstitutionsOrBanks005I",
        "DetailsOfSharesHeldByIndianFinancialInstitutionsOrBanks006I",
        "DetailsOfSharesHeldByIndianFinancialInstitutionsOrBanks007I",
        "DetailsOfSharesHeldByIndianFinancialInstitutionsOrBanks008I",
        "DetailsOfSharesHeldByIndianFinancialInstitutionsOrBanks009I",
        "DetailsOfSharesHeldByIndianFinancialInstitutionsOrBanks010I",
        "FinancialInstitutionOrBanksI",
        "IndianFinancialInstitutionsOrBanksI",
        "IndianFinancialInstitutionsOrBanks_Context15",
        "IndianFinancialInstitutionsOrBanks_Context16",
        "IndianFinancialInstitutionsOrBanks_Context17",
        "IndianFinancialInstitutionsOrBanks_Context18",
        "IndianFinancialInstitutionsOrBanks_Context19",
        "IndianFinancialInstitutionsOrBanks_Context20",
        "IndianFinancialInstitutionsOrBanks_Context21",
        "IndianFinancialInstitutionsOrBanks_Context22",
        "IndianFinancialInstitutionsOrBanks_Context23",
        "IndianFinancialInstitutionsOrBanks_Context24",
        "IndianFinancialInstitutionsOrBanks_Context25",
        "IndianFinancialInstitutionsOrBanks_Context26",
        "IndianFinancialInstitutionsOrBanks_Context27",
        "IndianFinancialInstitutionsOrBanks_Context28",
        "IndianFinancialInstitutionsOrBanks_ContextI",
    ]

    perf_promoter = [
        "DetailsOfSharesHeldByRelativesOfPromotersOtherThanPromoterGroup001I",
        "DetailsOfSharesHeldByRelativesOfPromotersOtherThanPromoterGroup002I",
        "DetailsOfSharesHeldByRelativesOfPromotersOtherThanPromoterGroup003I",
        "DetailsOfSharesHeldByRelativesOfPromotersOtherThanPromoterGroup004I",
        "DetailsOfSharesHeldByRelativesOfPromotersOtherThanPromoterGroup005I",
        "DetailsOfSharesHeldByRelativesOfPromotersOtherThanPromoterGroup006I",
        "DetailsOfSharesHeldByRelativesOfPromotersOtherThanPromoterGroup_Context15",
        "DetailsOfSharesHeldByRelativesOfPromotersOtherThanPromoterGroup_Context16",
        "DetailsOfSharesHeldByRelativesOfPromotersOtherThanPromoterGroup_Context17",
        "DetailsOfSharesHeldByRelativesOfPromotersOtherThanPromoterGroup_Context18",
        "DetailsOfSharesHeldByRelativesOfPromotersOtherThanPromoterGroup_Context19",
        "DetailsOfSharesHeldByRelativesOfPromotersOtherThanPromoterGroup_Context20",
        "DetailsOfSharesHeldByShareholdingByCompaniesOrBodiesCorporateWhereCentralOrStateGovernmentIsPromoter_Context15",
        "DetailsOfSharesHeldByShareholdingByCompaniesOrBodiesCorporateWhereCentralOrStateGovernmentIsPromoter_Context16",
        "DetailsOfSharesHeldByShareholdingByCompaniesOrBodiesCorporateWhereCentralOrStateGovernmentIsPromoter_Context17",
        "DetailsOfSharesHeldByShareholdingByCompaniesOrBodiesCorporatewhereCentralOrStateGovernmentIsPromoter001I",
        "DetailsOfSharesHeldByShareholdingByCompaniesOrBodiesCorporatewhereCentralOrStateGovernmentIsPromoter002I",
        "DetailsOfSharesHeldByShareholdingByCompaniesOrBodiesCorporatewhereCentralOrStateGovernmentIsPromoter003I",
        "DetailsOfSharesHeldByTrustsWhereAnyPersonBelongingToPromoterAndPromoterGroupIsTrusteeOrBeneficiaryOrAuthorOfTrust_Context15",
        "DetailsOfSharesHeldByTrustsWhereAnyPersonBelongingToPromoterAndPromoterGroupIsTrusteeOrBeneficiaryOrAuthorOfTrust_Context16",
        "DetailsOfSharesHeldByTrustsWhereAnyPersonBelongingToPromoterAndPromoterGroupIsTrusteeOrBeneficiaryOrAuthorOfTrust_Context17",
        "DetailsOfSharesHeldByTrustsWhereAnyPersonBelongingToPromoterAndPromoterGroupIsTrusteeOrBeneficiaryOrAuthorOfTrust_Context18",
        "DetailsOfSharesHeldByTrustsWhereAnyPersonBelongingToPromoterAndPromoterGroupIsTrusteeOrBeneficiaryOrAuthorOfTrust_Context19",
        "DetailsOfSharesHeldByTrustsWhereAnyPersonBelongingToPromoterAndPromoterGroupIsTrusteeOrBeneficiaryOrAuthorOfTrust_Context20",
        "DetailsOfSharesHeldByTrustsWhereAnyPersonBelongingToPromoterAndPromoterGroupIsTrusteeOrBeneficiaryOrAuthorOfTrust_Context21",
        "DetailsOfSharesHeldByTrustsWhereAnyPersonBelongingToPromoterAndPromoterGroupIsTrusteeOrBeneficiaryOrAuthorOfTrust_Context22",
        "DetailsOfSharesHeldByTrustsWhereAnyPersonBelongingToPromoterAndPromoterGroupIsTrusteeOrBeneficiaryOrAuthorOfTrust_Context23",
        "DetailsOfSharesHeldByTrustsWhereAnyPersonBelongingToPromoterAndPromoterGroupIsTrusteeOrBeneficiaryOrAuthorOfTrust_Context24",
        "DetailsOfSharesHeldByTrustsWhereAnyPersonBelongingToPromoterAndPromoterGroupIsTrusteeOrBeneficiaryOrAuthorOfTrust_Context25",
        "DetailsOfSharesHeldByTrustsWhereAnyPersonBelongingToPromoterAndPromoterGroupIsisTrusteeOrBeneficiaryOrAuthorOfTrust001I",
        "DetailsOfSharesHeldByTrustsWhereAnyPersonBelongingToPromoterAndPromoterGroupIsisTrusteeOrBeneficiaryOrAuthorOfTrust002I",
        "DetailsOfSharesHeldByTrustsWhereAnyPersonBelongingToPromoterAndPromoterGroupIsisTrusteeOrBeneficiaryOrAuthorOfTrust003I",
        "RelativesOfPromotersOtherThanPromoterGroupI",
        "RelativesOfPromotersOtherThanPromoterGroup_ContextI",
        "ShareholdingByCompaniesOrBodiesCorporateWhereCentralOrStateGovernmentIsPromoter_ContextI",
        "ShareholdingByCompaniesOrBodiesCorporatewhereCentralOrStateGovernmentIsPromoterI",
        "ShareholdingOfPromoterAndPromoterGroupI",
        "ShareholdingOfPromoterAndPromoterGroup_ContextI",
        "TrustsWhereAnyPersonBelongingToPromoterAndPromoterGroupIsTrusteeOrBeneficiaryOrAuthorOfTrust_ContextI",
        "TrustsWhereAnyPersonBelongingToPromoterAndPromoterGroupIsisTrusteeOrBeneficiaryOrAuthorOfTrustI",
    ]

    perf_public = [
        "PublicShareholdingI",
        "PublicShareholding_ContextI",
    ]

    perf_mf = [
        "DetailsOfSharesHeldByMutualFundsOrUti001I",
        "DetailsOfSharesHeldByMutualFundsOrUti002I",
        "DetailsOfSharesHeldByMutualFundsOrUti003I",
        "DetailsOfSharesHeldByMutualFundsOrUti004I",
        "DetailsOfSharesHeldByMutualFundsOrUti005I",
        "DetailsOfSharesHeldByMutualFundsOrUti006I",
        "DetailsOfSharesHeldByMutualFundsOrUti007I",
        "DetailsOfSharesHeldByMutualFundsOrUti008I",
        "DetailsOfSharesHeldByMutualFundsOrUti009I",
        "DetailsOfSharesHeldByMutualFundsOrUti010I",
        "DetailsOfSharesHeldByMutualFundsOrUti011I",
        "DetailsOfSharesHeldByMutualFundsOrUti012I",
        "DetailsOfSharesHeldByMutualFundsOrUti013I",
        "DetailsOfSharesHeldByMutualFundsOrUti014I",
        "DetailsOfSharesHeldByMutualFundsOrUti015I",
        "DetailsOfSharesHeldByMutualFundsOrUti016I",
        "DetailsOfSharesHeldByMutualFundsOrUti017I",
        "MutualFundsOrUTI_Context15",
        "MutualFundsOrUTI_Context16",
        "MutualFundsOrUTI_Context17",
        "MutualFundsOrUTI_Context18",
        "MutualFundsOrUTI_Context19",
        "MutualFundsOrUTI_Context20",
        "MutualFundsOrUTI_Context21",
        "MutualFundsOrUTI_Context22",
        "MutualFundsOrUTI_Context23",
        "MutualFundsOrUTI_Context24",
        "MutualFundsOrUTI_Context25",
        "MutualFundsOrUTI_Context26",
        "MutualFundsOrUTI_Context27",
        "MutualFundsOrUTI_Context28",
        "MutualFundsOrUTI_Context29",
        "MutualFundsOrUTI_ContextI",
        "MutualFundsOrUtiI",
    ]

    perf_FPI = [
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestor001I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestor002I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestor003I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestor004I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestor005I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestor006I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestor007I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestor008I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestor009I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestor010I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestor011I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestor012I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestor013I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestor014I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestor015I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestor016I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestor017I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne001I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne002I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne003I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne004I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne005I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne006I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne007I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne008I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne009I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne010I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne011I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne012I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne013I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne014I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne015I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne_Context15",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne_Context16",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne_Context17",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne_Context18",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne_Context19",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne_Context20",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne_Context21",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne_Context22",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne_Context23",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne_Context24",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne_Context25",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne_Context26",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne_Context27",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne_Context28",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne_Context29",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne_Context30",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne_Context31",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne_Context32",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne_Context33",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne_Context34",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorOne_Context35",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorTwo001I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorTwo002I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorTwo003I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorTwo004I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorTwo005I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorTwo006I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorTwo007I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorTwo008I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorTwo009I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorTwo010I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorTwo011I",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorTwo_Context15",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorTwo_Context16",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorTwo_Context16",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorTwo_Context17",
        "DetailsOfSharesHeldByInstitutionsForeignPortfolioInvestorTwo_Context18",
        "ForeignPortfolioInvestorI",
        "ForeignPortfolioInvestor_Context15",
        "ForeignPortfolioInvestor_Context16",
        "ForeignPortfolioInvestor_ContextI",
        "InstitutionsForeignPortfolioInvestorCategoryOne_ContextI",
        "InstitutionsForeignPortfolioInvestorCategoryTwo_ContextI",
        "InstitutionsForeignPortfolioInvestorCatergoryOneI",
        "InstitutionsForeignPortfolioInvestorCatergoryTwoI",
        "InstitutionsForeignPortfolioInvestorI"
    ]

    perf_insur = [
        "DetailsOfSharesHeldByInsuranceCompanies001I",
        "DetailsOfSharesHeldByInsuranceCompanies002I",
        "DetailsOfSharesHeldByInsuranceCompanies003I",
        "DetailsOfSharesHeldByInsuranceCompanies004I",
        "DetailsOfSharesHeldByInsuranceCompanies005I",
        "DetailsOfSharesHeldByInsuranceCompanies006I",
        "InsuranceCompaniesI",
        "InsuranceCompanies_Context15",
        "InsuranceCompanies_Context16",
        "InsuranceCompanies_Context17",
        "InsuranceCompanies_Context18",
        "InsuranceCompanies_Context19",
        "InsuranceCompanies_ContextI"
    ]

    CATEGORY_CONTEXTS = {
        "perf_bank": set(perf_bank),
        "perf_promoter": set(perf_promoter),
        "perf_public": set(perf_public),
        "perf_mf": set(perf_mf),
        "perf_FPI": set(perf_FPI),
        "perf_insur": set(perf_insur),
    }

    df = pd.read_csv(csv_path)

    tasks = [
        (idx, row, CATEGORY_CONTEXTS)
        for idx, row in df.iterrows()
    ]

    results = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_stock_for_context_refs, t) for t in tasks]

        for i, f in enumerate(as_completed(futures), 1):
            res = f.result()
            results.append(res)
            if i % 50 == 0:
                print(f"[Progress] {i}/{len(tasks)} done")

    # Flatten results
    rows = []
    for r in results:
        base = {
            "symbol": r["symbol"],
            "status": r["status"]
        }
        if r["status"] == "success":
            base.update(r["data"])
        else:
            base["error"] = r.get("error", "")
        rows.append(base)

    result_df = pd.DataFrame(rows)

    result_df.to_csv(output_file, index=False)
    print(f"\nSaved → {output_file}")

    print("\nSample output:")
    print(result_df.head())

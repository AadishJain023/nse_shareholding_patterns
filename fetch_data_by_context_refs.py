import requests
import pandas as pd
from lxml import etree
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CSV_PATH = "sample.csv"
FINAL_OUTPUT = "final.csv"
MAX_WORKERS = 5

# ─────────────────────────────────────────────
# CATEGORY → FIELD NAME
# ─────────────────────────────────────────────
FIELD_MAP = {
    "perf_FPI": "perc_FPI",
    "perf_bank": "perc_bank",
    "perf_insur": "perc_insur",
    "perf_mf": "perc_mf",
    "perf_public": "perc_public",
    "perf_promoter": "perc_promoters",
}

# ─────────────────────────────────────────────
def get_session():
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=10,
        pool_maxsize=10
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


# ─────────────────────────────────────────────
def fetch_xbrl(xbrl_url, category_contexts):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/xml, text/xml, */*',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive'
    }
    try:
        session = get_session()
        r = session.get(xbrl_url, headers = headers,timeout=60, verify=False)
        r.raise_for_status()

        root = etree.fromstring(r.content)

        totals = {k: 0.0 for k in category_contexts}

        nodes = root.xpath(
            "//*[local-name()='ShareholdingAsAPercentageOfTotalNumberOfShares']"
        )

        for n in nodes:
            ctx = n.attrib.get("contextRef")
            if not ctx or not n.text:
                continue

            try:
                val = float(n.text)
            except:
                continue

            for cat, ctx_set in category_contexts.items():
                if ctx in ctx_set:
                    totals[cat] += val

        return totals
    except Exception as e:
            raise Exception(f"Error fetching from {xbrl_url} : {type(e).__name__}: {str(e)}")


# ─────────────────────────────────────────────
def process_stock(args):
    idx, row, category_contexts = args
    symbol = row["symbol"]

    time.sleep(random.uniform(0.4, 1.2))

    try:
        data = fetch_xbrl(row["xbrl"], category_contexts)

        non_zero = sum(1 for v in data.values() if v > 0)
        print(f"✓ {symbol}: {non_zero} categories")

        return {
            "symbol": symbol,
            "date": row["date"],
            "broadcastDate": row["broadcastDate"],
            "pr_and_prgrp" : row["pr_and_prgrp"],
            "public_val" : row["public_val"],
            "status": "success",
            "data": data
        }

    except Exception as e:
        print(f"✗ {symbol}: {type(e).__name__}")
        return {
            "symbol": symbol,
            "date": row["date"],
            "broadcastDate": row["broadcastDate"],
            "status": "error",
            "error": str(e),
            "pr_and_prgrp" : row["pr_and_prgrp"],
            "public_val" : row["public_val"],
            "data": {}
        }


# ─────────────────────────────────────────────
if __name__ == "__main__":

     
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

    df = pd.read_csv(CSV_PATH)

    tasks = [(i, r, CATEGORY_CONTEXTS) for i, r in df.iterrows()]
    results = []
    total = len(tasks)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(process_stock, t) for t in tasks]

        for i, f in enumerate(as_completed(futures), 1):
            results.append(f.result())

            if i % 50 == 0 or i == total:
                print(f"[Progress] Completed {i}/{total}")

    # ─────────────────────────────────────────
    # LONG FORMAT OUTPUT
    # ─────────────────────────────────────────
    rows = []

    for r in results:
        if r["status"] != "success":
            continue

        first = True

        for cat, field_name in FIELD_MAP.items():
            rows.append({
                "Date_ReportingPeriod": r["date"],
                "Date_DataAvailability": r["broadcastDate"],
                "Symbol_AsOfReportingDate": r["symbol"],
                "Field": field_name,
                "Value": r["data"].get(cat, 0.0),
                "pr_and_prgrp" : r["pr_and_prgrp"] if first else None,
                "public_val" : r["public_val"] if first else None
            })
            first = False
            

    final_df = pd.DataFrame(rows)
    final_df.to_csv(FINAL_OUTPUT, index=False)

    print(f"\n✅ Final long-format file saved → {FINAL_OUTPUT}")
    print(final_df.head(10))

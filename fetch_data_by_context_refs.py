import asyncio
import aiohttp
import pandas as pd
from lxml import etree
import random
import time

# ---------------- CONFIG ----------------
MAX_CONCURRENCY = 15      # tune: 10–25 is usually safe
REQUEST_TIMEOUT = 60
RETRIES = 3

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/xml, text/xml, */*"
}
# ----------------------------------------


async def fetch_xbrl(session, semaphore, symbol, xbrl_url, category_contexts):
    async with semaphore:
        for attempt in range(RETRIES):
            try:
                async with session.get(xbrl_url, timeout=REQUEST_TIMEOUT) as resp:
                    resp.raise_for_status()
                    xml = await resp.read()

                root = etree.fromstring(xml)

                totals = {cat: 0.0 for cat in category_contexts}

                # single XPath scan
                for n in root.xpath(
                    "//*[local-name()='ShareholdingAsAPercentageOfTotalNumberOfShares']"
                ):
                    ctx = n.attrib.get("contextRef")
                    if not ctx or not n.text:
                        continue

                    val = float(n.text)

                    for cat, ctx_set in category_contexts.items():
                        if ctx in ctx_set:
                            totals[cat] += val

                return {
                    "symbol": symbol,
                    "status": "success",
                    **totals
                }

            except Exception as e:
                if attempt == RETRIES - 1:
                    return {
                        "symbol": symbol,
                        "status": "error",
                        "error": str(e)
                    }
                await asyncio.sleep(2 ** attempt + random.random())


async def run_async(csv_path, category_contexts, output_file=None, limit=None):
    df = pd.read_csv(csv_path)
    if limit:
        df = df.head(limit)

    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENCY, ssl=False)

    async with aiohttp.ClientSession(
        connector=connector,
        headers=HEADERS
    ) as session:

        tasks = [
            fetch_xbrl(
                session,
                semaphore,
                row["symbol"],
                row["xbrl"],
                category_contexts
            )
            for _, row in df.iterrows()
        ]

        results = []
        completed = 0

        for coro in asyncio.as_completed(tasks):
            res = await coro
            results.append(res)
            completed += 1
            if completed % 50 == 0:
                print(f"[Progress] {completed}/{len(tasks)} done")

    result_df = pd.DataFrame(results)

    if output_file:
        result_df.to_csv(output_file, index=False)
        print(f"\nSaved → {output_file}")

    return result_df


# ---------------- RUN ----------------
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


    start = time.time()
    df = asyncio.run(
        run_async(
            csv_path,
            CATEGORY_CONTEXTS,
            output_file=output_file
        )
    )
    print("\nSample output:")
    print(df.head())
    print(f"\nTotal time: {time.time() - start:.2f}s")
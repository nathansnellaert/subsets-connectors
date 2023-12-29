from dagster import asset, FreshnessPolicy
import pandas as pd
from integrations.assets.fmp.utils import make_v4_request

@asset(
    metadata={
        "source": "Financial Modeling Prep",
        "name": "Balance Sheet Data",
        "description": "Retrieves balance sheet data for multiple years and transforms it into a structured format.",
        "columns": [
            {"name": "symbol", "description": "Ticker symbol of the company."},
            {"name": "date", "description": "Date of the balance sheet."},
            {"name": "reported_currency", "description": "Currency in which the report is made."},
            {"name": "cik", "description": "Central Index Key (CIK) number."},
            {"name": "filling_date", "description": "Date when the report was filed."},
            {"name": "accepted_date", "description": "Date when the report was accepted."},
            {"name": "calendar_year", "description": "Calendar year of the report."},
            {"name": "period", "description": "Reporting period."},
            {"name": "cash_and_cash_equivalents", "description": "Cash and cash equivalents."},
            {"name": "short_term_investments", "description": "Short term investments."},
            {"name": "cash_and_short_term_investments", "description": "Cash and short-term investments combined."},
            {"name": "net_receivables", "description": "Net receivables."},
            {"name": "inventory", "description": "Inventory value."},
            {"name": "other_current_assets", "description": "Other current assets."},
            {"name": "total_current_assets", "description": "Total current assets."},
            {"name": "property_plant_equipment_net", "description": "Net property, plant, and equipment."},
            {"name": "goodwill", "description": "Goodwill."},
            {"name": "intangible_assets", "description": "Intangible assets."},
            {"name": "goodwill_and_intangible_assets", "description": "Goodwill and intangible assets combined."},
            {"name": "long_term_investments", "description": "Long term investments."},
            {"name": "tax_assets", "description": "Tax assets."},
            {"name": "other_non_current_assets", "description": "Other non-current assets."},
            {"name": "total_non_current_assets", "description": "Total non-current assets."},
            {"name": "other_assets", "description": "Other assets."},
            {"name": "total_assets", "description": "Total assets."},
            {"name": "account_payables", "description": "Account payables."},
            {"name": "short_term_debt", "description": "Short term debt."},
            {"name": "tax_payables", "description": "Tax payables."},
            {"name": "deferred_revenue", "description": "Deferred revenue."},
            {"name": "other_current_liabilities", "description": "Other current liabilities."},
            {"name": "total_current_liabilities", "description": "Total current liabilities."},
            {"name": "long_term_debt", "description": "Long term debt."},
            {"name": "deferred_revenue_non_current", "description": "Deferred revenue non-current."},
            {"name": "deferrred_tax_liabilities_non_current", "description": "Deferred tax liabilities non-current."},
            {"name": "other_non_current_liabilities", "description": "Other non-current liabilities."},
            {"name": "total_non_current_liabilities", "description": "Total non-current liabilities."},
            {"name": "other_liabilities", "description": "Other liabilities."},
            {"name": "total_liabilities", "description": "Total liabilities."},
            {"name": "preferred_stock", "description": "Preferred stock."},
            {"name": "common_stock", "description": "Common stock."},
            {"name": "retained_earnings", "description": "Retained earnings."},
            {"name": "accumulated_other_comprehensive_income_loss", "description": "Accumulated other comprehensive income loss."},
            {"name": "othertotal_stockholders_equity", "description": "Other total stockholders' equity."},
            {"name": "total_stockholders_equity", "description": "Total stockholders' equity."},
            {"name": "total_liabilities_and_stockholders_equity", "description": "Total liabilities and stockholders' equity."},
            {"name": "total_investments", "description": "Total investments."},
            {"name": "total_debt", "description": "Total debt."},
            {"name": "net_debt", "description": "Net debt."},
            {"name": "link", "description": "Link to the source document."},
            {"name": "final_link", "description": "Final link to the source document."},
            {"name": "minority_interest", "description": "Minority interest."},
            {"name": "capital_lease_obligations", "description": "Capital lease obligations."}
        ]
    },
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24 * 30, cron_schedule="0 0 1 * *")
)



def fmp_balance_sheet():
    def handle_request(year):
        df = make_v4_request('balance-sheet-statement-bulk', {
            "year": year,
            "period": "quarter"
        })
        column_name_mapping = {
            'symbol': 'symbol',
            'date': 'date',
            'reportedCurrency': 'reported_currency',
            'cik': 'cik',
            'fillingDate': 'filling_date',
            'acceptedDate': 'accepted_date',
            'calendarYear': 'calendar_year',
            'period': 'period',
            'cashAndCashEquivalents': 'cash_and_cash_equivalents',
            'shortTermInvestments': 'short_term_investments',
            'cashAndShortTermInvestments': 'cash_and_short_term_investments',
            'netReceivables': 'net_receivables',
            'inventory': 'inventory',
            'otherCurrentAssets': 'other_current_assets',
            'totalCurrentAssets': 'total_current_assets',
            'propertyPlantEquipmentNet': 'property_plant_equipment_net',
            'goodwill': 'goodwill',
            'intangibleAssets': 'intangible_assets',
            'goodwillAndIntangibleAssets': 'goodwill_and_intangible_assets',
            'longTermInvestments': 'long_term_investments',
            'taxAssets': 'tax_assets',
            'otherNonCurrentAssets': 'other_non_current_assets',
            'totalNonCurrentAssets': 'total_non_current_assets',
            'otherAssets': 'other_assets',
            'totalAssets': 'total_assets',
            'accountPayables': 'account_payables',
            'shortTermDebt': 'short_term_debt',
            'taxPayables': 'tax_payables',
            'deferredRevenue': 'deferred_revenue',
            'otherCurrentLiabilities': 'other_current_liabilities',
            'totalCurrentLiabilities': 'total_current_liabilities',
            'longTermDebt': 'long_term_debt',
            'deferredRevenueNonCurrent': 'deferred_revenue_non_current',
            'deferrredTaxLiabilitiesNonCurrent': 'deferrred_tax_liabilities_non_current',
            'otherNonCurrentLiabilities': 'other_non_current_liabilities',
            'totalNonCurrentLiabilities': 'total_non_current_liabilities',
            'otherLiabilities': 'other_liabilities',
            'totalLiabilities': 'total_liabilities',
            'preferredStock': 'preferred_stock',
            'commonStock': 'common_stock',
            'retainedEarnings': 'retained_earnings',
            'accumulatedOtherComprehensiveIncomeLoss': 'accumulated_other_comprehensive_income_loss',
            'othertotalStockholdersEquity': 'othertotal_stockholders_equity',
            'totalStockholdersEquity': 'total_stockholders_equity',
            'totalLiabilitiesAndStockholdersEquity': 'total_liabilities_and_stockholders_equity',
            'totalInvestments': 'total_investments',
            'totalDebt': 'total_debt',
            'netDebt': 'net_debt',
            'link': 'link',
            'finalLink': 'final_link',
            'minorityInterest': 'minority_interest',
            'capitalLeaseObligations': 'capital_lease_obligations'
        }
        df = df.rename(columns=column_name_mapping)
        df['date'] = pd.to_datetime(df['date']).dt.date
        return df

    dfs = [handle_request(year) for year in range(1985, 2024)]
    df = pd.concat(dfs)
    return df

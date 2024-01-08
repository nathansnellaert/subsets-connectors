from dagster import asset, FreshnessPolicy
import pandas as pd
from integrations.assets.fmp.utils import make_v4_request
from .source import financialmodellingprep


@asset(
    metadata={
        "id": "fmp_cash_flow_statement",
        "namespace": "fmp",
        "name": "Cash Flow Statement",
        "source": {
            "name": "Financial Modeling Prep",
            "url": "https://financialmodelingprep.com/developer/docs/",
            "logo": "https://site.financialmodelingprep.com/apple-touch-icon.png"
        },
        "description": "Cash Flow Statement",
        "columns": [
            {"name": "date", "description": "Date of the cash flow statement."},
            {"name": "symbol", "description": "Ticker symbol of the company."},
            {"name": "reported_currency", "description": "Currency in which the report is made."},
            {"name": "cik", "description": "Central Index Key (CIK) number."},
            {"name": "filling_date", "description": "Date when the report was filed."},
            {"name": "accepted_date", "description": "Date when the report was accepted."},
            {"name": "calendar_year", "description": "Calendar year of the report."},
            {"name": "period", "description": "Reporting period."},
            {"name": "net_income", "description": "Net income."},
            {"name": "depreciation_and_amortization", "description": "Depreciation and amortization."},
            {"name": "deferred_income_tax", "description": "Deferred income tax."},
            {"name": "stock_based_compensation", "description": "Stock based compensation."},
            {"name": "change_in_working_capital", "description": "Change in working capital."},
            {"name": "accounts_receivables", "description": "Accounts receivables."},
            {"name": "inventory", "description": "Inventory."},
            {"name": "accounts_payables", "description": "Accounts payables."},
            {"name": "other_working_capital", "description": "Other working capital."},
            {"name": "other_non_cash_items", "description": "Other non-cash items."},
            {"name": "net_cash_provided_by_operating_activites", "description": "Net cash provided by operating activities."},
            {"name": "investments_in_property_plant_and_equipment", "description": "Investments in property, plant, and equipment."},
            {"name": "acquisitions_net", "description": "Acquisitions net."},
            {"name": "purchases_of_investments", "description": "Purchases of investments."},
            {"name": "sales_maturities_of_investments", "description": "Sales maturities of investments."},
            {"name": "other_investing_activites", "description": "Other investing activities."},
            {"name": "net_cash_used_for_investing_activites", "description": "Net cash used for investing activities."},
            {"name": "debt_repayment", "description": "Debt repayment."},
            {"name": "common_stock_issued", "description": "Common stock issued."},
            {"name": "common_stock_repurchased", "description": "Common stock repurchased."},
            {"name": "dividends_paid", "description": "Dividends paid."},
            {"name": "other_financing_activites", "description": "Other financing activities."},
            {"name": "net_cash_used_provided_by_financing_activities", "description": "Net cash used/provided by financing activities."},
            {"name": "effect_of_forex_changes_on_cash", "description": "Effect of forex changes on cash."},
            {"name": "net_change_in_cash", "description": "Net change in cash."},
            {"name": "cash_at_end_of_period", "description": "Cash at end of period."},
            {"name": "cash_at_beginning_of_period", "description": "Cash at beginning of period."},
            {"name": "operating_cash_flow", "description": "Operating cash flow."},
            {"name": "capital_expenditure", "description": "Capital expenditure."},
            {"name": "free_cash_flow", "description": "Free cash flow."},
            {"name": "link", "description": "Link to the source document."},
            {"name": "final_link", "description": "Final link to the source document."}
        ]
    },
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24 * 30, cron_schedule="0 0 1 * *")
)
def fmp_cash_flow_statement():
    def handle_request(year):
        df = make_v4_request('cash-flow-statement-bulk', {
            "year": year,
            "period": "quarter",
        })
        if df.empty:
            return df
        column_name_mapping = {
            'date': 'date',
            'symbol': 'symbol',
            'reported_currency': 'reported_currency',
            'cik': 'cik',
            'filling_date': 'filling_date',
            'accepted_date': 'accepted_date',
            'calendar_year': 'calendar_year',
            'period': 'period',
            'netIncome': 'net_income',
            'depreciationAndAmortization': 'depreciation_and_amortization',
            'deferredIncomeTax': 'deferred_income_tax',
            'stockBasedCompensation': 'stock_based_compensation',
            'changeInWorkingCapital': 'change_in_working_capital',
            'accountsReceivables': 'accounts_receivables',
            'inventory': 'inventory',
            'accountsPayables': 'accounts_payables',
            'otherWorkingCapital': 'other_working_capital',
            'otherNonCashItems': 'other_non_cash_items',
            'netCashProvidedByOperatingActivites': 'net_cash_provided_by_operating_activites',
            'investmentsInPropertyPlantAndEquipment': 'investments_in_property_plant_and_equipment',
            'acquisitionsNet': 'acquisitions_net',
            'purchasesOfInvestments': 'purchases_of_investments',
            'salesMaturitiesOfInvestments': 'sales_maturities_of_investments',
            'otherInvestingActivites': 'other_investing_activites',
            'netCashUsedForInvestingActivites': 'net_cash_used_for_investing_activites',
            'debtRepayment': 'debt_repayment',
            'commonStockIssued': 'common_stock_issued',
            'commonStockRepurchased': 'common_stock_repurchased',
            'dividendsPaid': 'dividends_paid',
            'otherFinancingActivites': 'other_financing_activites',
            'netCashUsedProvidedByFinancingActivities': 'net_cash_used_provided_by_financing_activities',
            'effectOfForexChangesOnCash': 'effect_of_forex_changes_on_cash',
            'netChangeInCash': 'net_change_in_cash',
            'cashAtEndOfPeriod': 'cash_at_end_of_period',
            'cashAtBeginningOfPeriod': 'cash_at_beginning_of_period',
            'operatingCashFlow': 'operating_cash_flow',
            'capitalExpenditure': 'capital_expenditure',
            'freeCashFlow': 'free_cash_flow',
            'link': 'link',
            'final_link': 'final_link'
        }

        df = df.rename(columns=column_name_mapping)
        df['date'] = pd.to_datetime(df['date']).dt.date
        return df

    dfs = [handle_request(year) for year in range(1985, 2024)]
    df = pd.concat(dfs)
    return df

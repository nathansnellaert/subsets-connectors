from dagster import asset, FreshnessPolicy
import pandas as pd
import os

@asset(
    metadata={
        "id": "fmp_income_statement",
        "namespace": "fmp",
        "name": "Income Statement",
        "source": {
            "name": "Financial Modeling Prep",
            "url": "https://financialmodelingprep.com/developer/docs/",
            "logo": "https://site.financialmodelingprep.com/apple-touch-icon.png"
        },
        "description": "Income Statement data from Financial Modeling Prep API.",
        "columns": [
            {"name": "date", "description": "Date of the income statement."},
            {"name": "symbol", "description": "Ticker symbol of the company."},
            {"name": "reported_currency", "description": "Currency in which the report is made."},
            {"name": "cik", "description": "Central Index Key (CIK) number."},
            {"name": "filling_date", "description": "Date when the report was filed."},
            {"name": "accepted_date", "description": "Date when the report was accepted."},
            {"name": "calendar_year", "description": "Calendar year of the report."},
            {"name": "period", "description": "Reporting period."},
            {"name": "revenue", "description": "Revenue."},
            {"name": "cost_of_revenue", "description": "Cost of revenue."},
            {"name": "gross_profit", "description": "Gross profit."},
            {"name": "gross_profit_ratio", "description": "Gross profit ratio."},
            {"name": "research_and_development_expenses", "description": "Research and development expenses."},
            {"name": "general_and_administrative_expenses", "description": "General and administrative expenses."},
            {"name": "selling_and_marketing_expenses", "description": "Selling and marketing expenses."},
            {"name": "selling_general_and_administrative_expenses", "description": "Selling, general and administrative expenses."},
            {"name": "other_expenses", "description": "Other expenses."},
            {"name": "operating_expenses", "description": "Operating expenses."},
            {"name": "cost_and_expenses", "description": "Cost and expenses."},
            {"name": "interest_expense", "description": "Interest expense."},
            {"name": "depreciation_and_amortization", "description": "Depreciation and amortization."},
            {"name": "ebitda", "description": "EBITDA."},
            {"name": "ebitda_ratio", "description": "EBITDA ratio."},
            {"name": "operating_income", "description": "Operating income."},
            {"name": "operating_income_ratio", "description": "Operating income ratio."},
            {"name": "total_other_income_expenses_net", "description": "Total other income expenses net."},
            {"name": "income_before_tax", "description": "Income before tax."},
            {"name": "income_before_tax_ratio", "description": "Income before tax ratio."},
            {"name": "income_tax_expense", "description": "Income tax expense."},
            {"name": "net_income", "description": "Net income."},
            {"name": "net_income_ratio", "description": "Net income ratio."},
            {"name": "e_p_s", "description": "Earnings per share (EPS)."},
            {"name": "e_p_s_diluted", "description": "Diluted earnings per share (EPS)."},
            {"name": "weighted_average_shs_out", "description": "Weighted average shares outstanding."},
            {"name": "weighted_average_shs_out_dil", "description": "Weighted average shares outstanding (diluted)."},
            {"name": "link", "description": "Link to the source document."},
            {"name": "final_link", "description": "Final link to the source document."},
            {"name": "interest_income", "description": "Interest income."}
        ]
    },
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24 * 30, cron_schedule="0 0 1 * *")
)
def fmp_income_statement():
    def handle_request(year):
        BASE_URL = 'https://financialmodelingprep.com/api/v4/'
        api_key = os.environ['FMP_API_KEY']
        url = f"{BASE_URL}income-statement-bulk?year={year}&period=quarter&datatype=csv&apikey={api_key}"
        df = pd.read_csv(url)
        column_name_mapping = {
            "date": "date",
            "symbol": "symbol",
            "reportedCurrency": "reported_currency",
            "cik": "cik",
            "fillingDate": "filling_date",
            "acceptedDate": "accepted_date",
            "calendarYear": "calendar_year",
            "period": "period",
            "revenue": "revenue",
            "costOfRevenue": "cost_of_revenue",
            "grossProfit": "gross_profit",
            "grossProfitRatio": "gross_profit_ratio",
            "ResearchAndDevelopmentExpenses": "research_and_development_expenses",
            "GeneralAndAdministrativeExpenses": "general_and_administrative_expenses",
            "SellingAndMarketingExpenses": "selling_and_marketing_expenses",
            "SellingGeneralAndAdministrativeExpenses": "selling_general_and_administrative_expenses",
            "otherExpenses": "other_expenses",
            "operatingExpenses": "operating_expenses",
            "costAndExpenses": "cost_and_expenses",
            "interestExpense": "interest_expense",
            "depreciationAndAmortization": "depreciation_and_amortization",
            "EBITDA": "ebitda",
            "EBITDARatio": "ebitda_ratio",
            "operatingIncome": "operating_income",
            "operatingIncomeRatio": "operating_income_ratio",
            "totalOtherIncomeExpensesNet": "total_other_income_expenses_net",
            "incomeBeforeTax": "income_before_tax",
            "incomeBeforeTaxRatio": "income_before_tax_ratio",
            "incomeTaxExpense": "income_tax_expense",
            "netIncome": "net_income",
            "netIncomeRatio": "net_income_ratio",
            "EPS": "e_p_s",
            "EPSDiluted": "e_p_s_diluted",
            "weightedAverageShsOut": "weighted_average_shs_out",
            "weightedAverageShsOutDil": "weighted_average_shs_out_dil",
            "link": "link",
            "finalLink": "final_link",
            "interestIncome": "interest_income"
        }
        df = df.rename(columns=column_name_mapping)
        df['date'] = pd.to_datetime(df['date']).dt.date
        return df

    dfs = [handle_request(year) for year in range(1985, 2023)]
    df = pd.concat(dfs)
    df = df.dropna(how='all')
    return df
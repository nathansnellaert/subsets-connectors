from dagster import asset, FreshnessPolicy
import pandas as pd
from .utils import make_v4_request
from .source import financialmodellingprep

@asset(
    metadata={
        "source": financialmodellingprep,
        "name": "Executive Compensation Data",
        "description": "Retrieves detailed compensation data for executives, including salary, bonus, stock awards, and other incentives.",
        "columns": [
            {"name": "cik", "type": "string", "description": "Central Index Key (CIK) number."},
            {"name": "symbol", "type": "string", "description": "Ticker symbol of the company."},
            {"name": "filing_date", "type": "date", "description": "Date when the report was filed."},
            {"name": "accepted_date", "type": "datetime", "description": "Date and time when the report was accepted."},
            {"name": "name_and_position", "type": "string", "description": "Name and position of the executive."},
            {"name": "year", "type": "integer", "description": "Year of the compensation report."},
            {"name": "salary", "type": "float", "description": "Salary of the executive."},
            {"name": "bonus", "type": "float", "description": "Bonus awarded to the executive."},
            {"name": "stock_award", "type": "float", "description": "Stock awards granted to the executive."},
            {"name": "incentive_plan_compensation", "type": "float", "description": "Compensation from incentive plans."},
            {"name": "all_other_compensation", "type": "float", "description": "All other forms of compensation."},
            {"name": "total", "type": "float", "description": "Total compensation."},
            {"name": "url", "type": "string", "description": "URL to the source document."}
        ]
    },
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24 * 30, cron_schedule="0 0 1 * *")  # Adjust the freshness policy as needed
)
def fmp_executive_compensation(fmp_company_profiles: pd.DataFrame) -> pd.DataFrame:
    symbols = fmp_company_profiles['symbol'].tolist()
    df = pd.concat([handle_request(ticker) for ticker in symbols])
    return df

def handle_request(ticker):
    df = make_v4_request('governance/executive_compensation', {'symbol': ticker})
    if df.empty:
        return df
    column_name_mapping = {
        "cik": "cik",
        "symbol": "symbol",
        "filingDate": "filing_date",
        "acceptedDate": "accepted_date",
        "nameAndPosition": "name_and_position",
        "year": "year",
        "salary": "salary",
        "bonus": "bonus",
        "stock_award": "stock_award",
        "incentive_plan_compensation": "incentive_plan_compensation",
        "all_other_compensation": "all_other_compensation",
        "total": "total",
        "url": "url"
    }
    
    df = df.rename(columns=column_name_mapping)
    df['accepted_date'] = pd.to_datetime(df['accepted_date']).dt.tz_localize(None)
    df['filing_date'] = pd.to_datetime(df['filing_date']).dt.date
    return df

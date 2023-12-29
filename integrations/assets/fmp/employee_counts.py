from dagster import asset, FreshnessPolicy
import pandas as pd
from .utils import make_v4_request

@asset(
    metadata={
        "source": "Financial Modeling Prep",
        "name": "Employee Count Data",
        "description": "Historical data on employee counts for various companies, including details like company name, filing date, and report period.",
        "columns": [
            {"name": "symbol", "type": "string", "description": "Ticker symbol of the company."},
            {"name": "cik", "type": "string", "description": "Central Index Key (CIK) number."},
            {"name": "acceptance_time", "type": "datetime", "description": "Time when the report was accepted."},
            {"name": "period_of_report", "type": "date", "description": "Reporting period of the data."},
            {"name": "company_name", "type": "string", "description": "Name of the company."},
            {"name": "form_type", "type": "string", "description": "Type of the form filed."},
            {"name": "filing_date", "type": "date", "description": "Date when the report was filed."},
            {"name": "employee_count", "type": "integer", "description": "Count of employees as reported."},
            {"name": "source", "type": "string", "description": "Source of the data."}
        ]
    },
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24 * 30, cron_schedule="0 0 1 * *")  # Adjust the freshness policy as needed
)
def fmp_employee_counts(fmp_company_profiles: pd.DataFrame) -> pd.DataFrame:
    symbols = fmp_company_profiles['symbol'].tolist()
    df = pd.concat([handle_request(ticker) for ticker in symbols])
    return df

def handle_request(ticker):
    df = make_v4_request('historical/employee_count', {'symbol': ticker})

    column_name_mapping = {
        "symbol": "symbol",
        "cik": "cik",
        "acceptanceTime": "acceptance_time",
        "periodOfReport": "period_of_report",
        "companyName": "company_name",
        "formType": "form_type",
        "filingDate": "filing_date",
        "employeeCount": "employee_count",
        "source": "source"
    }
    
    df = df.rename(columns=column_name_mapping)
    df['acceptance_time'] = pd.to_datetime(df['acceptance_time']).dt.tz_localize(None)
    df['period_of_report'] = pd.to_datetime(df['period_of_report']).dt.date
    df['filing_date'] = pd.to_datetime(df['filing_date']).dt.date
    return df
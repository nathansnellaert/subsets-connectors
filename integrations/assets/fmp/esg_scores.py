from dagster import asset, FreshnessPolicy
import pandas as pd
from .utils import make_v4_request

@asset(
    metadata={
        "source": "Financial Modeling Prep",
        "name": "ESG Scores Data",
        "description": "Retrieves ESG (Environmental, Social, Governance) scores for various companies, providing insights into their sustainability and ethical impacts.",
        "columns": [
            {"name": "symbol", "type": "string", "description": "Ticker symbol of the company."},
            {"name": "cik", "type": "string", "description": "Central Index Key (CIK) number."},
            {"name": "company_name", "type": "string", "description": "Name of the company."},
            {"name": "form_type", "type": "string", "description": "Type of form filed."},
            {"name": "accepted_date", "type": "datetime", "description": "Date and time when the report was accepted."},
            {"name": "date", "type": "date", "description": "Date of the ESG score report."},
            {"name": "environmental_score", "type": "float", "description": "Environmental score of the company."},
            {"name": "social_score", "type": "float", "description": "Social score of the company."},
            {"name": "governance_score", "type": "float", "description": "Governance score of the company."},
            {"name": "esg_score", "type": "float", "description": "Overall ESG score of the company."},
            {"name": "url", "type": "string", "description": "URL to the source document."}
        ]
    },
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24 * 30, cron_schedule="0 0 1 * *")  # Adjust the freshness policy as needed
)
def fmp_esg_scores(fmp_company_profiles: pd.DataFrame) -> pd.DataFrame:
    symbols = fmp_company_profiles['symbol'].tolist()
    esg_scores_df = pd.concat([handle_request(ticker) for ticker in symbols])
    return esg_scores_df

def handle_request(ticker):
    df = make_v4_request('esg-environmental-social-governance-data', {'symbol': ticker})

    column_name_mapping = {
        "symbol": "symbol",
        "cik": "cik",
        "companyName": "company_name",
        "formType": "form_type",
        "acceptedDate": "accepted_date",
        "date": "date",
        "environmentalScore": "environmental_score",
        "socialScore": "social_score",
        "governanceScore": "governance_score",
        "ESGScore": "esg_score",
        "url": "url"
    }
        
    df = df.rename(columns=column_name_mapping)
    df['date'] = pd.to_datetime(df['date']).dt.date
    df['accepted_date'] = pd.to_datetime(df['accepted_date'])
    return df

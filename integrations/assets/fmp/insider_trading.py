from dagster import asset, FreshnessPolicy
import pandas as pd
from .utils import make_v4_request

def convert_mixed_format(date_str):
    try:
        return pd.to_datetime(date_str, format="%Y-%m-%d %H:%M:%S").date()
    except ValueError:
        try:
            return pd.to_datetime(date_str, format="%Y-%m-%d").date()
        except ValueError:
            print(f"Could not parse date: {date_str}")
            return None

@asset(
    metadata={
        "source": "Financial Modeling Prep",
        "name": "Insider Trading Data",
        "description": "Retrieves insider trading data for various companies, providing insights into the trading activities of company insiders.",
        "columns": [
            {"name": "symbol", "type": "string", "description": "Ticker symbol of the company."},
            {"name": "filing_date", "type": "datetime", "description": "Date of filing."},
            {"name": "transaction_date", "type": "date", "description": "Date of the transaction."},
            {"name": "reporting_cik", "type": "string", "description": "Reporting Central Index Key (CIK) number."},
            {"name": "transaction_type", "type": "string", "description": "Type of transaction."},
            {"name": "securities_owned", "type": "int", "description": "Number of securities owned."},
            {"name": "company_cik", "type": "string", "description": "Company CIK number."},
            {"name": "reporting_name", "type": "string", "description": "Name of the reporting individual."},
            {"name": "type_of_owner", "type": "string", "description": "Type of owner."},
            {"name": "acquisition_or_disposition", "type": "string", "description": "Whether the transaction was an acquisition or disposition."},
            {"name": "form_type", "type": "string", "description": "Type of form filed."},
            {"name": "securities_transacted", "type": "float", "description": "Number of securities transacted."},
            {"name": "price", "type": "float", "description": "Price of the security."},
            {"name": "security_name", "type": "string", "description": "Name of the security."}
        ]
    },
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24 * 7, cron_schedule="0 0 1 * *")  # Adjust the freshness policy as needed
)
def fmp_insider_trading(fmp_company_profiles: pd.DataFrame) -> pd.DataFrame:
    symbols = fmp_company_profiles['symbol'].tolist()
    insider_trading_df = pd.concat([handle_request(ticker) for ticker in symbols])
    return insider_trading_df

# Function to handle requests for each ticker symbol
def handle_request(ticker):
    dfs = []
    page = 0
    
    while True:
        df = make_v4_request('insider-trading', {'symbol': ticker, 'page': page})

        if df.empty:
            break
        
        column_name_mapping = {
            "symbol": "symbol",
            "filingDate": "filing_date",
            "transactionDate": "transaction_date",
            "reportingCik": "reporting_cik",
            "transactionType": "transaction_type",
            "securitiesOwned": "securities_owned",
            "companyCik": "company_cik",
            "reportingName": "reporting_name",
            "typeOfOwner": "type_of_owner",
            "acquistionOrDisposition": "acquisition_or_disposition",
            "formType": "form_type",
            "securitiesTransacted": "securities_transacted",
            "price": "price",
            "securityName": "security_name"
        }
        
        df = df.rename(columns=column_name_mapping)

        df['filing_date'] = df['filing_date'].apply(convert_mixed_format)
        df['transaction_date'] = df['transaction_date'].apply(convert_mixed_format)
        
        dfs.append(df)
        page += 1

    if len(dfs) == 0:
        return pd.DataFrame()
    
    return pd.concat(dfs)

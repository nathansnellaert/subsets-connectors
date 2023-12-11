from dagster import asset, FreshnessPolicy
import pandas as pd
import os
import requests
import json

@asset(
    metadata={
        "source": "Financial Modeling Prep",
        "name": "Market Capitalization Data",
        "description": "Retrieves historical market capitalization data for various companies, reflecting the total market value of their outstanding shares.",
        "columns": [
            {"name": "symbol", "type": "string", "description": "Ticker symbol of the company."},
            {"name": "date", "type": "date", "description": "Date of the market capitalization record."},
            {"name": "market_cap", "type": "float", "description": "Market capitalization of the company on the given date."}
        ]
    },
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24 * 30, cron_schedule="0 0 1 * *")  # Adjust the freshness policy as needed
)
def fmp_market_cap(fmp_company_profiles: pd.DataFrame) -> pd.DataFrame:
    symbols = fmp_company_profiles['symbol'].tolist()
    df = pd.concat([handle_request(ticker) for ticker in symbols])
    return df.dropna(how='all')

def handle_request(ticker):
    BASE_URL = 'https://financialmodelingprep.com/api/v3/'
    url = BASE_URL + f'historical-market-capitalization/{ticker}?apikey=' + os.environ['FMP_API_KEY']
    response = requests.get(url)
    
    data = json.loads(response.text)
    df = pd.DataFrame(data)
    
    if df.empty:
        return pd.DataFrame()
    
    column_name_mapping = {
        "symbol": "symbol",
        "date": "date",
        "marketCap": "market_cap"
    }
    
    df = df.rename(columns=column_name_mapping)
    df['date'] = pd.to_datetime(df['date']).dt.date
    
    return df

from dagster import asset, FreshnessPolicy
import pandas as pd
from .utils import make_v3_request

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
    return df

def handle_request(ticker):
    df = make_v3_request(f'historical-market-capitalization/{ticker}', {})    
    column_name_mapping = {
        "symbol": "symbol",
        "date": "date",
        "marketCap": "market_cap"
    }
    df = df.rename(columns=column_name_mapping)
    df['date'] = pd.to_datetime(df['date']).dt.date
    return df

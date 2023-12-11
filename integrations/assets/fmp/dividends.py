from dagster import asset, FreshnessPolicy
import pandas as pd
import os
import json
import requests

@asset(
    metadata={
        "source": "Financial Modeling Prep",
        "name": "Stock Dividends Data",
        "description": "Retrieves historical dividend data for stocks, including adjusted dividends, record, payment, and declaration dates.",
        "columns": [
            {"name": "date", "type": "date", "description": "Date of the dividend."},
            {"name": "label", "type": "string", "description": "Label of the dividend event."},
            {"name": "adj_dividend", "type": "float", "description": "Adjusted dividend value."},
            {"name": "dividend", "type": "float", "description": "Dividend value."},
            {"name": "record_date", "type": "date", "description": "Record date of the dividend."},
            {"name": "payment_date", "type": "date", "description": "Payment date of the dividend."},
            {"name": "declaration_date", "type": "date", "description": "Declaration date of the dividend."},
            {"name": "symbol", "type": "string", "description": "Stock ticker symbol."}
        ]
    },
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24 * 30, cron_schedule="0 0 1 * *")
)
def fmp_dividends(fmp_company_profiles: pd.DataFrame) -> pd.DataFrame:
    symbols = fmp_company_profiles['symbol'].tolist()
    df = pd.concat([handle_request(ticker) for ticker in symbols])
    return df.dropna(how='all')

def handle_request(ticker):
    BASE_URL = 'https://financialmodelingprep.com/api/v3/'
    url = BASE_URL + f'historical-price-full/stock_dividend/{ticker}?apikey=' + os.environ['FMP_API_KEY']
    response = requests.get(url)
    data = json.loads(response.text)['historical']
    df = pd.DataFrame(data)
    
    if df.empty:
        return df
    
    column_name_mapping = {
        "date": "date",
        "label": "label",
        "adjDividend": "adj_dividend",
        "dividend": "dividend",
        "recordDate": "record_date",
        "paymentDate": "payment_date",
        "declarationDate": "declaration_date"
    }
    
    df = df.rename(columns=column_name_mapping)
    df['symbol'] = ticker
    df['date'] = pd.to_datetime(df['date']).dt.date
    df['record_date'] = pd.to_datetime(df['record_date']).dt.date
    df['payment_date'] = pd.to_datetime(df['payment_date']).dt.date
    df['declaration_date'] = pd.to_datetime(df['declaration_date']).dt.date
    return df

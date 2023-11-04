from dagster import asset
import pandas as pd
import os
import json
import requests

@asset(metadata={
    "source": "Financial Modeling Prep",
    "name": "Stock Dividend",
    "description": "Historical dividend data for stocks including the dividend amount, record, payment, and declaration dates.",
    "columns": [{
        "name": "date",
        "description": "The date of the dividend event"
    }, {
        "name": "label",
        "description": "The label or description of the dividend event"
    }, {
        "name": "adj_dividend",
        "description": "The adjusted dividend value"
    }, {
        "name": "dividend",
        "description": "The actual dividend value"
    }, {
        "name": "record_date",
        "description": "The record date for the dividend"
    }, {
        "name": "payment_date",
        "description": "The payment date for the dividend"
    }, {
        "name": "declaration_date",
        "description": "The declaration date for the dividend"
    }, {
        "name": "symbol",
        "description": "The stock ticker symbol"
    }]
})
def stock_dividend(ticker):
    BASE_URL = 'https://financialmodelingprep.com/api/v3/'
    url = BASE_URL + f'historical-price-full/stock_dividend/{ticker}?apikey=' + os.environ['FMP_API_KEY']
    response = requests.get(url)
    data = json.loads(response.text).get('historical', [])
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

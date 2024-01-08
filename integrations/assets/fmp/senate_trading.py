from dagster import asset, FreshnessPolicy
import pandas as pd
from .utils import make_v4_request
from .source import financialmodellingprep

def handle_request(ticker):
    df = make_v4_request('senate-trading', {'symbol': ticker})
    if df.empty:
        return df
    column_name_mapping = {
        "firstName": "first_name",
        "lastName": "last_name",
        "office": "office",
        "link": "link",
        "dateRecieved": "date_received",
        "transactionDate": "transaction_date",
        "owner": "owner",
        "assetDescription": "asset_description",
        "assetType": "asset_type",
        "type": "type",
        "amount": "amount",
        "comment": "comment",
        "symbol": "symbol"
    }
    
    df = df.rename(columns=column_name_mapping)
    df['date_received'] = pd.to_datetime(df['date_received']).dt.date
    df['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.date
    return df

@asset(metadata={
    "source": financialmodellingprep,
    "name": "Senate Trading",
    "description": "Records of stock trades made by US senators, including the transaction date, amount, and other details.",
    "columns": [{
        "name": "first_name",
        "description": "First name of the senator"
    }, {
        "name": "last_name",
        "description": "Last name of the senator"
    }, {
        "name": "office",
        "description": "Office or title held by the senator"
    }, {
        "name": "link",
        "description": "Link to the detailed transaction record"
    }, {
        "name": "date_received",
        "description": "Date when the transaction was received"
    }, {
        "name": "transaction_date",
        "description": "Date when the transaction occurred"
    }, {
        "name": "owner",
        "description": "Owner of the asset, whether the senator or an immediate family member"
    }, {
        "name": "asset_description",
        "description": "Description of the asset traded"
    }, {
        "name": "asset_type",
        "description": "Type of the asset traded"
    }, {
        "name": "type",
        "description": "Type of transaction, e.g., purchase, sale"
    }, {
        "name": "amount",
        "description": "Amount of the transaction"
    }, {
        "name": "comment",
        "description": "Any additional comments on the transaction"
    }, {
        "name": "symbol",
        "description": "Ticker symbol of the traded asset"
    }]
}, freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24, cron_schedule="0 0 * * *"))
def senator_trading(fmp_company_profiles: pd.DataFrame) -> pd.DataFrame:
    symbols = fmp_company_profiles['symbol'].tolist()
    df = pd.concat([handle_request(ticker) for ticker in symbols])
    return df
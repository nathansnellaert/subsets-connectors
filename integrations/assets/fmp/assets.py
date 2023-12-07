import os
import pandas as pd
from dagster import asset, Partition, StaticPartitionsDefinition, DailyPartitionsDefinition
from ratelimit import limits, sleep_and_retry
from tenacity import retry, stop_after_attempt, wait_exponential
import requests
from datetime import datetime
import json

current_dt = datetime.now()

yearly_partitions = [str(year) for year in range(1985, current_dt.year + 1)]

@sleep_and_retry
@limits(calls=750, period=60)
@retry(wait=wait_exponential(multiplier=2), stop=stop_after_attempt(5), reraise=True)
def make_request(path) -> pd.DataFrame:
    BASE_URL = 'https://financialmodelingprep.com/api/v4/'
    url = BASE_URL + path
    df = pd.read_csv(url)
    return df

@asset
def fmp_company_profile():
    path = 'profile/all?datatype=csv&apikey=' + os.environ['FMP_API_KEY']
    df = make_request(path)

    column_name_mapping = {
        "Symbol": "symbol",
        "Price": "price",
        "Beta": "beta",
        "VolAvg": "vol_avg",
        "MktCap": "market_cap",
        "LastDiv": "last_div",
        "Range": "range",
        "Changes": "changes",
        "companyName": "company_name",
        "currency": "currency",
        "cik": "cik",
        "isin": "isin",
        "cusip": "cusip",
        "exchange": "exchange",
        "exchangeShortName": "exchange_short_name",
        "industry": "industry",
        "website": "website",
        "description": "description",
        "CEO": "ceo",
        "sector": "sector",
        "country": "country",
        "fullTimeEmployees": "full_time_employees",
        "phone": "phone",
        "address": "address",
        "city": "city",
        "state": "state",
        "zip": "zip",
        "DCF_diff": "dcf_diff",
        "DCF": "dcf",
        "image": "image",
        "ipoDate": "ipo_date",
        "defaultImage": "default_image",
        "isEtf": "is_etf",
        "isActivelyTrading": "is_actively_trading",
        "isFund": "is_fund",
        "isAdr": "is_adr"
    }
    df = df.rename(columns=column_name_mapping)
    return df


@asset(partitions_def=StaticPartitionsDefinition(yearly_partitions))
def fmp_balance_sheet(context) -> pd.DataFrame:
    path = f'balance-sheet-statement-bulk?year={context.partition_key}&period=quarter&datatype=csv&apikey=' + os.environ['FMP_API_KEY']
    return make_request(path)

@asset(partitions_def=StaticPartitionsDefinition(yearly_partitions))
def fmp_cash_flow_statement(context) -> pd.DataFrame:
    path = f'cash-flow-statement-bulk?year={context.partition_key}&period=quarter&datatype=csv&apikey=' + os.environ['FMP_API_KEY']
    return make_request(path)

@asset(partitions_def=StaticPartitionsDefinition(yearly_partitions))
def fmp_income_statement(context) -> pd.DataFrame:
    path = f'income-statement-bulk?year={context.partition_key}&period=quarter&datatype=csv&apikey=' + os.environ['FMP_API_KEY']
    return make_request(path)

@asset(partitions_def=StaticPartitionsDefinition(yearly_partitions))
def fmp_key_metrics(context) -> pd.DataFrame:
    path = f'key-metrics-bulk?year={context.partition_key}&period=quarter&datatype=csv&apikey=' + os.environ['FMP_API_KEY']
    return make_request(path)

@asset
def fmp_commodity_symbols() -> pd.DataFrame:
    url = "https://financialmodelingprep.com/api/v3/symbol/available-commodities?apikey=" + os.environ['FMP_API_KEY']
    response = requests.get(url).json()
    return pd.DataFrame(response)

@asset
def fmp_crypto_symbols() -> pd.DataFrame:
    url = "https://financialmodelingprep.com/api/v3/symbol/available-cryptocurrencies?apikey=" + os.environ['FMP_API_KEY']
    response = requests.get(url).json()
    return pd.DataFrame(response)

@asset
def fmp_forex_symbols() -> pd.DataFrame:
    url = "https://financialmodelingprep.com/api/v3/symbol/available-forex-currency-pairs?apikey=" + os.environ['FMP_API_KEY']
    response = requests.get(url).json()
    return pd.DataFrame(response)

@asset
def fmp_indices_symbols() -> list:
    url = "https://financialmodelingprep.com/api/v3/quotes/index?apikey=" + os.environ['FMP_API_KEY']
    response = requests.get(url)
    data = json.loads(response.text)
    symbols_data = [(entry['symbol'], entry['name']) for entry in data]
    return symbols_data


@asset(partitions_def=DailyPartitionsDefinition(start_date="2013-01-01"))
def fmp_eod_prices(context) -> pd.DataFrame:
    path = f'batch-request-end-of-day-prices?date={context.partition_key}&apikey=' + os.environ['FMP_API_KEY']
    return make_request(path)


@asset(partitions_def=DailyPartitionsDefinition(start_date="2013-01-01"))
def cryptocurrency_prices(fmp_crypto_symbols: pd.DataFrame, fmp_eod_prices: pd.DataFrame) -> pd.DataFrame:
    crypto_symbols = fmp_crypto_symbols['symbol'].unique()
    crypto_prices = fmp_eod_prices[fmp_eod_prices['symbol'].isin(crypto_symbols)]
    return crypto_prices


@asset(partitions_def=DailyPartitionsDefinition(start_date="2013-01-01"))
def commodity_prices(fmp_commodity_symbols: pd.DataFrame, fmp_eod_prices: pd.DataFrame) -> pd.DataFrame:
    commodity_symbols = fmp_commodity_symbols['symbol'].unique()
    commodity_prices = fmp_eod_prices[fmp_eod_prices['symbol'].isin(commodity_symbols)]
    return commodity_prices


@asset(partitions_def=DailyPartitionsDefinition(start_date="2013-01-01"))
def forex_prices(fmp_forex_symbols: pd.DataFrame, fmp_eod_prices: pd.DataFrame) -> pd.DataFrame:
    forex_symbols = fmp_forex_symbols['symbol'].unique()
    forex_prices = fmp_eod_prices[fmp_eod_prices['symbol'].isin(forex_symbols)]
    return forex_prices


@asset(partitions_def=DailyPartitionsDefinition(start_date="2013-01-01"))
def indices_prices(fmp_indices_symbols: list, fmp_eod_prices: pd.DataFrame) -> pd.DataFrame:
    indices_symbols = [symbol for symbol, name in fmp_indices_symbols]
    indices_prices = fmp_eod_prices[fmp_eod_prices['symbol'].isin(indices_symbols)]
    return indices_prices

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

@asset(partitions_def=DailyPartitionsDefinition(start_date="2014-01-01"))
def fmp_eod_prices(context) -> pd.DataFrame:
    path = f'batch-request-end-of-day-prices?date={context.partition_key}&apikey=' + os.environ['FMP_API_KEY']
    return make_request(path)


@asset(partitions_def=DailyPartitionsDefinition(start_date="2014-01-01"))
def cryptocurrency_prices(fmp_crypto_symbols: pd.DataFrame, fmp_eod_prices: pd.DataFrame) -> pd.DataFrame:
    crypto_symbols = fmp_crypto_symbols['symbol'].unique()
    crypto_prices = fmp_eod_prices[fmp_eod_prices['symbol'].isin(crypto_symbols)]
    return crypto_prices


@asset(partitions_def=DailyPartitionsDefinition(start_date="2014-01-01"))
def commodity_prices(fmp_commodity_symbols: pd.DataFrame, fmp_eod_prices: pd.DataFrame) -> pd.DataFrame:
    commodity_symbols = fmp_commodity_symbols['symbol'].unique()
    commodity_prices = fmp_eod_prices[fmp_eod_prices['symbol'].isin(commodity_symbols)]
    return commodity_prices


@asset(partitions_def=DailyPartitionsDefinition(start_date="2014-01-01"))
def forex_prices(fmp_forex_symbols: pd.DataFrame, fmp_eod_prices: pd.DataFrame) -> pd.DataFrame:
    forex_symbols = fmp_forex_symbols['symbol'].unique()
    forex_prices = fmp_eod_prices[fmp_eod_prices['symbol'].isin(forex_symbols)]
    return forex_prices


@asset(partitions_def=DailyPartitionsDefinition(start_date="2014-01-01"))
def indices_prices(fmp_indices_symbols: list, fmp_eod_prices: pd.DataFrame) -> pd.DataFrame:
    indices_symbols = [symbol for symbol, name in fmp_indices_symbols]
    indices_prices = fmp_eod_prices[fmp_eod_prices['symbol'].isin(indices_symbols)]
    return indices_prices

@asset(partitions_def=DailyPartitionsDefinition(start_date="2014-01-01"))
def etf_prices(fmp_etf_symbols: list, fmp_eod_prices: pd.DataFrame) -> pd.DataFrame:
    etf_symbols = [symbol for symbol, name in fmp_etf_symbols]
    etf_prices = fmp_eod_prices[fmp_eod_prices['symbol'].isin(etf_symbols)]
    return etf_prices

import os
import pandas as pd
from dagster import asset, DailyPartitionsDefinition
from datetime import datetime
from .utils import make_v4_request
current_dt = datetime.now()

@asset(partitions_def=DailyPartitionsDefinition(start_date="2014-01-01"))
def fmp_eod_prices(context) -> pd.DataFrame:
    df = make_v4_request('batch-request-end-of-day-prices', {'date': context.partition_key})
    df['volume'] = df['volume'].astype(int)
    return df

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

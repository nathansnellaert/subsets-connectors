from dagster import asset, FreshnessPolicy
import pandas as pd
import os
import requests
import json
from .utils import make_v4_request, make_v3_request

@asset(
    metadata={
        "source": "Financial Modeling Prep",
        "name": "Company Profiles Data",
        "description": "Retrieves comprehensive profiles for a wide range of companies, including key financial metrics, industry classification, and corporate information.",
        "columns": [
            {"name": "symbol", "type": "string", "description": "Ticker symbol of the company."},
            {"name": "price", "type": "float", "description": "Current stock price."},
            {"name": "beta", "type": "float", "description": "Beta value measuring the volatility of the stock."},
            {"name": "vol_avg", "type": "float", "description": "Average volume of stock traded."},
            {"name": "market_cap", "type": "float", "description": "Market capitalization value."},
            {"name": "last_div", "type": "float", "description": "Value of the last dividend paid."},
            {"name": "range", "type": "string", "description": "52-week trading range of the stock."},
            {"name": "changes", "type": "float", "description": "Change in stock price."},
            {"name": "company_name", "type": "string", "description": "Name of the company."},
            {"name": "currency", "type": "string", "description": "Currency in which financials are reported."},
            {"name": "cik", "type": "string", "description": "Central Index Key (CIK) number."},
            {"name": "isin", "type": "string", "description": "International Securities Identification Number (ISIN)."},
            {"name": "cusip", "type": "string", "description": "Committee on Uniform Securities Identification Procedures (CUSIP) number."},
            {"name": "exchange", "type": "string", "description": "Stock exchange where the company is listed."},
            {"name": "exchange_short_name", "type": "string", "description": "Short name of the exchange."},
            {"name": "industry", "type": "string", "description": "Industry in which the company operates."},
            {"name": "website", "type": "string", "description": "Company's official website."},
            {"name": "description", "type": "string", "description": "Brief description of the company."},
            {"name": "ceo", "type": "string", "description": "Chief Executive Officer of the company."},
            {"name": "sector", "type": "string", "description": "Sector to which the company belongs."},
            {"name": "country", "type": "string", "description": "Country where the company is headquartered."},
            {"name": "full_time_employees", "type": "integer", "description": "Number of full-time employees."},
            {"name": "phone", "type": "string", "description": "Contact phone number of the company."},
            {"name": "address", "type": "string", "description": "Address of the company's headquarters."},
            {"name": "city", "type": "string", "description": "City where the company is headquartered."},
            {"name": "state", "type": "string", "description": "State where the company is headquartered."},
            {"name": "zip", "type": "string", "description": "ZIP code of the company's headquarters."},
            {"name": "dcf_diff", "type": "float", "description": "Difference in Discounted Cash Flow (DCF)."},
            {"name": "dcf", "type": "float", "description": "Discounted Cash Flow (DCF) value."},
            {"name": "image", "type": "string", "description": "URL to the company's logo image."},
            {"name": "ipo_date", "type": "date", "description": "Date of the company's initial public offering (IPO)."},
            {"name": "default_image", "type": "boolean", "description": "Indicates whether the image is a default image."},
            {"name": "is_etf", "type": "boolean", "description": "Indicates if the security is an Exchange-Traded Fund (ETF)."},
            {"name": "is_actively_trading", "type": "boolean", "description": "Indicates whether the company is actively trading."},
            {"name": "is_fund", "type": "boolean", "description": "Indicates if the security is a mutual fund."},
            {"name": "is_adr", "type": "boolean", "description": "Indicates if the security is an American Depositary Receipt (ADR)."}
        ]
    },
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24 * 30, cron_schedule="0 0 1 * *")  # Adjust the freshness policy as needed
)
def fmp_company_profiles():
    df = make_v4_request('profile/all', {})
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

@asset
def fmp_commodity_symbols() -> pd.DataFrame:
    return make_v3_request('symbol/available-commodities', {})

@asset
def fmp_crypto_symbols() -> pd.DataFrame:
    return make_v3_request('symbol/available-cryptocurrencies', {})

@asset
def fmp_forex_symbols() -> pd.DataFrame:
    return make_v3_request('symbol/available-forex-currency-pairs', {})

@asset
def fmp_indices_symbols() -> pd.DataFrame:
    return make_v3_request('symbol/available-indexes', {})

@asset
def fmp_etf_symbols():
    return make_v3_request('etf/list', {})

@asset
def fmp_stock_symbols():
    return make_v3_request('stock/list', {})

@asset
def delisted_company_symbols():
    return make_v3_request('delisted-companies', {})

@asset(
    metadata={
        "source": "Financial Modeling Prep",
        "name": "CIK List Data",
        "description": "Provides a comprehensive database of CIK numbers for SEC-registered entities.",
        "columns": [
            {"name": "cik", "type": "string", "description": "Central Index Key (CIK) number, a unique identifier for each SEC-registered entity."},
            {"name": "name", "type": "string", "description": "Name of the SEC-registered entity."}
        ]
    }
)
def cik_list() -> pd.DataFrame:
    return make_v3_request('cik_list', {})


@asset(
    metadata={
        "source": "Financial Modeling Prep",
        "name": "Symbol Changes Data",
        "description": "Tracks symbol changes due to mergers, acquisitions, stock splits, and name changes.",
        "columns": [
            {"name": "date", "type": "date", "description": "Date of the symbol change."},
            {"name": "name", "type": "string", "description": "Name of the company."},
            {"name": "oldSymbol", "type": "string", "description": "Old ticker symbol of the company."},
            {"name": "newSymbol", "type": "string", "description": "New ticker symbol of the company."}
        ]
    }
)
def fmp_symbol_changes() -> pd.DataFrame:
    df = make_v4_request('symbol_change', {})
    df['date'] = pd.to_datetime(df['date']).dt.date 
    return df

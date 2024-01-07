import os 
import pandas as pd

def make_v4_request(route, args={}):
    BASE_URL = 'https://financialmodelingprep.com/api/v4/'
    default_args = {
        "apikey": os.environ['FMP_API_KEY'],
        "datatype": "csv",
    }
    all_args = {**default_args, **args}
    full_url = BASE_URL + route + "?" + "&".join([f"{key}={value}" for key, value in all_args.items()])
    try:
        return pd.read_csv(full_url)
    except pd.errors.ParserError as e:
        print('Error parsing CSV', full_url)
        return pd.DataFrame()

def make_v3_request(route, args={}):
    BASE_URL = 'https://financialmodelingprep.com/api/v3/'
    default_args = {
        "apikey": os.environ['FMP_API_KEY'],
        "datatype": "csv",
    }
    all_args = {**default_args, **args}
    full_url = BASE_URL + route + "?" + "&".join([f"{key}={value}" for key, value in all_args.items()])
    try:
        return pd.read_csv(full_url)
    except pd.errors.ParserError as e:
        print('Error parsing CSV', full_url)
        return pd.DataFrame()
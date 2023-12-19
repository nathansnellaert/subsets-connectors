from dagster import asset, FreshnessPolicy
import pandas as pd
import os
import requests
import json

@asset(
    metadata={
        "source": "Financial Modeling Prep",
        "name": "Stock Grade Data",
        "description": "Retrieves grading data for stocks, including historical and current grades assigned by various grading companies.",
        "columns": [
            {"name": "symbol", "type": "string", "description": "Ticker symbol of the company."},
            {"name": "date", "type": "date", "description": "Date of the grade."},
            {"name": "grading_company", "type": "string", "description": "Company that issued the grade."},
            {"name": "previous_grade", "type": "string", "description": "Previous grade of the stock."},
            {"name": "new_grade", "type": "string", "description": "New grade of the stock."}
        ]
    },
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24 * 7, cron_schedule="0 0 1 * *")  # Adjust the freshness policy as needed
)
def fmp_stock_grade(fmp_company_profiles: pd.DataFrame) -> pd.DataFrame:
    symbols = fmp_company_profiles['symbol'].tolist()
    stock_grade_df = pd.concat([handle_request(symbol) for symbol in symbols])
    return stock_grade_df.dropna(how='all')

def handle_request(symbol):
    BASE_URL = 'https://financialmodelingprep.com/api/v3/'
    url = BASE_URL + f'grade/{symbol}?apikey=' + os.environ['FMP_API_KEY']
    response = requests.get(url)
    data = json.loads(response.text)
    df = pd.DataFrame(data)
    
    if df.empty:
        return df
    
    column_name_mapping = {
        "symbol": "symbol",
        "date": "date",
        "gradingCompany": "grading_company",
        "previousGrade": "previous_grade",
        "newGrade": "new_grade"
    }
    
    df = df.rename(columns=column_name_mapping)
    df['date'] = pd.to_datetime(df['date']).dt.date
    return df
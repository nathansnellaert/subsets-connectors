from dagster import asset, FreshnessPolicy
import pandas as pd
import os
import requests
import json

@asset(
    metadata={
        "source": "Financial Modeling Prep",
        "name": "Upgrades and Downgrades Data",
        "description": "Retrieves data on upgrades and downgrades of company stocks, including the details of the analyst reports and the impact on stock prices.",
        "columns": [
            {"name": "symbol", "type": "string", "description": "Ticker symbol of the company."},
            {"name": "published_date", "type": "datetime", "description": "Date when the upgrade/downgrade was published."},
            {"name": "news_url", "type": "string", "description": "URL of the news article."},
            {"name": "news_title", "type": "string", "description": "Title of the news article."},
            {"name": "news_base_url", "type": "string", "description": "Base URL of the news source."},
            {"name": "news_publisher", "type": "string", "description": "Publisher of the news."},
            {"name": "new_grade", "type": "string", "description": "New grade given to the company."},
            {"name": "previous_grade", "type": "string", "description": "Previous grade of the company."},
            {"name": "grading_company", "type": "string", "description": "Company that issued the grade."},
            {"name": "action", "type": "string", "description": "Action taken (upgrade/downgrade)."},
            {"name": "price_when_posted", "type": "float", "description": "Price of the stock when the report was posted."}
        ]
    },
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24 * 7, cron_schedule="0 0 1 * *")
)
def fmp_upgrades_downgrades(fmp_company_profiles: pd.DataFrame) -> pd.DataFrame:
    symbols = fmp_company_profiles['symbol'].tolist()
    upgrades_downgrades_df = pd.concat([handle_request(ticker) for ticker in symbols])
    return upgrades_downgrades_df.dropna(how='all')

def handle_request(ticker):
    BASE_URL = 'https://financialmodelingprep.com/api/v4/'
    url = BASE_URL + f'upgrades-downgrades?symbol={ticker}&apikey=' + os.environ['FMP_API_KEY']
    response = requests.get(url)
    data = json.loads(response.text)
    df = pd.DataFrame(data)
    
    if df.empty:
        return df
    
    column_name_mapping = {
        "symbol": "symbol",
        "publishedDate": "published_date",
        "newsURL": "news_url",
        "newsTitle": "news_title",
        "newsBaseURL": "news_base_url",
        "newsPublisher": "news_publisher",
        "newGrade": "new_grade",
        "previousGrade": "previous_grade",
        "gradingCompany": "grading_company",
        "action": "action",
        "priceWhenPosted": "price_when_posted"
    }
    
    df = df.rename(columns=column_name_mapping)

    # remove \n from published_date (bug fix)
    df['published_date'] = df['published_date'].str.replace('\n', '')
    df['published_date'] = pd.to_datetime(df['published_date']).dt.date
    return df

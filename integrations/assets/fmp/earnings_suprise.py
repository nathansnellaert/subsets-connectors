from dagster import asset, FreshnessPolicy
import pandas as pd
from .utils import make_v4_request

@asset(
    metadata={
        "source": "Financial Modeling Prep",
        "name": "Earnings Surprises Data",
        "description": "Retrieves data on earnings surprises, comparing actual earnings results with estimated earnings for various companies over time.",
        "columns": [
            {"name": "date", "type": "date", "description": "Date of the earnings report."},
            {"name": "symbol", "type": "string", "description": "Ticker symbol of the company."},
            {"name": "actual_earning_result", "type": "float", "description": "Actual earnings result reported by the company."},
            {"name": "estimated_earning", "type": "float", "description": "Estimated earnings projected for the company."}
        ]
    },
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24 * 30, cron_schedule="0 0 1 * *")  # Adjust the freshness policy as needed
)
def fmp_earnings_surprises():
    dfs = [handle_request(year) for year in range(1985, 2024)]
    df = pd.concat(dfs)
    return df

def handle_request(year):
    df = make_v4_request('earnings-surprises-bulk', {'year': year, 'period': 'quarter'})
    column_name_mapping = {
	    "date": "date",
	    "symbol": "symbol",
	    "actualEarningResult": "actual_earning_result",
	    "estimatedEarning": "estimated_earning"
	}
    df = df.rename(columns=column_name_mapping)
    df['date'] = pd.to_datetime(df['date']).dt.date
    return df

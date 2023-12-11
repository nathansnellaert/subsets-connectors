import pandas as pd
from dagster import asset, FreshnessPolicy
from .download import download_ycombinator_companies

@asset(metadata={
    "source": "ycombinator",
    "name": "YCombinator Companies",
    "description": "Detailed data of companies that have participated in Y Combinator's accelerator program.",
}, freshness_policy=FreshnessPolicy(cron_schedule="0 0 * * *", maximum_lag_minutes=60 * 24))
def ycombinator_companies():
    start_year = 2005
    end_year = 2024

    # static algolia config that powers https://ycombinator.com/companies
    algolia_config = {
        "url": "https://45bwzj1sgc-dsn.algolia.net/1/indexes/*/queries",
        "app_id": "45BWZJ1SGC",
        "api_key": "Zjk5ZmFjMzg2NmQxNTA0NGM5OGNiNWY4MzQ0NDUyNTg0MDZjMzdmMWY1NTU2YzZkZGVmYjg1ZGZjMGJlYjhkN3Jlc3RyaWN0SW5kaWNlcz1ZQ0NvbXBhbnlfcHJvZHVjdGlvbiZ0YWdGaWx0ZXJzPSU1QiUyMnljZGNfcHVibGljJTIyJTVEJmFuYWx5dGljc1RhZ3M9JTVCJTIyeWNkYyUyMiU1RA%3D%3D",
        "index_name": "YCCompany_production" 
    }

    companies = download_ycombinator_companies(start_year, end_year, algolia_config)
    df = pd.DataFrame(companies)
    return df

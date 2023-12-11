from dagster import asset, FreshnessPolicy
import pandas as pd
import requests

@asset(metadata={
    "source": "tsa",
    "name": "TSA checkpoint travel numbers",
    "description": "The number of travelers passing through TSA checkpoints per day.",
}, freshness_policy=FreshnessPolicy(cron_schedule="0 0 * * *", maximum_lag_minutes=60 * 24))
def tsa_checkpoint_travel_numbers():
    url = "https://www.tsa.gov/travel/passenger-volumes"
    # 403 if directly using read_html, so load with requests
    html = requests.get(url).text
    df = pd.read_html(html, attrs={'class': 'views-table'})[0]
    # Step 1: Melt the DataFrame from wide to long format
    df_long = df.melt(id_vars='Date', var_name='Year', value_name='Value')
    # Step 2: Extract the month and day from the 'Date' column, and combine it with the 'Year' column to create a new date column
    df_long['Date'] = df_long.apply(lambda row: f'{row.Date.split("/")[0]}/{row.Date.split("/")[1]}/{row.Year}', axis=1)
    # Step 3: Convert the 'Date' column to datetime format, if desired
    df_long['Date'] = pd.to_datetime(df_long['Date'], format='%m/%d/%Y')
    return df_long
import pandas as pd
from dagster import asset, FreshnessPolicy

our_world_in_data = {
    "id": "our_world_in_data",
    "name": "Our World in Data",
    "description": "Our World in Data is a scientific online publication that focuses on large global problems such as poverty, disease, hunger, climate change, war, existential risks, and inequality.",
    "url": "https://ourworldindata.org/"

}

@asset(metadata={
    "source": our_world_in_data,
    "name": "COVID-19 Statistics",
    "description": "Daily COVID-19 cases and deaths data by country."
}, freshness_policy=FreshnessPolicy(cron_schedule="0 0 * * *", maximum_lag_minutes=60 * 24))
def covid_stats(countries):
    url = "https://covid.ourworldindata.org/data/owid-covid-data.csv"
    df = pd.read_csv(url)

    country_code_mapping = countries[['country_code2', 'country_code3']] 
    df = df.merge(country_code_mapping, left_on='iso_code', right_on='country_code3', how='left')

    df = df[['date', 'country_code2', 'new_cases', 'total_cases', 'new_deaths', 'total_deaths']]    
    return df
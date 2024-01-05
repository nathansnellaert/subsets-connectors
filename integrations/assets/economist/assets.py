from dagster import asset, FreshnessPolicy
import pandas as pd

@asset(metadata={
    "source": "economist",
    "name": "The Big Mac Index",
    "description": "The Big Mac Index compares relative price levels across countries using the price of McDonald's Big Mac.",
    "columns": [{
        "name": "country_code2",
        "description": "Two-letter country code"
    }, {
        "name": "country_name",
        "description": "Name of the country"
    }, {
        "name": "date",
        "description": "Date of the data"
    }, {
        "name": "dollar_price",
        "description": "Price of a Big Mac in USD"
    }, {
        "name": "adjusted_dollar_price",
        "description": "Price of a Big Mac in USD adjusted for GDP"
    }]
}, freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24 * 7, cron_schedule="0 0 1 * *"))
def economist_big_mac_index(countries):
    url = "https://raw.githubusercontent.com/TheEconomist/big-mac-data/master/output-data/big-mac-full-index.csv"
    df = pd.read_csv(url)

    # join country_code3 on iso_a3
    df = df.merge(countries[['country_code2', 'country_code3', 'country_name']], left_on="iso_a3", right_on="country_code3", how="left")
    df = df.rename(columns={'adj_price': 'adjusted_dollar_price'})
    # only keep relevant columns
    df = df[['country_code2', 'country_name', 'date', 'dollar_price', 'adjusted_dollar_price']]
    return df
    
from dagster import asset, FreshnessPolicy
import pandas as pd

source = {
    "id": "economist",
    "logo": "https://storage.googleapis.com/subsets-public-assets/source_logos/economist.png",
    "name": "The Economist",
    "description": "The Economist is an international weekly newspaper printed in magazine-format and published digitally that focuses on current affairs, international business, politics, and technology.",
    "url": "https://www.economist.com/"
}

@asset(metadata={
    "source": source,
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
def economist_big_mac_index():
    url = "https://raw.githubusercontent.com/TheEconomist/big-mac-data/master/output-data/big-mac-full-index.csv"
    df = pd.read_csv(url)
    return df
    
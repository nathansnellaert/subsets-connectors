import requests
import os
from dagster import asset, AssetKey
import pandas as pd
from ratelimit import limits, sleep_and_retry
from tenacity import retry, stop_after_attempt, wait_exponential

@sleep_and_retry
@limits(calls=100, period=60)
@retry(wait=wait_exponential(multiplier=2), stop=stop_after_attempt(5), reraise=True)
def make_request(url):
    response = requests.get(url)
    response.raise_for_status() 
    return response.json()


def get_child_categories(category_id, api_key):
    url = f"https://api.stlouisfed.org/fred/category/children?category_id={category_id}&file_type=json&api_key={api_key}"
    return make_request(url)['categories']

def get_series_metadata_for_category(api_key, category_id):
    url = f"https://api.stlouisfed.org/fred/category/series?category_id={category_id}&api_key={api_key}&file_type=json"
    df = pd.DataFrame(make_request(url)['seriess'])
    df['category_id'] = category_id
    return df

def get_series_observations(series_id, api_key):
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={api_key}&file_type=json"
    df = pd.DataFrame(make_request(url)['observations'])[['date', 'value']]
    df['id'] = series_id
    return df

def get_series_for_release(release_id, api_key):
    url = f"https://api.stlouisfed.org/fred/release/series?release_id={release_id}&api_key={api_key}&file_type=json"
    return make_request(url)['seriess']['id'].values().tolist()

def get_category_tree(category_id, api_key):
    child_categories = get_child_categories(category_id, api_key)
    return child_categories + [item for child in child_categories for item in get_category_tree(child['id'], api_key)]

@asset(metadata={
    "source": "fred",
    "name": "Federal Reserve Economic Category Taxonomy",
    "description": "A hierarchical structure of economic categories from the Federal Reserve Economic Data (FRED) API.",
    "columns": [{
        "name": "id",
        "description": "Unique identifier for the economic category."
    }, {
        "name": "name",
        "description": "Name of the economic category."
    }, {
        "name": "parent_id",
        "description": "Identifier of the parent category for nested categorization."
    }]
})
def fred_category_taxonomy() -> pd.DataFrame:
    api_key = os.getenv("FRED_API_KEY")
    categories = get_category_tree('0', api_key)
    return pd.DataFrame(categories, columns=["id", "name", "parent_id"])


@asset(metadata={
    "source": "fred",
    "name": "Federal Reserve Economic Series Metadata",
    "description": "Metadata for all time series from the Federal Reserve Economic Data (FRED) API.",
    "columns": [{
        "name": "id",
        "description": "Unique identifier for the time series."
    }, {
        "name": "realtime_start",
        "description": "Start date of the real-time period when the data was available."
    }, {
        "name": "realtime_end",
        "description": "End date of the real-time period when the data was available."
    }, {
        "name": "title",
        "description": "Title of the data series."
    }, {
        "name": "observation_start",
        "description": "Start date of the observations in the data series."
    }, {
        "name": "observation_end",
        "description": "End date of the observations in the data series."
    }, {
        "name": "frequency",
        "description": "Frequency of data recording (e.g., Quarterly, Monthly)."
    }, {
        "name": "frequency_short",
        "description": "Abbreviated form of the data frequency."
    }, {
        "name": "units",
        "description": "Measurement units of the data series."
    }, {
        "name": "units_short",
        "description": "Abbreviated form of the measurement units."
    }, {
        "name": "seasonal_adjustment",
        "description": "Indicates if the data is seasonally adjusted."
    }, {
        "name": "seasonal_adjustment_short",
        "description": "Abbreviated form of the seasonal adjustment status."
    }, {
        "name": "last_updated",
        "description": "The date when the data series was last updated."
    }, {
        "name": "popularity",
        "description": "Popularity score of the data series."
    }, {
        "name": "group_popularity",
        "description": "Popularity score within the group of related series."
    }, {
        "name": "notes",
        "description": "Additional notes or comments about the data series."
    }, {
        "name": "category",
        "description": "The category to which the data series belongs."
    }, {
        "name": "category_id",
        "description": "Identifier of the category to which the data series belongs."
    }]
})
def fred_series_metadata(fred_category_taxonomy: pd.DataFrame) -> pd.DataFrame:
    dfs = []
    for id, name in fred_category_taxonomy[['id', 'name']].values:
        df = get_series_metadata_for_category(os.getenv("FRED_API_KEY"), id)
        df['category'] = name
        df['category_id'] = id
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


@asset(metadata={
    "source": "fred",
    "name": "Federal Reserve Economic Series Data",
    "description": "Observations for all time series from the Federal Reserve Economic Data (FRED) API.",
    "columns": [{
        "name": "id",
        "description": "Identifier of the data series to which the observation belongs."
    }, {
        "name": "date",
        "description": "Date of the data observation."
    }, {
        "name": "value",
        "description": "Observed value for the given date."
    }]
})
def fred_series_data(context, fred_series_metadata: pd.DataFrame, fred_releases: pd.DataFrame) -> pd.DataFrame:
    # It takes long to retrieve all observations, so for now only retrieve data for new series. There may be a better way to achieve this.
    latest_materialization_event = context.instance.get_latest_materialization_event(AssetKey(["tsa_checkpoint_travel_numbers"]))

    if latest_materialization_event:
        last_materialization_date = pd.to_datetime(latest_materialization_event.timestamp, unit='s')
        releases_since_last_materialization = fred_releases[fred_releases['realtime_start'] > last_materialization_date]
        # Retrieve all series for releases that have been published since the last materialization
        release_ids = fred_series_metadata[fred_series_metadata['realtime_start'].isin(releases_since_last_materialization['realtime_start'])]['id'].unique()
        ids = [series_id for release_id in release_ids for series_id in get_series_for_release(release_id, os.getenv("FRED_API_KEY"))]
    else:
        # If there is no previous materialization, we want to retrieve all data
        ids = fred_series_metadata['id'].unique()
    series = [get_series_observations(series_id, os.getenv("FRED_API_KEY")) for series_id in ids]
    return pd.concat(series, ignore_index=True)


@asset(metadata={
    "source": "fred",
    "name": "Federal Reserve Economic Data Releases",
    "description": "A collection of recent economic data releases from the FRED API, including release id, real-time availability dates, name of the release, press release status, and related links.",
    "columns": [{
        "name": "id",
        "description": "Unique identifier for the data release."
    }, {
        "name": "realtime_start",
        "description": "The start date of the real-time period when the data was available."
    }, {
        "name": "realtime_end",
        "description": "The end date of the real-time period when the data was available."
    }, {
        "name": "name",
        "description": "Name of the data release."
    }, {
        "name": "press_release",
        "description": "Boolean indicating if the release is a press release."
    }, {
        "name": "link",
        "description": "Link to more information about the data release, if available."
    }]
})
def fred_releases() -> pd.DataFrame:
    api_key = os.getenv("FRED_API_KEY")
    url = f"https://api.stlouisfed.org/fred/releases?api_key={api_key}&file_type=json"
    releases_data = make_request(url)['releases']
    return pd.DataFrame(releases_data, columns=["id", "realtime_start", "realtime_end", "name", "press_release", "link"])

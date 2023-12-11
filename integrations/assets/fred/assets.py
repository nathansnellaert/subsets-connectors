import requests
import os
from dagster import DagsterEventType, EventRecordsFilter, asset, AssetKey
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
    df = pd.DataFrame(releases_data, columns=["id", "realtime_start", "realtime_end", "name", "press_release", "link"])
    df['realtime_start'] = pd.to_datetime(df['realtime_start'])
    df['realtime_end'] = pd.to_datetime(df['realtime_end'])
    return df

def fred_series_data_for_category(category_id: int, fred_series_metadata: pd.DataFrame, category_taxonomy: pd.DataFrame) -> pd.DataFrame:
    api_key = os.getenv("FRED_API_KEY")

    def get_all_child_category_ids(parent_id, taxonomy):
        """
        Recursively fetches all child category IDs for a given parent category.
        """
        child_categories = taxonomy[taxonomy['parent_id'] == parent_id]
        child_ids = child_categories['id'].tolist()
        for child_id in child_ids:
            child_ids.extend(get_all_child_category_ids(child_id, taxonomy))
        return child_ids

    print(category_taxonomy)
    print(category_id)
    # Include the category itself and all its subcategories
    all_category_ids = [category_id] + get_all_child_category_ids(category_id, category_taxonomy)
    
    print(f"Fetching data for {len(all_category_ids)} categories")
    # Filter metadata for the specific category and its subcategories
    filtered_metadata = fred_series_metadata[fred_series_metadata['category_id'].isin(all_category_ids)]
    
    if filtered_metadata.empty:
        return pd.DataFrame()

    series_ids = filtered_metadata['id'].unique()
    series_data = [get_series_observations(series_id, api_key) for series_id in series_ids]

    return pd.concat(series_data, ignore_index=True)

@asset
def fred_series_money_banking_finance(fred_series_metadata: pd.DataFrame, fred_category_taxonomy: pd.DataFrame) -> pd.DataFrame:
    return fred_series_data_for_category(32991, fred_series_metadata, fred_category_taxonomy)

@asset
def fred_series_population_employment_labor_markets(fred_series_metadata: pd.DataFrame, fred_category_taxonomy: pd.DataFrame) -> pd.DataFrame:
    return fred_series_data_for_category(10, fred_series_metadata, fred_category_taxonomy)

@asset
def fred_series_national_accounts(fred_series_metadata: pd.DataFrame, fred_category_taxonomy: pd.DataFrame) -> pd.DataFrame:
    return fred_series_data_for_category(32992, fred_series_metadata, fred_category_taxonomy)

@asset
def fred_series_production_business_activity(fred_series_metadata: pd.DataFrame, fred_category_taxonomy: pd.DataFrame) -> pd.DataFrame:
    return fred_series_data_for_category(1, fred_series_metadata, fred_category_taxonomy)

@asset
def fred_series_prices(fred_series_metadata: pd.DataFrame, fred_category_taxonomy: pd.DataFrame) -> pd.DataFrame:
    return fred_series_data_for_category(32455, fred_series_metadata, fred_category_taxonomy)

@asset
def fred_series_international_data(fred_series_metadata: pd.DataFrame, fred_category_taxonomy: pd.DataFrame) -> pd.DataFrame:
    return fred_series_data_for_category(32263, fred_series_metadata, fred_category_taxonomy)

@asset
def fred_series_us_regional_data(fred_series_metadata: pd.DataFrame, fred_category_taxonomy: pd.DataFrame) -> pd.DataFrame:
    return fred_series_data_for_category(3008, fred_series_metadata, fred_category_taxonomy)

@asset
def fred_series_academic_data(fred_series_metadata: pd.DataFrame, fred_category_taxonomy: pd.DataFrame) -> pd.DataFrame:
    return fred_series_data_for_category(33060, fred_series_metadata, fred_category_taxonomy)
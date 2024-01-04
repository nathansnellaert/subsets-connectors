import pandas as pd
from dagster import AssetOut, multi_asset, FreshnessPolicy
import requests
import zipfile
import io

@multi_asset(
    outs={
        "world_development_indicators": AssetOut(metadata={
            "source": "worldbank",
            "name": "World Development Indicators",
            "description": "Primary World Bank collection of development indicators, compiled from officially recognized international sources.",
            "columns": [
                {"name": "country_name", "description": "Name of the country"},
                {"name": "country_code2", "description": "Two-letter country code"},
                {"name": "indicator_name", "description": "Name of the indicator"},
                {"name": "indicator_code", "description": "Unique code for the indicator"},
                {"name": "year", "description": "Year of the data"},
                {"name": "value", "description": "Value of the indicator"}
            ]
        }, freshness_policy=FreshnessPolicy(cron_schedule="0 0 * * 1", maximum_lag_minutes=60 * 24 * 7)),
        "world_development_indicators_series_metadata": AssetOut(metadata={
            "source": "worldbank",
            "name": "WDI Series Metadata",
            "description": "Metadata for each indicator series in the World Development Indicators dataset.",
            "columns": [
                {"name": "indicator_code", "description": "Unique code for the indicator"},
                {"name": "indicator_name", "description": "Name of the indicator"},
                {"name": "short_definition", "description": "Short definition of the indicator"},
                {"name": "long_definition", "description": "Detailed definition of the indicator"},
                {"name": "unit_of_measure", "description": "Unit of measurement for the indicator"},
                {"name": "periodicity", "description": "Frequency of data collection"},
                {"name": "base_period", "description": "The base period for the indicator"},
                {"name": "other_notes", "description": "Additional notes about the indicator"},
                {"name": "aggregation_method", "description": "Method used for aggregating data"},
                {"name": "limitations_and_exceptions", "description": "Known limitations and exceptions"},
                {"name": "notes_from_original_source", "description": "Notes from the original data source"},
                {"name": "general_comments", "description": "General comments about the indicator"},
                {"name": "source", "description": "Source of the data"},
                {"name": "statistical_concept_and_methodology", "description": "Explanation of statistical concepts and methodology"},
                {"name": "development_relevance", "description": "Relevance of the indicator to development"},
                {"name": "license_type", "description": "Type of license for the data"}
            ]
        }, freshness_policy=FreshnessPolicy(cron_schedule="0 0 * * 1", maximum_lag_minutes=60 * 24 * 7)),
        "world_development_indicators_country_series_metadata": AssetOut(metadata={
            "source": "worldbank",
            "name": "WDI Country Series Metadata",
            "description": "Metadata for an indicator measured in a given country",
            "columns": [
                {"name": "country_code2", "description": "Three-letter country code"},
                {"name": "indicator_code", "description": "Unique code for the indicator"},
                {"name": "description", "description": "Description of the country-indicator relationship"}
            ]
        }, freshness_policy=FreshnessPolicy(cron_schedule="0 0 * * 1", maximum_lag_minutes=60 * 24 * 7)),
        "world_development_indicators_observation_metadata": AssetOut(metadata={
            "source": "worldbank",
            "name": "WDI Footnote Metadata",
            "description": "Metadata for individual observations.",
            "columns": [
                {"name": "country_code2", "description": "Country code"},
                {"name": "series_code", "description": "Series code"},
                {"name": "year", "description": "Year of the footnote"},
                {"name": "description", "description": "Description of the footnote"}
            ]
        }, freshness_policy=FreshnessPolicy(cron_schedule="0 0 * * 1", maximum_lag_minutes=60 * 24 * 7)),
        "world_development_indicators_series_year_metadata": AssetOut(metadata={
            "source": "worldbank",
            "name": "WDI Series Time Metadata",
            "description": "Metadata for an indicator at a given year.",
            "columns": [
                {"name": "indicator_code", "description": "Unique code for the indicator"},
                {"name": "year", "description": "Year related to the series"},
                {"name": "description", "description": "Description of the time series metadata"}
            ]
        }, freshness_policy=FreshnessPolicy(cron_schedule="0 0 * * 1", maximum_lag_minutes=60 * 24 * 7)),
    }
)
def process_world_development_indicators(countries: pd.DataFrame):
    download_url = "https://databank.worldbank.org/data/download/WDI_CSV.zip"
    response = requests.get(download_url)
    
    response.raise_for_status()
    
    zip_bytes_io = io.BytesIO(response.content)
    
    with zipfile.ZipFile(zip_bytes_io) as zip_ref:
        csv_contents = {}
        
        for file_info in zip_ref.filelist:
            if file_info.filename.endswith('.csv'):
                with zip_ref.open(file_info) as file:
                    csv_contents[file_info.filename] = pd.read_csv(file)
    
    # datasets use 3 letter country codes, while most subsets datasets use 2 letter country codes. map them.
    country_code_mapping = countries[['country_code2', 'country_code3']]

    return (
        process_wdi_series(csv_contents.get('WDIData.csv', pd.DataFrame()), country_code_mapping),
        process_wdi_series_metadata(csv_contents.get('WDISeries.csv', pd.DataFrame())),
        process_wdi_country_series_metadata(csv_contents.get('WDICountry-Series.csv', pd.DataFrame()), country_code_mapping),
        process_wdi_observation_metadata(csv_contents.get('WDIFootNote.csv', pd.DataFrame()), country_code_mapping),
        process_wdi_year_series_metadata(csv_contents.get('WDISeries-Time.csv', pd.DataFrame())),
    )



def process_wdi_series_metadata(df):
    columns = {
        'Series Code': 'indicator_code',
        'Indicator Name': 'indicator_name',
        'Short definition': 'short_definition',
        'Long definition': 'long_definition',
        'Unit of measure': 'unit_of_measure',
        'Periodicity': 'periodicity',
        'Base Period': 'base_period',
        'Other notes': 'other_notes',
        'Aggregation method': 'aggregation_method',
        'Limitations and exceptions': 'limitations_and_exceptions',
        'Notes from original source': 'notes_from_original_source',
        'General comments': 'general_comments',
        'Source': 'source',
        'Statistical concept and methodology': 'statistical_concept_and_methodology',
        'Development relevance': 'development_relevance',
        'License Type': 'license_type',
    }
    return df.rename(columns=columns)[list(columns.values())]



def clean_year_str(item):
    return item.replace('YR', '').replace('yr', '').strip()

def process_wdi_country_series_metadata(df, country_code_mapping):
    df = df.rename(columns={'CountryCode': 'country_code3', 'SeriesCode': 'indicator_code', 'DESCRIPTION': 'description'})
    df = df.merge(country_code_mapping, on='country_code3', how='left').drop(columns=['country_code3'])
    return df[['country_code2', 'indicator_code', 'description']]

def process_wdi_observation_metadata(df, country_code_mapping):
    df = df.rename(columns={'CountryCode': 'country_code3', 'SeriesCode': 'series_code', 'Year': 'year', 'DESCRIPTION': 'description'})
    df['year'] = df['year'].transform(clean_year_str).astype(int)
    df = df.merge(country_code_mapping, on='country_code3', how='left').drop(columns=['country_code3'])
    return df[['country_code2', 'series_code', 'year', 'description']]

def process_wdi_year_series_metadata(df):
    df = df.rename(columns={'SeriesCode': 'indicator_code', 'Year': 'year', 'DESCRIPTION': 'description'})
    df['year'] = df['year'].transform(clean_year_str).astype(int)
    return df[['indicator_code', 'year', 'description']]

def process_wdi_series(df, country_code_mapping):
    df = df.melt(
        id_vars=['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code'],
        var_name='year',
        value_name='value'
    )
    df['year'] = df['year'].transform(clean_year_str).astype(int)
    df = df.rename(columns={'Country Name': 'country_name', 'Country Code': 'country_code3', 'Indicator Name': 'indicator_name', 'Indicator Code': 'indicator_code'})
    df = df.dropna(subset=['value'])
    df = df.merge(country_code_mapping, on='country_code3', how='left').drop(columns=['country_code3'])
    return df[['country_name', 'country_code2', 'indicator_name', 'indicator_code', 'year', 'value']]

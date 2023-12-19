from dagster import asset, FreshnessPolicy
import pandas as pd

@asset(metadata={
    "source": "github",
    "name": "Countries",
    "description": "Information about countries including codes, names, and geographical details.",
    "columns": [{
        "name": "country_code2",
        "description": "Two-letter country code"
    }, {
        "name": "country_code3",
        "description": "Three-letter country code"
    }, {
        "name": "country_name",
        "description": "Common name of the country"
    }, {
        "name": "country_name_formal",
        "description": "Formal name of the country"
    }, {
        "name": "capital",
        "description": "Capital city of the country"
    }, {
        "name": "region_name",
        "description": "Geographic region where the country is located"
    }, {
        "name": "subregion_name",
        "description": "Geographic sub-region where the country is located"
    }, {
        "name": "intermediate_region_name",
        "description": "Intermediate geographic region where the country is located"
    }, {
        "name": "continent",
        "description": "Continent where the country is situated"
    }, {
        "name": "tld",
        "description": "Top-level domain of the country"
    }, {
        "name": "telephone_code",
        "description": "Telephone dialing code of the country"
    }, {
        "name": "fips",
        "description": "FIPS code of the country"
    }, {
        "name": "languages",
        "description": "Languages spoken in the country"
    }, {
        "name": "currency_name",
        "description": "Official currency of the country"
    }, {
        "name": "is_independent",
        "description": "Indicates if the country is independent"
    }]
}, freshness_policy=FreshnessPolicy(cron_schedule="0 0 1 * *", maximum_lag_minutes=60 * 24))
def countries(context):
    df = pd.read_csv('https://raw.githubusercontent.com/datasets/country-codes/master/data/country-codes.csv', keep_default_na=False)
    keep_cols = {
        'ISO3166-1-Alpha-2': 'country_code2',
        'ISO3166-1-Alpha-3': 'country_code3',
        'UNTERM English Short': 'country_name',
        'UNTERM English Formal': 'country_name_formal',
        'Capital': 'capital',
        'Region Name': 'region_name',
        'Sub-region Name': 'subregion_name',
        'Intermediate Region Name': 'intermediate_region_name',
        'Continent': 'continent',
        'TLD': 'tld',
        'Dial': 'telephone_code',
        'FIPS': 'fips',
        'Languages': 'languages',
        'ISO4217-currency_name': 'currency_name',
        'is_independent': 'is_independent',
    }
    df = df[list(keep_cols.keys())]
    df = df.rename(columns=keep_cols)
    context.log.info(f"Loaded {len(df)} countries")
    return df

@asset(metadata={
    "source": "github",
    "name": "Regions",
    "description": "Region names and mapping between region and subregion.",
    "columns": [{
        "name": "region_name",
        "description": "Name of the geographic region"
    }, {
        "name": "subregion_name",
        "description": "Name of the geographic sub-region"
    }, {
        "name": "intermediate_region_name",
        "description": "Name of the intermediate geographic region"
    }]
}, freshness_policy=FreshnessPolicy(cron_schedule="0 0 1 * *", maximum_lag_minutes=60 * 24))
def regions(countries):
    return countries[['region_name', 'subregion_name', 'intermediate_region_name']].drop_duplicates()

@asset(metadata={
    "source": "opendatasoft",
    "name": "Cities Data",
    "description": "Information about cities with more than 1000 inhabitants.",
    "columns": [{
        "name": "name",
        "description": "Name of the city"
    }, {
        "name": "ascii_name",
        "description": "ASCII-compatible name of the city"
    }, {
        "name": "country_code",
        "description": "Two-letter country code of the city's country"
    }, {
        "name": "population",
        "description": "Population of the city"
    }, {
        "name": "elevation",
        "description": "Elevation of the city in meters"
    }, {
        "name": "timezone",
        "description": "Timezone of the city"
    }, {
        "name": "latitude",
        "description": "Latitude coordinate of the city"
    }, {
        "name": "longitude",
        "description": "Longitude coordinate of the city"
    }]
}, freshness_policy=FreshnessPolicy(cron_schedule="0 0 1 * *", maximum_lag_minutes=60 * 24))
def cities():
    url = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/geonames-all-cities-with-a-population-1000/exports/csv"
    cities = pd.read_csv(url, sep=";")
    cities = cities[['name', 'ascii_name', 'country_code', 'population', 'elevation', 'timezone', 'coordinates']]
    cities['latitude'] = cities['coordinates'].str.split(',').str[0].astype(float)
    cities['longitude'] = cities['coordinates'].str.split(',').str[1].astype(float)
    cities = cities.drop(columns=['coordinates'])
    return cities

@asset(metadata={
    "source": "github",
    "name": "Country Border Mapping",
    "description": "Mapping of countries to their bordering countries.",
    "columns": [{
        "name": "country",
        "description": "Country code"
    }, {
        "name": "border_country",
        "description": "Bordering country code"
    }]
}, freshness_policy=FreshnessPolicy(cron_schedule="0 0 1 * *", maximum_lag_minutes=60 * 24))
def country_border_mapping():
    url = "https://raw.githubusercontent.com/geodatasource/country-borders/master/GEODATASOURCE-COUNTRY-BORDERS.CSV"
    df = pd.read_csv(url, keep_default_na=False)
    df = df[['country_code', 'country_border_code']].rename(columns={'country_code': 'country', 'country_border_code': 'border_country'})
    df = df[df['border_country'] != '']
    return df

@asset(metadata={
    "source": "github",
    "name": "International Organizations",
    "description": "Mapping between countries and international organizations, such as the UN, NATO, etc.",
    "columns": [{
        "name": "country_code2",
        "description": "Two-letter country code"
    }, {
        "name": "org_code",
        "description": "Organization code"
    }, {
        "name": "org_name",
        "description": "Name of the organization"
    }]
}, freshness_policy=FreshnessPolicy(cron_schedule="0 0 1 * *", maximum_lag_minutes=60 * 24))
def organisation():
    orgs_url = "https://raw.githubusercontent.com/dieghernan/Country-Codes-and-International-Organizations/master/outputs/CountrycodesOrgs.csv"
    orgs = pd.read_csv(orgs_url)
    orgs = orgs.rename(columns={'ISO_3166_2': 'country_code2', 'org_id': 'org_code'})
    orgs = orgs[['country_code2', 'org_code', 'org_name']]
    return orgs

@asset(metadata={
    "source": "opendatasoft",
    "name": "US States",
    "description": "Information about US states including name, code, and location.",
    "columns": [{
        "name": "name",
        "description": "Name of the US state"
    }, {
        "name": "code",
        "description": "Two-letter code of the US state"
    }, {
        "name": "type",
        "description": "Type of the US state (state, commonwealth, etc.)"
    }, {
        "name": "gnis_code",
        "description": "GNIS code of the US state"
    }, {
        "name": "lat",
        "description": "Latitude coordinate of the US state"
    }, {
        "name": "lon",
        "description": "Longitude coordinate of the US state"
    }]
}, io_manager_key="vanilla_parquet_io_manager")
def us_states():
    url = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/georef-united-states-of-america-state/exports/csv?"
    states = pd.read_csv(url, sep=';')
    keep_cols = {
        'ste_name': 'name',
        'ste_stusps_code': 'code',
        'ste_type': 'type',
        'ste_gnis_code': 'gnis_code',
        'geo_point_2d': 'coordinates',
    }
    states = states[list(keep_cols.keys())]
    states = states.rename(columns=keep_cols)
    states['gnis_code'] = states['gnis_code'].astype(str)
    states['lat'] = states['coordinates'].apply(lambda x: x.split(',')[0])
    states['lon'] = states['coordinates'].apply(lambda x: x.split(',')[1])
    states = states.drop(columns=['coordinates'])
    return states

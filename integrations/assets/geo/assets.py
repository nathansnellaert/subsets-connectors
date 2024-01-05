from dagster import asset, FreshnessPolicy
import pandas as pd
from .country_code_2_to_simple_name import mapping



@asset(metadata = {
    "source": "github",
    "name": "Countries",
    "description": "Information about countries including codes, names, and geographical details.",
    "columns": [
        {
            "name": "fifa_code",
            "description": "FIFA country code"
        },
        {
            "name": "dial_code",
            "description": "Country dialing code"
        },
        {
            "name": "country_code3",
            "description": "ISO 3166-1 alpha-3 country code"
        },
        {
            "name": "marc_code",
            "description": "MARC country code"
        },
        {
            "name": "is_independent",
            "description": "Indicates if the country is independent"
        },
        {
            "name": "country_code2",
            "description": "ISO 3166-1 numeric country code"
        },
        {
            "name": "country_name",
            "description": "Simple country name"
        },
        {
            "name": "gaul_code",
            "description": "Global Administrative Unit Layers (GAUL) code"
        },
        {
            "name": "fips_code",
            "description": "FIPS (Federal Information Processing Standards) code"
        },
        {
            "name": "wmo_code",
            "description": "World Meteorological Organization (WMO) code"
        },
        {
            "name": "iso3166_1_alpha_2_code",
            "description": "ISO 3166-1 alpha-2 country code"
        },
        {
            "name": "itu_code",
            "description": "International Telecommunication Union (ITU) code"
        },
        {
            "name": "ioc_code",
            "description": "International Olympic Committee (IOC) code"
        },
        {
            "name": "ds_code",
            "description": "Distinguishing sign of vehicles in international traffic"
        },
        {
            "name": "unterm_spanish_formal",
            "description": "United Nations Terminology Database (UNTERM) formal name in Spanish"
        },
        {
            "name": "global_code",
            "description": "Global code"
        },
        {
            "name": "intermediate_region_code",
            "description": "Code for intermediate geographic region"
        },
        {
            "name": "official_name_fr",
            "description": "Official name of the country in French"
        },
        {
            "name": "unterm_french_short",
            "description": "UNTERM short name in French"
        },
        {
            "name": "iso4217_currency_name",
            "description": "ISO 4217 currency name"
        },
        {
            "name": "developed_developing_countries",
            "description": "Classification as a developed or developing country"
        },
         {
            "name": "unterm_russian_formal",
            "description": "UNTERM formal name in Russian"
        },
        {
            "name": "unterm_english_short",
            "description": "UNTERM short name in English"
        },
        {
            "name": "iso4217_currency_alphabetic_code",
            "description": "ISO 4217 currency alphabetic code"
        },
        {
            "name": "small_island_developing_states_sids",
            "description": "Indicates if the country is a Small Island Developing State"
        },
        {
            "name": "unterm_spanish_short",
            "description": "UNTERM short name in Spanish"
        },
        {
            "name": "iso4217_currency_numeric_code",
            "description": "ISO 4217 currency numeric code"
        },
        {
            "name": "unterm_chinese_formal",
            "description": "UNTERM formal name in Chinese"
        },
        {
            "name": "unterm_french_formal",
            "description": "UNTERM formal name in French"
        },
        {
            "name": "unterm_russian_short",
            "description": "UNTERM short name in Russian"
        },
        {
            "name": "m49_code",
            "description": "United Nations M49 standard code for area and country"
        },
        {
            "name": "sub_region_code",
            "description": "Code for the geographic sub-region"
        },
        {
            "name": "region_code",
            "description": "Code for the geographic region"
        },
        {
            "name": "official_name_ar",
            "description": "Official name of the country in Arabic"
        },
        {
            "name": "iso4217_currency_minor_unit",
            "description": "ISO 4217 minor unit of currency"
        },
        {
            "name": "unterm_arabic_formal",
            "description": "UNTERM formal name in Arabic"
        },
        {
            "name": "unterm_chinese_short",
            "description": "UNTERM short name in Chinese"
        },
        {
            "name": "land_locked_developing_countries_lldc",
            "description": "Indicates if the country is a Land Locked Developing Country"
        },
        {
            "name": "intermediate_region_name",
            "description": "Name of the intermediate geographic region"
        },
        {
            "name": "official_name_es",
            "description": "Official name of the country in Spanish"
        },
        {
            "name": "unterm_english_formal",
            "description": "UNTERM formal name in English"
        },
        {
            "name": "official_name_cn",
            "description": "Official name of the country in Chinese"
        },
        {
            "name": "official_name_en",
            "description": "Official name of the country in English"
        },
        {
            "name": "iso4217_currency_country_name",
            "description": "Country name associated with the ISO 4217 currency code"
        },
        {
            "name": "least_developed_countries_ldc",
            "description": "Indicates if the country is classified as a Least Developed Country (LDC)"
        },
        {
            "name": "region_name",
            "description": "Name of the geographic region"
        },
        {
            "name": "unterm_arabic_short",
            "description": "UNTERM short name in Arabic"
        },
        {
            "name": "sub_region_name",
            "description": "Name of the geographic sub-region"
        },
        {
            "name": "official_name_ru",
            "description": "Official name of the country in Russian"
        },
        {
            "name": "global_name",
            "description": "Global name of the country"
        },
        {
            "name": "capital",
            "description": "Capital city of the country"
        },
        {
            "name": "continent",
            "description": "Continent where the country is located"
        },
        {
            "name": "tld",
            "description": "Top-level domain of the country"
        },
        {
            "name": "languages",
            "description": "Languages spoken in the country"
        },
        {
            "name": "geoname_id",
            "description": "GeoNames ID of the country"
        },
        {
            "name": "cldr_display_name",
            "description": "Country name as displayed in CLDR (Common Locale Data Repository)"
        },
        {
            "name": "edgar_code",
            "description": "EDGAR (Electronic Data Gathering, Analysis, and Retrieval) code"
        }
    ]
}, freshness_policy=FreshnessPolicy(cron_schedule="0 0 1 * *", maximum_lag_minutes=60 * 24))
def countries(context):
    df = pd.read_csv('https://raw.githubusercontent.com/datasets/country-codes/master/data/country-codes.csv', keep_default_na=False)
    column_name_mapping = {
        "ISO3166-1-Alpha-2": "country_code2",
        "ISO3166-1-Alpha-3": "country_code3",
        "UNTERM English Formal": "unterm_english_formal",
        "FIFA": "fifa_code",
        "Dial": "dial_code",
        "MARC": "marc_code",
        "is_independent": "is_independent",
        "ISO3166-1-numeric": "iso3166_1_numeric_code",
        "GAUL": "gaul_code",
        "FIPS": "fips_code",
        "WMO": "wmo_code",
        "ITU": "itu_code",
        "IOC": "ioc_code",
        "DS": "ds_code",
        "UNTERM Spanish Formal": "unterm_spanish_formal",
        "Global Code": "global_code",
        "Intermediate Region Code": "intermediate_region_code",
        "official_name_fr": "official_name_fr",
        "UNTERM French Short": "unterm_french_short",
        "ISO4217-currency_name": "iso4217_currency_name",
        "Developed / Developing Countries": "developed_developing_countries",
        "UNTERM Russian Formal": "unterm_russian_formal",
        "UNTERM English Short": "unterm_english_short",
        "ISO4217-currency_alphabetic_code": "iso4217_currency_alphabetic_code",
        "Small Island Developing States (SIDS)": "small_island_developing_states_sids",
        "UNTERM Spanish Short": "unterm_spanish_short",
        "ISO4217-currency_numeric_code": "iso4217_currency_numeric_code",
        "UNTERM Chinese Formal": "unterm_chinese_formal",
        "UNTERM French Formal": "unterm_french_formal",
        "UNTERM Russian Short": "unterm_russian_short",
        "M49": "m49_code",
        "Sub-region Code": "sub_region_code",
        "Region Code": "region_code",
        "official_name_ar": "official_name_ar",
        "ISO4217-currency_minor_unit": "iso4217_currency_minor_unit",
        "UNTERM Arabic Formal": "unterm_arabic_formal",
        "UNTERM Chinese Short": "unterm_chinese_short",
        "Land Locked Developing Countries (LLDC)": "land_locked_developing_countries_lldc",
        "Intermediate Region Name": "intermediate_region_name",
        "official_name_es": "official_name_es",
        "UNTERM English Formal": "unterm_english_formal",
        "official_name_cn": "official_name_cn",
        "official_name_en": "official_name_en",
        "ISO4217-currency_country_name": "iso4217_currency_country_name",
        "Least Developed Countries (LDC)": "least_developed_countries_ldc",
        "Region Name": "region_name",
        "UNTERM Arabic Short": "unterm_arabic_short",
        "Sub-region Name": "sub_region_name",
        "official_name_ru": "official_name_ru",
        "Global Name": "global_name",
        "Capital": "capital",
        "Continent": "continent",
        "TLD": "tld",
        "Languages": "languages",
        "Geoname ID": "geoname_id",
        "CLDR display name": "cldr_display_name",
        "EDGAR": "edgar_code"
    }
    df = df.rename(columns=column_name_mapping)
    df['country_name'] = df['country_code2'].apply(lambda x: mapping.get(x, x))
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
        "name": "sub_region_name",
        "description": "Name of the geographic sub-region"
    }, {
        "name": "intermediate_region_name",
        "description": "Name of the intermediate geographic region"
    }]
}, freshness_policy=FreshnessPolicy(cron_schedule="0 0 1 * *", maximum_lag_minutes=60 * 24))
def regions(countries):
    return countries[['region_name', 'sub_region_name', 'intermediate_region_name']].drop_duplicates()

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
})
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

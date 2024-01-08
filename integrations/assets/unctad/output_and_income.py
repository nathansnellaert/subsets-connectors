from dagster import asset
from .utils import download_dataset
from .source import unctad

@asset(metadata={
    "source": unctad,
    "name": "Gross domestic product: Total and per capita, current and constant (2015) prices, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USGDPTotal.",
}, io_manager_key="vanilla_parquet_io_manager")
def gross_domestic_product_total_and_per_capita_current_and_constant_2015_prices_annual():
    return download_dataset('US_GDPTotal')
    

@asset(metadata={
    "source": unctad,
    "name": "Gross domestic product: Total and per capita, growth rates, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USGDPGR.",
}, io_manager_key="vanilla_parquet_io_manager")
def gross_domestic_product_total_and_per_capita_growth_rates_annual():
    return download_dataset('US_GDPGR')
    

@asset(metadata={
    "source": unctad,
    "name": "Gross domestic product: GDP by type of expenditure, VA by kind of economic activity, total and shares, annual (~5MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USGDPComponent.",
}, io_manager_key="vanilla_parquet_io_manager")
def gross_domestic_product_gdp_by_type_of_expenditure_va_by_kind_of_economic_activity_total_and_shares_annual():
    return download_dataset('US_GDPComponent')
    

@asset(metadata={
    "source": unctad,
    "name": "Nominal GNI, total and per capita, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USGNI.",
}, io_manager_key="vanilla_parquet_io_manager")
def nominal_gni_total_and_per_capita_annual():
    return download_dataset('US_GNI')
    
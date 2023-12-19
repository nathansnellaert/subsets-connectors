from dagster import asset
from .utils import download_dataset

@asset(metadata={
    "source": "unctad",
    "name": "Currency exchange rates, annual (~5MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USExchangeRateCrosstab.",
}, io_manager_key="vanilla_parquet_io_manager")
def currency_exchange_rates_annual():
    return download_dataset('US_ExchangeRateCrosstab')
    

@asset(metadata={
    "source": "unctad",
    "name": "Consumer price indices, annual (~100MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USCpiA.",
}, io_manager_key="vanilla_parquet_io_manager")
def consumer_price_indices_annual():
    return download_dataset('US_Cpi_A')
    
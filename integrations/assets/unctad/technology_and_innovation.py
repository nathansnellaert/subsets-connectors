
from dagster import asset
from .utils import download_dataset
from .source import unctad

@asset(metadata={
    "source": unctad,
    "name": "Frontier technology readiness index, annual (~50MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USFTRI.",
}, io_manager_key="vanilla_parquet_io_manager")
def frontier_technology_readiness_index_annual():
    return download_dataset('US_FTRI')
    
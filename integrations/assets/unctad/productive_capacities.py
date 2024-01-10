
from dagster import asset
from .utils import download_dataset
from .source import unctad


@asset(metadata={
    "source": unctad,
    "name": "Productive capacities index, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/US.PCI.",
}, io_manager_key="vanilla_parquet_io_manager")
def productive_capacities_index_annual():
    return download_dataset('US_PCI')
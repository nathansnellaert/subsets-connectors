
from dagster import asset
from .utils import download_dataset


@asset(metadata={
    "source": "unctad",
    "name": "Productive capacities index, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USPCI.",
})
def productive_capacities_index_annual():
    return download_dataset('US_PCI')
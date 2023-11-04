from dagster import asset
from .utils import download_dataset

@asset(metadata={
    "source": "unctad",
    "name": "Government expenditures (~500KB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USGovExpenditures.",
})
def government_expenditures():
    return download_dataset('US.GovExpenditures')

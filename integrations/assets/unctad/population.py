from dagster import asset
from .utils import download_dataset
from .source import unctad

@asset(metadata={
    "source": unctad,
    "name": "Total and urban population, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/US.PopTotal.",
}, io_manager_key="vanilla_parquet_io_manager")
def total_and_urban_population_annual():
    return download_dataset('US_PopTotal')
    

@asset(metadata={
    "source": unctad,
    "name": "Total population growth rates, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/US.PopGR.",
}, io_manager_key="vanilla_parquet_io_manager")
def total_population_growth_rates_annual():
    return download_dataset('US_PopGR')
    

@asset(metadata={
    "source": unctad,
    "name": "Population structure by gender and age-group, annual (~20MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/US.PopAgeStruct.",
}, io_manager_key="vanilla_parquet_io_manager")
def population_structure_by_gender_and_age_group_annual():
    return download_dataset('US_PopAgeStruct')
    

@asset(metadata={
    "source": unctad,
    "name": "Total, child and old-age dependency ratios, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/US.PopDependency.",
}, io_manager_key="vanilla_parquet_io_manager")
def total_child_and_old_age_dependency_ratios_annual():
    return download_dataset('US_PopDependency')
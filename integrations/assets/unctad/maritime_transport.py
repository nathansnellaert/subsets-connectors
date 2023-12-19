
from dagster import asset
from .utils import download_dataset

@asset(metadata={
    "source": "unctad",
    "name": "Merchant fleet by flag of registration and by type of ship, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USMerchantFleet.",
}, io_manager_key="vanilla_parquet_io_manager")
def merchant_fleet_by_flag_of_registration_and_by_type_of_ship_annual():
    return download_dataset('US_MerchantFleet')
    

@asset(metadata={
    "source": "unctad",
    "name": "Merchant fleet by country of beneficial ownership, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USFleetBeneficialOwners.",
}, io_manager_key="vanilla_parquet_io_manager")
def merchant_fleet_by_country_of_beneficial_ownership_annual():
    return download_dataset('US_FleetBeneficialOwners')
    

@asset(metadata={
    "source": "unctad",
    "name": "Ship scrapping by country of demolition, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USShipScrapping.",
}, io_manager_key="vanilla_parquet_io_manager")
def ship_scrapping_by_country_of_demolition_annual():
    return download_dataset('US_ShipScrapping')
    

@asset(metadata={
    "source": "unctad",
    "name": "Ships built by country of building, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USShipBuilding.",
}, io_manager_key="vanilla_parquet_io_manager")
def ships_built_by_country_of_building_annual():
    return download_dataset('US_ShipBuilding')
    

@asset(metadata={
    "source": "unctad",
    "name": "Liner shipping connectivity index, quarterly (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USLSCI.",
}, io_manager_key="vanilla_parquet_io_manager")
def liner_shipping_connectivity_index_quarterly():
    return download_dataset('US_LSCI')
    

@asset(metadata={
    "source": "unctad",
    "name": "Liner shipping bilateral connectivity index, quarterly (~5MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USLSBCI.",
}, io_manager_key="vanilla_parquet_io_manager")
def liner_shipping_bilateral_connectivity_index_quarterly():
    return download_dataset('US_LSBCI')
    

@asset(metadata={
    "source": "unctad",
    "name": "Container port throughput, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USContPortThroughput.",
}, io_manager_key="vanilla_parquet_io_manager")
def container_port_throughput_annual():
    return download_dataset('US_ContPortThroughput')
    

@asset(metadata={
    "source": "unctad",
    "name": "Port liner shipping connectivity index, quarterly (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USPLSCI.",
}, io_manager_key="vanilla_parquet_io_manager")
def port_liner_shipping_connectivity_index_quarterly():
    return download_dataset('US_PLSCI')
    

@asset(metadata={
    "source": "unctad",
    "name": "Port call and performance statistics: time spent in ports, vessel age and size, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USPortCalls.",
}, io_manager_key="vanilla_parquet_io_manager")
def port_call_and_performance_statistics_time_spent_in_ports_vessel_age_and_size_annual():
    return download_dataset('US_PortCalls')
    

@asset(metadata={
    "source": "unctad",
    "name": "Port call and performance statistics: time spent in ports, vessel age and size, semi-annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USPortCallsS.",
}, io_manager_key="vanilla_parquet_io_manager")
def port_call_and_performance_statistics_time_spent_in_ports_vessel_age_and_size_semi_annual():
    return download_dataset('US_PortCalls_S')
    

@asset(metadata={
    "source": "unctad",
    "name": "Port call and performance statistics: number of port calls, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USPortCallsArrivals.",
}, io_manager_key="vanilla_parquet_io_manager")
def port_call_and_performance_statistics_number_of_port_calls_annual():
    return download_dataset('US_PortCallsArrivals')
    

@asset(metadata={
    "source": "unctad",
    "name": "Port call and performance statistics: number of port calls, semi-annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USPortCallsArrivalsS.",
}, io_manager_key="vanilla_parquet_io_manager")
def port_call_and_performance_statistics_number_of_port_calls_semi_annual():
    return download_dataset('US_PortCallsArrivals_S')
    

@asset(metadata={
    "source": "unctad",
    "name": "World seaborne trade by types of cargo and by group of economies, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USSeaborneTrade.",
}, io_manager_key="vanilla_parquet_io_manager")
def world_seaborne_trade_by_types_of_cargo_and_by_group_of_economies_annual():
    return download_dataset('US_SeaborneTrade')
    

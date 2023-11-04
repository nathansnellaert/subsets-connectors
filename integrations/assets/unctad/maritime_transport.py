
from dagster import asset
from .utils import download_dataset

@asset(metadata={
    "source": "unctad",
    "name": "Merchant fleet by flag of registration and by type of ship, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USMerchantFleet.",
})
def merchant_fleet_by_flag_of_registration_and_by_type_of_ship_annual():
    return download_dataset('US_MerchantFleet')
    

@asset(metadata={
    "source": "unctad",
    "name": "Merchant fleet by country of beneficial ownership, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USFleetBeneficialOwners.",
})
def merchant_fleet_by_country_of_beneficial_ownership_annual():
    return download_dataset('US_FleetBeneficialOwners')
    

@asset(metadata={
    "source": "unctad",
    "name": "Ship scrapping by country of demolition, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USShipScrapping.",
})
def ship_scrapping_by_country_of_demolition_annual():
    return download_dataset('US_ShipScrapping')
    

@asset(metadata={
    "source": "unctad",
    "name": "Ships built by country of building, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USShipBuilding.",
})
def ships_built_by_country_of_building_annual():
    return download_dataset('US_ShipBuilding')
    

@asset(metadata={
    "source": "unctad",
    "name": "Liner shipping connectivity index, quarterly (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USLSCI.",
})
def liner_shipping_connectivity_index_quarterly():
    return download_dataset('US_LSCI')
    

@asset(metadata={
    "source": "unctad",
    "name": "Liner shipping bilateral connectivity index, quarterly (~5MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USLSBCI.",
})
def liner_shipping_bilateral_connectivity_index_quarterly():
    return download_dataset('US_LSBCI')
    

@asset(metadata={
    "source": "unctad",
    "name": "Container port throughput, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USContPortThroughput.",
})
def container_port_throughput_annual():
    return download_dataset('US_ContPortThroughput')
    

@asset(metadata={
    "source": "unctad",
    "name": "Port liner shipping connectivity index, quarterly (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USPLSCI.",
})
def port_liner_shipping_connectivity_index_quarterly():
    return download_dataset('US_PLSCI')
    

@asset(metadata={
    "source": "unctad",
    "name": "Port call and performance statistics: time spent in ports, vessel age and size, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USPortCalls.",
})
def port_call_and_performance_statistics_time_spent_in_ports_vessel_age_and_size_annual():
    return download_dataset('US_PortCalls')
    

@asset(metadata={
    "source": "unctad",
    "name": "Port call and performance statistics: time spent in ports, vessel age and size, semi-annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USPortCallsS.",
})
def port_call_and_performance_statistics_time_spent_in_ports_vessel_age_and_size_semi_annual():
    return download_dataset('US_PortCalls_S')
    

@asset(metadata={
    "source": "unctad",
    "name": "Port call and performance statistics: number of port calls, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USPortCallsArrivals.",
})
def port_call_and_performance_statistics_number_of_port_calls_annual():
    return download_dataset('US_PortCallsArrivals')
    

@asset(metadata={
    "source": "unctad",
    "name": "Port call and performance statistics: number of port calls, semi-annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USPortCallsArrivalsS.",
})
def port_call_and_performance_statistics_number_of_port_calls_semi_annual():
    return download_dataset('US_PortCallsArrivals_S')
    

@asset(metadata={
    "source": "unctad",
    "name": "World seaborne trade by types of cargo and by group of economies, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USSeaborneTrade.",
})
def world_seaborne_trade_by_types_of_cargo_and_by_group_of_economies_annual():
    return download_dataset('US_SeaborneTrade')
    

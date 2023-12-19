

from dagster import asset
from .utils import download_dataset

@asset(metadata={
    "source": "unctad",
    "name": "Trade by air – output variables – transport costs, transport costs per unit, transport costs per unit and km (~100MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USTransportCostsAirMain.",
}, io_manager_key="vanilla_parquet_io_manager")
def trade_by_air_output_variables_transport_costs_transport_costs_per_unit_transport_costs_per_unit_and_km():
    return download_dataset('US_TransportCosts_Air_Main')
    

@asset(metadata={
    "source": "unctad",
    "name": "Trade by air – input variables – CIF, FOB, quantity, and distance (~500MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USTransportCostsAirDetail.",
}, io_manager_key="vanilla_parquet_io_manager")
def trade_by_air_input_variables_cif_fob_quantity_and_distance():
    return download_dataset('US_TransportCosts_Air_Detail')
    

@asset(metadata={
    "source": "unctad",
    "name": "Trade by sea – output variables – transport costs, transport costs per unit, transport costs per unit and km (~100MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USTransportCostsSeaMain.",
}, io_manager_key="vanilla_parquet_io_manager")
def trade_by_sea_output_variables_transport_costs_transport_costs_per_unit_transport_costs_per_unit_and_km():
    return download_dataset('US_TransportCosts_Sea_Main')
    

@asset(metadata={
    "source": "unctad",
    "name": "Trade by sea – input variables – CIF, FOB, quantity, and distance (~500MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USTransportCostsSeaDetail.",
}, io_manager_key="vanilla_parquet_io_manager")
def trade_by_sea_input_variables_cif_fob_quantity_and_distance():
    return download_dataset('US_TransportCosts_Sea_Detail')
    

@asset(metadata={
    "source": "unctad",
    "name": "Trade by railway – output variables – transport costs, transport costs per unit, transport costs per unit and km (~50MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USTransportCostsRailwayMain.",
}, io_manager_key="vanilla_parquet_io_manager")
def trade_by_railway_output_variables_transport_costs_transport_costs_per_unit_transport_costs_per_unit_and_km():
    return download_dataset('US_TransportCosts_Railway_Main')
    

@asset(metadata={
    "source": "unctad",
    "name": "Trade by railway – input variables – CIF, FOB, quantity, and distance (~250MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USTransportCostsRailwayDetail.",
}, io_manager_key="vanilla_parquet_io_manager")
def trade_by_railway_input_variables_cif_fob_quantity_and_distance():
    return download_dataset('US_TransportCosts_Railway_Detail')
    

@asset(metadata={
    "source": "unctad",
    "name": "Trade by road – output variables – transport costs, transport costs per unit, transport costs per unit and km (~100MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USTransportCostsRoadMain.",
}, io_manager_key="vanilla_parquet_io_manager")
def trade_by_road_output_variables_transport_costs_transport_costs_per_unit_transport_costs_per_unit_and_km():
    return download_dataset('US_TransportCosts_Road_Main')
    

@asset(metadata={
    "source": "unctad",
    "name": "Trade by road – input variables – CIF, FOB, quantity, and distance (~500MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USTransportCostsRoadDetail.",
}, io_manager_key="vanilla_parquet_io_manager")
def trade_by_road_input_variables_cif_fob_quantity_and_distance():
    return download_dataset('US_TransportCosts_Road_Detail')
    

@asset(metadata={
    "source": "unctad",
    "name": "Trade by all modes of transport – output variables – transport costs, transport costs per unit, transport costs per unit and km (~250MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USTransportCostsAllMain.",
}, io_manager_key="vanilla_parquet_io_manager")
def trade_by_all_modes_of_transport_output_variables_transport_costs_transport_costs_per_unit_transport_costs_per_unit_and_km():
    return download_dataset('US_TransportCosts_All_Main')
    

@asset(metadata={
    "source": "unctad",
    "name": "Trade by all modes of transport – input variables – CIF, FOB, and quantity (~250MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USTransportCostsAllDetail.",
}, io_manager_key="vanilla_parquet_io_manager")
def trade_by_all_modes_of_transport_input_variables_cif_fob_and_quantity():
    return download_dataset('US_TransportCosts_All_Detail')
    

@asset(metadata={
    "source": "unctad",
    "name": "Trade by non-standard modes of transport – output variables – transport costs, transport costs per unit, transport costs per unit and km (~50MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USTransportCostsNonStandardMain.",
}, io_manager_key="vanilla_parquet_io_manager")
def trade_by_non_standard_modes_of_transport_output_variables_transport_costs_transport_costs_per_unit_transport_costs():
    return download_dataset('US_TransportCosts_NonStandard_Main')
    

@asset(metadata={
    "source": "unctad",
    "name": "Trade by non-standard modes of transport – input variables – CIF, FOB and quantity Table summary (~50MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USTransportCostsNonStandardDetail.",
}, io_manager_key="vanilla_parquet_io_manager")
def trade_by_non_standard_modes_of_transport_input_variables_cif_fob_and_quantity_table_summary():
    return download_dataset('US_TransportCosts_NonStandard_Detail')
    

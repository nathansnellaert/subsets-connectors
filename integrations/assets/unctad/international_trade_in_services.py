
from dagster import asset
from .utils import download_dataset
from .source import unctad


@asset(metadata={
    "source": unctad,
    "name": "Services (BPM6): Trade and growth by main service-category, quarterly (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/US.TotAndComServicesQuarterly.",
}, io_manager_key="vanilla_parquet_io_manager")
def services_bpm6_trade_and_growth_by_main_service_category_quarterly():
    return download_dataset('US_TotAndComServicesQuarterly')
    

@asset(metadata={
    "source": unctad,
    "name": "Services (BPM6): Exports and imports by service category, trading partner world, annual (~10MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/US.TradeServCatTotal.",
}, io_manager_key="vanilla_parquet_io_manager")
def services_bpm6_exports_and_imports_by_service_category_trading_partner_world_annual():
    return download_dataset('US_TradeServCatTotal')
    

@asset(metadata={
    "source": unctad,
    "name": "Services (BPM6): Exports and imports by service category and trading partner, annual (~50MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/US.TradeServCatByPartner.",
}, io_manager_key="vanilla_parquet_io_manager")
def services_bpm6_exports_and_imports_by_service_category_and_trading_partner_annual():
    return download_dataset('US_TradeServCatByPartner')

from dagster import asset
from .utils import download_dataset
from .source import unctad

@asset(metadata={
    "source": unctad,
    "name": "Bilateral trade flows by ICT goods categories, annual (~150MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USIctGoodsValue.",
}, io_manager_key="vanilla_parquet_io_manager")
def bilateral_trade_flows_by_ict_goods_categories_annual():
    return download_dataset('US_IctGoodsValue')
    

@asset(metadata={
    "source": unctad,
    "name": "Share of ICT goods as percentage of total trade, annual (~500KB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USIctGoodsShare.",
}, io_manager_key="vanilla_parquet_io_manager")
def share_of_ict_goods_as_percentage_of_total_trade_annual():
    return download_dataset('US_IctGoodsShare')
    

@asset(metadata={
    "source": unctad,
    "name": "ICT producing sector core indicators, annual (~500KB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USIctProductionSector.",
}, io_manager_key="vanilla_parquet_io_manager")
def ict_producing_sector_core_indicators_annual():
    return download_dataset('US_IctProductionSector')
    

@asset(metadata={
    "source": unctad,
    "name": "Core indicators on ICT use in business by location type, annual (~500KB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USIctUseLocation.",
}, io_manager_key="vanilla_parquet_io_manager")
def core_indicators_on_ict_use_in_business_by_location_type_annual():
    return download_dataset('US_IctUseLocation')
    

@asset(metadata={
    "source": unctad,
    "name": "Core indicators on ICT use in business by enterprise size class, annual (~500KB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USIctUseEnterprSize.",
}, io_manager_key="vanilla_parquet_io_manager")
def core_indicators_on_ict_use_in_business_by_enterprise_size_class_annual():
    return download_dataset('US_IctUseEnterprSize')
    

@asset(metadata={
    "source": unctad,
    "name": "Core indicators on ICT use in business by industrial classification of economic activity (ISIC Rev. 3.1), annual (~500KB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USIctUseEconActivity.",
}, io_manager_key="vanilla_parquet_io_manager")
def core_indicators_on_ict_use_in_business_by_industrial_classification_of_economic_activity():
    return download_dataset('US_IctUseEconActivity')
    

@asset(metadata={
    "source": unctad,
    "name": "Core indicators on ICT use in business by industrial classification of economic activity (ISIC Rev. 4), annual (~500KB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USIctUseEconActivityIsic4.",
}, io_manager_key="vanilla_parquet_io_manager")
def core_indicators_on_ict_use_in_business_by_industrial_classification_of_economic_activity_isic_rev_4_annual_500kb_():
    return download_dataset('US_IctUseEconActivity_Isic4')
    

@asset(metadata={
    "source": unctad,
    "name": "International trade in digitally-deliverable services, value, shares and growth, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USDigitallyDeliverableService.",
}, io_manager_key="vanilla_parquet_io_manager")
def international_trade_in_digitally_deliverable_services_value_shares_and_growth_annual_1mb_():
    return download_dataset('US_DigitallyDeliverableServices')
    

@asset(metadata={
    "source": unctad,
    "name": "International trade in ICT services, value, shares and growth, annual (~500KB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USTradeServICT.",
}, io_manager_key="vanilla_parquet_io_manager")
def international_trade_in_ict_services_value_shares_and_growth_annual_500kb_():
    return download_dataset('US_TradeServICT')
    

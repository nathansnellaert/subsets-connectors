
from dagster import asset
from .utils import download_dataset
from .source import unctad

@asset(metadata={
    "source": unctad,
    "name": "Volume growth rates of merchandise exports and imports, quarterly (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/US.MerchVolumeQuarterly.",
}, io_manager_key="vanilla_parquet_io_manager")
def volume_growth_rates_of_merchandise_exports_and_imports_quarterly():
    return download_dataset('US_MerchVolumeQuarterly')
    

@asset(metadata={
    "source": unctad,
    "name": "Merchandise: Total trade and share, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/US.TradeMerchTotal.",
}, io_manager_key="vanilla_parquet_io_manager")
def merchandise_total_trade_and_share_annual():
    return download_dataset('US_TradeMerchTotal')
    

@asset(metadata={
    "source": unctad,
    "name": "Merchandise: Total trade growth rates, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/US.TradeMerchGR.",
}, io_manager_key="vanilla_parquet_io_manager")
def merchandise_total_trade_growth_rates_annual():
    return download_dataset('US_TradeMerchGR')
    

@asset(metadata={
    "source": unctad,
    "name": "Merchandise: Trade balance, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/US.TradeMerchBalance.",
}, io_manager_key="vanilla_parquet_io_manager")
def merchandise_trade_balance_annual():
    return download_dataset('US_TradeMerchBalance')
    

@asset(metadata={
    "source": unctad,
    "name": "Merchandise: Intra-trade and extra-trade of country groups by product, annual (~100MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/US.IntraTrade.",
}, io_manager_key="vanilla_parquet_io_manager")
def merchandise_intra_trade_and_extra_trade_of_country_groups_by_product_annual():
    return download_dataset('US_IntraTrade')
    

@asset(metadata={
    "source": unctad,
    "name": "Merchandise: Trade value, volume, unit value, terms of trade indices and purchasing power index of exports, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/US.TermsOfTrade.",
}, io_manager_key="vanilla_parquet_io_manager")
def merchandise_trade_value_volume_unit_value_terms_of_trade_indices_and_purchasing_power_index_of_exports_annual():
    return download_dataset('US_TermsOfTrade')
    

@asset(metadata={
    "source": unctad,
    "name": "Merchandise: Product concentration and diversification indices, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/US.ConcentDiversIndices.",
}, io_manager_key="vanilla_parquet_io_manager")
def merchandise_product_concentration_and_diversification_indices_annual():
    return download_dataset('US_ConcentDiversIndices')
    

@asset(metadata={
    "source": unctad,
    "name": "Merchandise: Market concentration and structural change indices, annual (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/US.ConcentStructIndices.",
}, io_manager_key="vanilla_parquet_io_manager")
def merchandise_market_concentration_and_structural_change_indices_annual_1mb_():
    return download_dataset('US_ConcentStructIndices')
    

@asset(metadata={
    "source": unctad,
    "name": "Revealed comparative advantage index, annual (~10MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/US.RCA.",
}, io_manager_key="vanilla_parquet_io_manager")
def revealed_comparative_advantage_index_annual_10mb_():
    return download_dataset('US_RCA')
    

@asset(metadata={
    "source": unctad,
    "name": "Import tariff rates on non-agricultural and non-fuel products, annual (~50MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/US.Tariff.",
}, io_manager_key="vanilla_parquet_io_manager")
def import_tariff_rates_on_non_agricultural_and_non_fuel_products_annual_50mb_():
    return download_dataset('US_Tariff')
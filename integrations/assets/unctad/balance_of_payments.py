from dagster import asset
from .utils import download_dataset
from .source import unctad

@asset(metadata={
    "source": unctad,
    "name": "Balance of payments, Current account balance",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/US.CurrAccBalance.",
}, io_manager_key="vanilla_parquet_io_manager")
def balance_of_payments_current_account_balance_annual():
    return download_dataset('US_CurrAccBalance')
    

@asset(metadata={
    "source": unctad,
    "name": "Foreign direct investment: Inward and outward flows and stock",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/US.FdiFlowsStock.",
}, io_manager_key="vanilla_parquet_io_manager")
def foreign_direct_investment_inward_and_outward_flows_and_stock_annual():
    return download_dataset('US_FdiFlowsStock')
    

@asset(metadata={
    "source": unctad,
    "name": "Personal remittances: receipts and payments",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/US.Remittances.",
}, io_manager_key="vanilla_parquet_io_manager")
def personal_remittances_receipts_and_payments_annual():
    return download_dataset('US_Remittances')
    

@asset(metadata={
    "source": unctad,
    "name": "Goods and Services (BPM6): Exports and imports of goods and services",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/US.GoodsAndServicesBpm6.",
}, io_manager_key="vanilla_parquet_io_manager")
def goods_and_services_bpm6_exports_and_imports_of_goods_and_services_annual():
    return download_dataset('US_GoodsAndServicesBpm6')
    

@asset(metadata={
    "source": unctad,
    "name": "Goods and services (BPM6): Trade balance indicators",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/US.GoodsAndServBalanceBpm6.",
}, io_manager_key="vanilla_parquet_io_manager")
def goods_and_services_bpm6_trade_balance_indicators_annual():
    return download_dataset('US_GoodsAndServBalanceBpm6')
    

@asset(metadata={
    "source": unctad,
    "name": "Goods and services (BPM6): Trade openness indicators",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/US.GoodsAndServTradeOpennessBpm6.",
}, io_manager_key="vanilla_parquet_io_manager")
def goods_and_services_bpm6_trade_openness_indicators_annual():
    return download_dataset('US_GoodsAndServTradeOpennessBpm6')
    
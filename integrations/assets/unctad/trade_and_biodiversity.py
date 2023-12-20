from dagster import asset
from .utils import download_dataset

@asset(metadata={
    "source": "unctad",
    "name": "Trade, growth rates, annual",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USBiotradeMerchGR.",
}, io_manager_key="vanilla_parquet_io_manager")
def trade_growth_rates_annual():
    return download_dataset('US_BiotradeMerchGR')
    
            
@asset(metadata={
    "source": "unctad",
    "name": "Biotrade as percentage of total trade, annual",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USBiotradeMerchShare.",
}, io_manager_key="vanilla_parquet_io_manager")
def biotrade_as_percentage_of_total_trade_annual():
    return download_dataset('US_BiotradeMerchShare')
    

@asset(metadata={
    "source": "unctad",
    "name": "Biotrade as percentage of Gross Domestic Product, annual",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USBioTradeMerchGDPShare.",
}, io_manager_key="vanilla_parquet_io_manager")
def biotrade_as_percentage_of_gross_domestic_product_annual():
    return download_dataset('US_BioTradeMerchGDPShare')
    

@asset(metadata={
    "source": "unctad",
    "name": "Product concentration indices of exports and imports, annual",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USBioTradeMerchProdConcent.",
}, io_manager_key="vanilla_parquet_io_manager")
def product_concentration_indices_of_exports_and_imports_annual():
    return download_dataset('US_BioTradeMerchProdConcent')
    

@asset(metadata={
    "source": "unctad",
    "name": "Market concentration indices of exports and imports of products, annual",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USBioTradeMerchMarketConcent.",
}, io_manager_key="vanilla_parquet_io_manager")
def market_concentration_indices_of_exports_and_imports_of_products_annual():
    return download_dataset('US_BioTradeMerchMarketConcent')
    

@asset(metadata={
    "source": "unctad",
    "name": "Market structural change indices of exports and imports of products, annual",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USBioTradeMerchStructChange.",
}, io_manager_key="vanilla_parquet_io_manager")
def market_structural_change_indices_of_exports_and_imports_of_products_annual():
    return download_dataset('US_BioTradeMerchStructChange')
    

@asset(metadata={
    "source": "unctad",
    "name": "Revealed comparative advantage index, annual",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USBiotradeMerchRCA.",
}, io_manager_key="vanilla_parquet_io_manager")
def revealed_comparative_advantage_index_annual():
    return download_dataset('US_BiotradeMerchRCA')
    
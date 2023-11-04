from dagster import asset
from .utils import download_dataset

@asset(metadata={
    "source": "unctad",
    "name": "Trade and Biodiversity - Trade in thousands of United States dollars, annual (~900MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USBiotradeMerch.",
})
def trade_and_biodiversity_trade_in_thousands_of_united_states_dollars_annual():
    return download_dataset('US_BiotradeMerch')
    

@asset(metadata={
    "source": "unctad",
    "name": "Trade and Biodiversity - Trade, growth rates, annual (~900MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USBiotradeMerchGR.",
})
def trade_and_biodiversity_trade_growth_rates_annual():
    return download_dataset('US_BiotradeMerchGR')
    

@asset(metadata={
    "source": "unctad",
    "name": "Trade and Biodiversity - Trade balance in thousands of United States dollars, annual (~350MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USBiotradeMerchB.",
})
def trade_and_biodiversity_trade_balance_in_thousands_of_united_states_dollars_annual():
    return download_dataset('US_BiotradeMerch_B')
    

@asset(metadata={
    "source": "unctad",
    "name": "Trade and Biodiversity - Trade balance, growth rates, annual (~300MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USBiotradeMerchGRB.",
})
def trade_and_biodiversity_trade_balance_growth_rates_annual():
    return download_dataset('US_BiotradeMerchGR_B')
    

@asset(metadata={
    "source": "unctad",
    "name": "Trade and Biodiversity - Trade of priority products in thousands of United States dollars, annual (~600MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USBiotradeMerchPrioProdGR.",
})
def trade_and_biodiversity_trade_of_priority_products_in_thousands_of_united_states_dollars_annual():
    return download_dataset('US_BiotradeMerchPrioProdGR')
    

@asset(metadata={
    "source": "unctad",
    "name": "Trade and Biodiversity - Trade of priority products, growth rates, annual (~500MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USBiotradeMerchPrioProd.",
})
def trade_and_biodiversity_trade_of_priority_products_growth_rates_annual():
    return download_dataset('US_BiotradeMerchPrioProd')
    

@asset(metadata={
    "source": "unctad",
    "name": "Trade and Biodiversity - Biotrade as percentage of total trade, annual (~10MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USBiotradeMerchShare.",
})
def trade_and_biodiversity_biotrade_as_percentage_of_total_trade_annual():
    return download_dataset('US_BiotradeMerchShare')
    

@asset(metadata={
    "source": "unctad",
    "name": "Trade and Biodiversity - Biotrade as percentage of Gross Domestic Product, annual (~10KB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USBioTradeMerchGDPShare.",
})
def trade_and_biodiversity_biotrade_as_percentage_of_gross_domestic_product_annual():
    return download_dataset('US_BioTradeMerchGDPShare')
    

@asset(metadata={
    "source": "unctad",
    "name": "Trade and Biodiversity - Product concentration indices of exports and imports, annual (~20KB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USBioTradeMerchProdConcent.",
})
def trade_and_biodiversity_product_concentration_indices_of_exports_and_imports_annual():
    return download_dataset('US_BioTradeMerchProdConcent')
    

@asset(metadata={
    "source": "unctad",
    "name": "Trade and Biodiversity - Market concentration indices of exports and imports of products, annual (~300KB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USBioTradeMerchMarketConcent.",
})
def trade_and_biodiversity_market_concentration_indices_of_exports_and_imports_of_products_annual():
    return download_dataset('US_BioTradeMerchMarketConcent')
    

@asset(metadata={
    "source": "unctad",
    "name": "Trade and Biodiversity - Market structural change indices of exports and imports of products, annual (~300KB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USBioTradeMerchStructChange.",
})
def trade_and_biodiversity_market_structural_change_indices_of_exports_and_imports_of_products_annual():
    return download_dataset('US_BioTradeMerchStructChange')
    

@asset(metadata={
    "source": "unctad",
    "name": "Trade and Biodiversity - Revealed comparative advantage index, annual (~15MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USBiotradeMerchRCA.",
})
def trade_and_biodiversity_revealed_comparative_advantage_index_annual():
    return download_dataset('US_BiotradeMerchRCA')
    

from dagster import asset
from .utils import download_dataset

@asset(metadata={
    "source": "unctad",
    "name": "Values and shares of creative goods exports, annual (~2GB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USCreativeGoodsValueE.",
})
def values_and_shares_of_creative_goods_exports_annual():
    return download_dataset('US_CreativeGoodsValue_E')
    

@asset(metadata={
    "source": "unctad",
    "name": "Values and shares of creative goods imports, annual (~2GB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USCreativeGoodsValueI.",
})
def values_and_shares_of_creative_goods_imports_annual():
    return download_dataset('US_CreativeGoodsValue_I')
    

@asset(metadata={
    "source": "unctad",
    "name": "Growth rates of creative goods exports and imports, annual (~1GB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USCreativeGoodsGR.",
})
def growth_rates_of_creative_goods_exports_and_imports_annual():
    return download_dataset('US_CreativeGoodsGR')
    

@asset(metadata={
    "source": "unctad",
    "name": "Market concentration index of creative goods exports and imports, annual (~5MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USCreativeGoodsIndex.",
})
def market_concentration_index_of_creative_goods_exports_and_imports_annual():
    return download_dataset('US_CreativeGoodsIndex')
    

@asset(metadata={
    "source": "unctad",
    "name": "Creative services exports of selected groups of economies (experimental) (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USCreativeServGroupE.",
})
def creative_services_exports_of_selected_groups_of_economies_experimental():
    return download_dataset('US_CreativeServ_Group_E')
    

@asset(metadata={
    "source": "unctad",
    "name": "International trade in creative services: estimates for individual economies (experimental) (~1MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USCreativeServIndivTot.",
})
def international_trade_in_creative_services_estimates_for_individual_economies_experimental():
    return download_dataset('US_CreativeServ_Indiv_Tot')
    
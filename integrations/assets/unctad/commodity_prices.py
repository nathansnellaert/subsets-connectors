from dagster import asset
from .utils import download_dataset

@asset(metadata={
    "source": "unctad",
    "name": "Free market commodity prices indices, annual (2015=100)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USCommodityPriceIndicesA.",
})
def free_market_commodity_prices_indices_annual_2015_100():
    return download_dataset('US_CommodityPriceIndices_A')
    

@asset(metadata={
    "source": "unctad",
    "name": "Free market commodity prices, annual",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USCommodityPriceA.",
})
def free_market_commodity_prices_annual():
    return download_dataset('US_CommodityPrice_A')
    

@asset(metadata={
    "source": "unctad",
    "name": "Free market commodity prices indices, monthly (2015=100)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USCommodityPriceIndicesM.",
})
def free_market_commodity_prices_indices_monthly():
    return download_dataset('US_CommodityPriceIndices_M')
    

@asset(metadata={
    "source": "unctad",
    "name": "Free market commodity prices, monthly",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USCommodityPriceM.",
})
def free_market_commodity_prices_monthly():
    return download_dataset('US_CommodityPrice_M')
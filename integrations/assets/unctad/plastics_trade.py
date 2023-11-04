
from dagster import asset
from .utils import download_dataset

@asset(metadata={
    "source": "unctad",
    "name": "Global plastics trade, annual (~10MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USPlasticsTrade.",
})
def global_plastics_trade_annual():
    return download_dataset('US_PlasticsTrade')
    

@asset(metadata={
    "source": "unctad",
    "name": "Plastics trade by partner, annual (~300MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USPlasticsTradebyPartner.",
})
def plastics_trade_by_partner_annual():
    return download_dataset('US_PlasticsTradebyPartner')
    

@asset(metadata={
    "source": "unctad",
    "name": "Spotlight on selected plastic trade trends, by economy and by partner (~100MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USHiddenPlasticsTradebyPartner.",
})
def spotlight_on_selected_plastic_trade_trends_by_economy_and_by_partner():
    return download_dataset('US_HiddenPlasticsTradebyPartner')
    

@asset(metadata={
    "source": "unctad",
    "name": "Associated plastics trade by partner, annual (~150MB)",
    "description": "This dataset was downloaded from UNCTADStat. More information about this dataset can be found at https://unctadstat.unctad.org/datacentre/reportInfo/USAssociatedPlasticsTradebyPartner.",
})
def associated_plastics_trade_by_partner_annual():
    return download_dataset('US_AssociatedPlasticsTradebyPartner')
    
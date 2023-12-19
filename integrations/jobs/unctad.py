from dagster import (
    load_assets_from_modules,
    define_asset_job,
)
from integrations.assets.unctad import (
    balance_of_payments,
    commodity_prices,
    creative_economy,
    digital_economy,
    inflation_and_exchange_rates,
    international_merchandise_trade,
    international_trade_in_services,
    maritime_transport,
    output_and_income,
    plastics_trade,
    population,
    productive_capacities,
    public_finance,
    technology_and_innovation,
    trade_and_biodiversity,
    transport_costs,
)


assets = load_assets_from_modules([
    balance_of_payments,
    commodity_prices,
    creative_economy,
    digital_economy,
    inflation_and_exchange_rates,
    international_merchandise_trade,
    international_trade_in_services,
    maritime_transport,
    output_and_income,
    plastics_trade,
    population,
    productive_capacities,
    public_finance,
    technology_and_innovation,
    trade_and_biodiversity,
    transport_costs,
])

job = define_asset_job(
    name='unctad_asset_job',
    selection=assets,
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 1,    
                },
            }
        }
    }
)
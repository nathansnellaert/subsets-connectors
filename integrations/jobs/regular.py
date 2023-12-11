from dagster import (
    load_assets_from_modules,
    define_asset_job,
)
import integrations.assets.geo.assets as geo_assets
import integrations.assets.tsa.assets as tsa_assets
import integrations.assets.our_world_in_data.assets as our_world_in_data_assets
import integrations.assets.zillow.assets as zillow_assets
import integrations.assets.ycombinator.assets as ycombinator_assets
import integrations.assets.economist.assets as economist_assets
import integrations.assets.worldbank.world_development_indicators as worldbank_world_development_indicators
import integrations.assets.fmp.end_of_day_prices as fmp_assets
import integrations.assets.economist.assets as economist_assets
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
    geo_assets,
    tsa_assets,
    our_world_in_data_assets,
    zillow_assets,
    ycombinator_assets,
    economist_assets,
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
    worldbank_world_development_indicators,
])

job = define_asset_job(
    name='regular',
    selection=assets,
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 4,    
                },
            }
        }
    }
)
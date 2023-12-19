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
import integrations.assets.economist.assets as economist_assets


assets = load_assets_from_modules([
    geo_assets,
    tsa_assets,
    our_world_in_data_assets,
    zillow_assets,
    ycombinator_assets,
    economist_assets,
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
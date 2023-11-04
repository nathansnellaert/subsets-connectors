from dagster import define_asset_job, load_assets_from_modules
import integrations.assets.fred.assets as fred_assets

assets = load_assets_from_modules(modules=[fred_assets])

job = define_asset_job(
    name='fred',
    selection=assets
)
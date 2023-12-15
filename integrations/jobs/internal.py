from dagster import define_asset_job, load_assets_from_modules
import integrations.assets.internal.assets as internal_assets

assets = load_assets_from_modules(modules=[internal_assets])

job = define_asset_job(
    name='internal_assets',
    selection=assets,
    tags={"concurrency_group": "internal"},
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
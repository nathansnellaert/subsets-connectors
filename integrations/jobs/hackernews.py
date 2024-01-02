from dagster import define_asset_job, load_assets_from_modules
import integrations.assets.ycombinator.hackernews as hackernews_assets

assets = load_assets_from_modules(modules=[hackernews_assets])

job = define_asset_job(
    name='hackernews',
    selection=assets,
    tags={"concurrency_group": "hackernews"},
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
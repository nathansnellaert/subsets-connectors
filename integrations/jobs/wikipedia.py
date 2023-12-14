from dagster import define_asset_job, load_assets_from_modules
import integrations.assets.wikimedia.assets as wikimedia_assets

assets = load_assets_from_modules(modules=[wikimedia_assets])

job = define_asset_job(
    name='wikipedia',
    selection=assets,
    tags={
        "concurrency_group": "wikipedia",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {
                        "cpu": "1000m",
                        "memory": "4Gi"
                    },
                    "limits": {
                        "cpu": "1000m",
                        "memory": "4Gi"
                    }
                }
            }
        }
    }
)
from dagster import (
    load_assets_from_modules,
    define_asset_job,
)
import integrations.assets.fmp.assets as fmp_assets

yearly_partition_assets = [
    fmp_assets.fmp_cash_flow_statement,
    fmp_assets.fmp_balance_sheet,
    fmp_assets.fmp_income_statement,
    fmp_assets.fmp_key_metrics
]

yearly_partition_job = define_asset_job(
    name='yearly_partition_job',
    selection=yearly_partition_assets,
    tags={"concurrency_group": "fmp"},
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

daily_partition_assets = [fmp_assets.fmp_eod_prices]

daily_partition_job = define_asset_job(
    name='daily_partition_job',
    selection=daily_partition_assets,
    tags={"concurrency_group": "fmp"}
)
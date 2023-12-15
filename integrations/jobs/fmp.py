from dagster import (
    load_assets_from_modules,
    define_asset_job,
)
import integrations.assets.fmp.end_of_day_prices as fmp_assets
import integrations.assets.fmp.dividends as dividends_assets
import integrations.assets.fmp.cash_flow_statements as cash_flow_statements_assets
import integrations.assets.fmp.balance_sheet_statements as balance_sheets_assets
import integrations.assets.fmp.income_statements as income_statements_assets
import integrations.assets.fmp.key_metrics as key_metrics_assets
import integrations.assets.fmp.senate_trading as senate_trading_assets
import integrations.assets.fmp.coverage as coverage_assets
import integrations.assets.fmp.employee_counts as employee_counts_assets
import integrations.assets.fmp.earnings_suprise as earnings_suprise_assets
import integrations.assets.fmp.executive_compensation as executive_compensation_assets
import integrations.assets.fmp.market_cap as market_cap_assets
import integrations.assets.fmp.esg_scores as esg_scores_assets

# yearly_partition_assets = [
#     fmp_assets.fmp_cash_flow_statement,
#     fmp_assets.fmp_balance_sheet,
#     fmp_assets.fmp_income_statement,
#     fmp_assets.fmp_key_metrics
# ]

# yearly_partition_job = define_asset_job(
#     name='yearly_partition_job',
#     selection=yearly_partition_assets,
#     tags={"concurrency_group": "fmp"},
#     config={
#         "execution": {
#             "config": {
#                 "multiprocess": {
#                     "max_concurrent": 1,
#                 },
#             }
#         }
#     }
# )

fmp_eod_assets = [fmp_assets.fmp_eod_prices, fmp_assets.indices_prices, fmp_assets.commodity_prices, fmp_assets.cryptocurrency_prices, fmp_assets.forex_prices]

fmp_eod_job = define_asset_job(
    name='fmp_eod_assets',
    selection=fmp_eod_assets,
    tags={"concurrency_group": "fmp"}
)

fmp_unpartitioned_assets = load_assets_from_modules(
    modules=[
        dividends_assets,
        cash_flow_statements_assets,
        balance_sheets_assets,
        income_statements_assets,
        key_metrics_assets,
        senate_trading_assets,
        coverage_assets,
        employee_counts_assets,
        earnings_suprise_assets,
        executive_compensation_assets,
        market_cap_assets,
        esg_scores_assets,
    ]
)

fmp_unpartitioned_assets_job = define_asset_job(
    name='fmp_unpartitioned_assets',
    selection=fmp_unpartitioned_assets,
    tags={"concurrency_group": "fmp"}
)
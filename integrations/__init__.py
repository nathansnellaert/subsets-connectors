import warnings
from dagster import ExperimentalWarning, Definitions, FilesystemIOManager
warnings.filterwarnings("ignore", category=ExperimentalWarning)
import os
from integrations.io_managers.gcs_parquet_io_manager import GCSParquetIOManager
from integrations.io_managers.vanilla_partitioned_parquet import VanillaPartitionedParquetIOManager
from integrations.jobs.fmp import (
    daily_partition_job as fmp_daily_partition_job,
    yearly_partition_job as fmp_yearly_partition_job,
    daily_partition_assets as fmp_daily_partition_assets,
    yearly_partition_assets as fmp_yearly_partition_assets,
    unpartitioned_assets as fmp_unpartitioned_assets,
    unpartitioned_job as fmp_unpartitioned_job
)
from integrations.jobs.fred import job as fred_job, assets as fred_assets
from integrations.jobs.regular import job as regular_job, assets as regular_assets
from integrations.jobs.wikipedia import job as wikipedia_job, assets as wikipedia_assets


ENV = os.environ.get("ENV", "dev")

io_manager = FilesystemIOManager() if ENV == 'test' else GCSParquetIOManager(os.environ['GCS_BUCKET'])

defs = Definitions(
    assets=fmp_daily_partition_assets + fmp_yearly_partition_assets + fred_assets + regular_assets + wikipedia_assets + fmp_unpartitioned_assets,
    jobs=[fmp_daily_partition_job, fmp_yearly_partition_job, fred_job, regular_job, wikipedia_job, fmp_unpartitioned_job],
    resources={
        "io_manager": io_manager,
        "vanilla_parquet_io_manager": VanillaPartitionedParquetIOManager()
    },
)

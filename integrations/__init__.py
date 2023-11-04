from dotenv import load_dotenv
load_dotenv()

from dagster import (
    Definitions,
)
from integrations.io_managers.partitioned_parquet import LocalPartitionedParquetIOManager
from integrations.io_managers.vanilla_partitioned_parquet import VanillaPartitionedParquetIOManager
from integrations.jobs.fmp import daily_partition_job, yearly_partition_job, daily_partition_assets, yearly_partition_assets
from integrations.jobs.fred import job as fred_job, assets as fred_assets
from integrations.jobs.regular import job as regular_job, assets as regular_assets
from integrations.jobs.wikipedia import job as wikipedia_job, assets as wikipedia_assets

defs = Definitions(
    assets=daily_partition_assets + yearly_partition_assets + fred_assets + regular_assets + wikipedia_assets,
    jobs=[daily_partition_job, yearly_partition_job, fred_job, regular_job,wikipedia_job],
    resources={
        "io_manager": LocalPartitionedParquetIOManager(),
        "vanilla_parquet_io_manager": VanillaPartitionedParquetIOManager()
    },

)
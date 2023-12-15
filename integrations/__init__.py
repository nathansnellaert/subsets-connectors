import warnings
from dagster import ExperimentalWarning, Definitions
warnings.filterwarnings("ignore", category=ExperimentalWarning)
import os
from integrations.io_managers.gcs_pandas_io_manager import GCSPandasIOManager
from integrations.io_managers.gcs_pyarrow_io_manager import GCSPyArrowIOManager
from integrations.io_managers.local_pandas_io_manager import LocalPandasIOManager
from integrations.io_managers.local_pyarrow_io_manager import LocalPyArrowIOManager
from integrations.jobs.fmp import (
    fmp_eod_assets,
    fmp_eod_job,
    fmp_unpartitioned_assets,
    fmp_unpartitioned_assets_job
)
from integrations.jobs.fred import job as fred_job, assets as fred_assets
from integrations.jobs.regular import job as regular_job, assets as regular_assets
from integrations.jobs.wikipedia import job as wikipedia_job, assets as wikipedia_assets
from integrations.jobs.internal import job as internal_job, assets as internal_assets

ENV = os.environ.get("ENV", "dev")

pandas_io_manager = LocalPandasIOManager() if ENV == 'test' else GCSPandasIOManager(os.environ['GCS_BUCKET'])
pyarrow_io_manager = LocalPyArrowIOManager() if ENV == 'test' else GCSPyArrowIOManager(os.environ['GCS_BUCKET'])

defs = Definitions(
    assets=fmp_eod_assets + fmp_unpartitioned_assets + fred_assets + regular_assets + wikipedia_assets + internal_assets,
    jobs=[fmp_eod_job, fmp_unpartitioned_assets_job, fred_job, regular_job, wikipedia_job, internal_job],
    resources={
        "io_manager": pandas_io_manager,
        "vanilla_parquet_io_manager": pyarrow_io_manager 
    },
)

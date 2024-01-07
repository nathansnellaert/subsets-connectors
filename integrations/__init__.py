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
from integrations.jobs.unctad import job as unctad_job, assets as unctad_assets
from integrations.jobs.hackernews import job as hackernews_job, assets as hackernews_assets

DATA_STORAGE = os.environ.get("DATA_STORAGE", "local")

if DATA_STORAGE == 'local':
    resources = {
        "io_manager": LocalPandasIOManager(path=os.environ['DAGSTER_DATA_DIR']),
        "vanilla_parquet_io_manager": LocalPyArrowIOManager(path=os.environ['DAGSTER_DATA_DIR'])
    }
elif DATA_STORAGE == 'gcs':
    resources = {
        "io_manager": GCSPandasIOManager(
            os.environ['GCP_PROJECT_ID'],
            os.environ['GCS_BUCKET_NAME']
        ),
        "vanilla_parquet_io_manager": GCSPyArrowIOManager(
            os.environ['GCP_PROJECT_ID'],
            os.environ['GCS_BUCKET_NAME']
        )
    }
else:
    raise Exception(f"Unknown data storage type: {DATA_STORAGE}")

assets = [
    *fmp_eod_assets,
    *fmp_unpartitioned_assets,
    *fred_assets,
    *regular_assets,
    *wikipedia_assets,
    *internal_assets,
    *unctad_assets,
    *hackernews_assets,
]

jobs = [
    fmp_eod_job,
    fmp_unpartitioned_assets_job,
    fred_job,
    regular_job,
    wikipedia_job,
    internal_job,
    unctad_job,
    hackernews_job,
]
defs = Definitions(
    assets=assets,
    jobs=jobs,
    resources=resources
)

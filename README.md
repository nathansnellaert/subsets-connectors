# Subsets Connectors

This project contains the data connectors used to build the subset data warehouse. It currently contains 120+ datasets including an index of YC companies, U.S. house prices, and Wikipedia search volumes. 

It is built with [Dagster](https://dagster.io/), a python-based data orchestrator.

## Project Setup

This project utilizes `pyproject.toml` for managing build tool configurations and dependencies. To set up the project:

1. Ensure you have Poetry installed: `pip install poetry`.
2. Navigate to the project root directory containing the `pyproject.toml` file.
3. Activate the virtual environment with: `poetry shell`.
4. Install the project dependencies with: `poetry install`.

## Environment Variables

This project relies on certain environment variables to function correctly:

- `DAGSTER_HOME`: Directory where dagster will store its metadata. See the [official docs](https://docs.dagster.io/deployment/dagster-instance) for more information.

### Storage

Set the `DATA_STORAGE` environment variable to either 'local' or 'gcs'. For local storage, set the `DAGSTER_DATA_DIR` environment variable. For GCS storage, set the `GCS_BUCKET_NAME` environment variable. Storage on GCS assumes you are already authenticated with GCP.

### Job-specific configuration
`fred` requires FRED_API_KEY to be set. An API key can be obtained from [Federal Reserve Economic Data](https://fred.stlouisfed.org/docs/api/fred/)
`fmp_eod_assets` and `fmp_unpartitioned_assets` require FMP_API_KEY, which offers free and paid tiers. An API key can be obtained from
[Financial Modeling Prep](https://financialmodelingprep.com/developer/docs/)

## Development
`dagster dev` is the easiest way to develop Dagster locally. It will run the Dagster daemon and Dagit UI in the background, and will automatically reload the pipeline when changes are made. See the [dagster docs](https://docs.dagster.io/deployment/open-source) for deploying in production.

## Contributing

We welcome contributions to this project. 

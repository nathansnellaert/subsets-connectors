# Subsets Connectors

This project contains the data connectors used to build the subset data warehouse. It currently contains 100+ datasets, such as:

- Commodity prices
- Federal reserve economic data
- Company financials
- Daily page views for all english wikipedia pages
- World Development Indicators

It is built with [Dagster](https://dagster.io/), a python-based data orchestrator.

## Project Setup

This project utilizes `pyproject.toml` for managing build tool configurations and dependencies. To set up the project:

1. Ensure you have Poetry installed: `pip install poetry`.
2. Navigate to the project root directory containing the `pyproject.toml` file.
3. Activate the virtual environment with: `poetry shell`.
4. Install the project dependencies with: `poetry install`.
5. Set the environment variables in `.env.example` and rename the file to `.env`. Some environment variables are connector specific, and are not needed if you do not plan to pull data from that connector.

## Environment Variables

This project relies on certain environment variables to function correctly:

- `DAGSTER_HOME`: Directory with dagster configuration files. This is where the dagster daemon and dagit UI will be run from.
- `DAGSTER_DATA_DIR`: Directory with outputs from data asset materializations. The project currently uses a parquet IO manager to store data. 


## Development

`dagster dev` is the easiest way to develop Dagster locally. It will run the Dagster daemon and Dagit UI in the background, and will automatically reload the pipeline when changes are made. See the [dagster docs](https://docs.dagster.io/deployment/open-source) for deploying in production.

## Contributing

We welcome contributions to this project. 

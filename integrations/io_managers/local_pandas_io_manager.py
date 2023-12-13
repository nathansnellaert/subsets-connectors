import os
from typing import Union
import pandas as pd

from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    ResourceDependency,
    _check as check,
)

class LocalPandasIOManager(ConfigurableIOManager):
    @property
    def _base_path(self):
        return os.environ['DAGSTER_DATA_DIR']  

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        path = self._get_path(context)
        if "://" not in self._base_path:
            os.makedirs(os.path.dirname(path), exist_ok=True)

        if isinstance(obj, pd.DataFrame):
            row_count = len(obj)
            context.log.info(f"Row count: {row_count}")
            obj.to_parquet(path=path, index=False)
        else:
            raise Exception(f"Outputs of type {type(obj)} not supported.")

        context.add_output_metadata({"row_count": row_count, "path": path})

    def load_input(self, context: InputContext) -> Union[pd.DataFrame, str]:
        path = self._get_path(context)
        if context.dagster_type.typing_type == pd.DataFrame:
            # return pandas dataframe
            return pd.read_parquet(path)

        return check.failed(
            f"Inputs of type {context.dagster_type} not supported. Please specify a valid type "
            "for this input either on the argument of the @asset-decorated function."
        )

    def _get_path(self, context: Union[InputContext, OutputContext]):
        key = context.asset_key.path[-1]

        if context.has_asset_partitions:
            partition_key = context.partition_key
            # Construct the file path using the partition key
            return os.path.join(self._base_path, key, f"{partition_key}.parquet")
        else:
            # Construct the file path using only the asset key
            return os.path.join(self._base_path, f"{key}.parquet")

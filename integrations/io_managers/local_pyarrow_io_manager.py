import os
from dagster import IOManager, OutputContext, InputContext
import pyarrow as pa
import pyarrow.parquet as pq

class LocalPyArrowIOManager(IOManager):

    @property
    def _base_path(self):
        return os.environ['DAGSTER_DATA_DIR']

    def _get_path(self, context: OutputContext):
        key = context.asset_key.path[-1]

        if context.has_asset_partitions:
            partition_key = context.partition_key
            # Construct the file path using the partition key
            return os.path.join(self._base_path, key, f"{partition_key}.parquet")
        else:
            # Construct the file path using only the asset key
            return os.path.join(self._base_path, f"{key}.parquet")

    def handle_output(self, context: OutputContext, obj: pa.Table):
        path = self._get_path(context)
        # Ensure the directory exists
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # Using PyArrow to write the table to a Parquet file
        pq.write_table(obj, path)
        context.log.info(f"Wrote Parquet file to {path}")

    def load_input(self, context: InputContext) -> pa.Table:
        path = self._get_path(context)
        return pq.read_table(path)

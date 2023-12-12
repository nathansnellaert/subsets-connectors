import os
from typing import Union
import pandas as pd
from google.cloud import storage
from dagster import IOManager, InputContext, OutputContext, _check as check
from io import BytesIO

class GCSParquetIOManager(IOManager):
    def __init__(self, gcs_bucket_name: str = None):
        self.gcs_bucket_name = gcs_bucket_name
        self.gcs_client = storage.Client()

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        gcs_path = self._get_gcs_path(context)
        row_count = len(obj)
        context.log.info(f"Row count: {row_count}")

        buffer = BytesIO()
        obj.to_parquet(buffer, index=False)
        buffer.seek(0)  
        self._upload_to_gcs(gcs_path, buffer)
        context.add_output_metadata({"row_count": row_count, "path": gcs_path})

    def load_input(self, context: InputContext) -> Union[pd.DataFrame, str]:
        gcs_path = self._get_gcs_path(context)
        full_path = f"gs://{self.gcs_bucket_name}/{gcs_path}"
        return pd.read_parquet(full_path)
    
    def _get_gcs_path(self, context: Union[InputContext, OutputContext]):
        key = context.asset_key.path[-1]
        if context.has_asset_partitions:
            partition_key = context.partition_key
            return f"{key}/{partition_key}.parquet"
        else:
            return f"{key}.parquet"

    def _upload_to_gcs(self, gcs_path, buffer):
        bucket = self.gcs_client.bucket(self.gcs_bucket_name)
        blob = bucket.blob(gcs_path)
        blob.upload_from_file(buffer, content_type='application/octet-stream')


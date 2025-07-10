# here connection to clickhouse via airflow with clickhouse_hook
import os
from project_functions.python.minio_client import get_minio_client
import pandas as pd
import io


# retrieve data
class MinioUtils:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.minio_client = get_minio_client()

    def retrieve_data(self, object_name: str) -> pd.DataFrame:
        """
        Retrieve data from MinIO bucket.
        """
        if not self.bucket_name or not object_name:
            raise ValueError("Bucket name and object name must be provided.")
        try:
            response = self.minio_client.get_object(self.bucket_name, object_name) # HTTPResponse
            return pd.read_parquet(io.BytesIO(response.read())) # Convert response to DataFrame

        except Exception as e:
            print(f"Error retrieving data from MinIO: {e}")
            return None

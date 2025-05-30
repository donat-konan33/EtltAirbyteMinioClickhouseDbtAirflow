import os
from project_functions.python.minio_client import get_minio_client
from project_functions.python.clickhouse_hook import ClickHouseHook
import pandas as pd
import io
from typing import Optional
# retrieve data
class MinioUtils:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.minio_client = get_minio_client()
        self.clickhouse_hook = ClickHouseHook()

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

    def load_data_to_clickhouse(self, table_name: str, data: pd.DataFrame, check_if_exists: Optional[bool] = False) -> None:
        """
        Load data into ClickHouse table.
        raw_weather table has columns 'preciptype', 'stations' contain lists, but ClickHouse does not support list type.
        Therefore, we convert these columns to string before loading.
        if raw_weather table is to be created, set check_if_exists to True otherwise False.
        """
        try:
            client = self.clickhouse_hook.get_conn()
            if not client:
                raise ValueError("ClickHouse client is not initialized.")
            if data.empty:
                raise ValueError("Data to be loaded is empty.")
            else :
                print(f"Loading data into ClickHouse table {table_name}...")
                if check_if_exists:
                    # Check if the table exists
                    if not client.has_table(table_name):
                        raise ValueError(f"Table {table_name} does not exist in ClickHouse.")
                # convert preciptype and stations columns because clickhouse does not support list type
                    for col in ['preciptype', 'stations']:
                        if col in data.columns:
                            data[col] = data[col].apply(lambda x: str(x) if not pd.isna(x) else x)
                else :
                    data["dep_status"] = data["dep_status"].astype("string")
                # Insert data into ClickHouse table
                client.insert_df(table_name, df=data)
                print(f"Data loaded into ClickHouse table {table_name} successfully.")
        except Exception as e:
            print(f"Error loading data to ClickHouse: {e}")

import os
import sys

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
sys.path.append(AIRFLOW_HOME)

from project_functions.python.minio_client import get_minio_client
from project_functions.python.clickhouse_hook import ClickHouseHook

minio_client = get_minio_client()
clickhouse_hook = ClickHouseHook()

# create table and schema in ClickHouse
def create_clickhouse_table(sql_file_path: str, table_name: str):
    """
    Create a ClickHouse table with the specified schema.
    Args ``table_name`` should mandatorily be those defined in the SQL file.
    """
    with open(sql_file_path, 'r') as file:
        sql = file.read()
    try:
        clickhouse_hook.run_query(sql)
        print(f"Table {table_name} created successfully.")
    except Exception as e:
        print(f"Error creating table in ClickHouse: {e}")

# retrieve data
def retrieve_data_from_minio(bucket_name: str, object_name: str) -> bytes:
    """
    Retrieve data from MinIO bucket.
    """
    try:
        data = minio_client.get_object(bucket_name, object_name)
        return data.read()
    except Exception as e:
        print(f"Error retrieving data from MinIO: {e}")
        return None

# load data to ClickHouse
def load_data_to_clickhouse(table_name: str, data: bytes):
    """
    Load data into ClickHouse table.
    """
    try:
        sql = f"INSERT INTO {table_name} FORMAT JSONEachRow"
        clickhouse_hook.run_query(sql, data)
        print(f"Data loaded into ClickHouse table {table_name} successfully.")
    except Exception as e:
        print(f"Error loading data to ClickHouse: {e}")

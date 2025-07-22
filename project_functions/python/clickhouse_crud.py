import os
from project_functions.python.clickhouse_hook import ClickHouseHook
from project_functions.python.clickhouse_client import ClickHouseClient
from typing import Union, Optional
import pandas as pd
import logging
import json
import shapely.wkb
from shapely import wkt
import textwrap # for formatting SQL queries

def wkb_to_wkt(x):
    try:
        return shapely.wkb.loads(x).wkt
    except Exception:
        return None

# Configuration for connecting to ClickHouse
def get_clickhouse_client() -> Union[ClickHouseHook, ClickHouseClient]:
    """
    Returns a ClickHouse client configured with the environment variables.
    """
    # Keep the logging level for ClickHouse and Airflow quiet to avoid cluttering the output
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING) # LOG FROM SQLALCHEMY
    logging.getLogger("airflow").setLevel(logging.WARNING) # LOG FROM AIRFLOW

    clickhouse_hook = None
    clickhouse_client = None
    try:
        print("Trying ClickHouseHook from Airflow...")
        clickhouse_hook = ClickHouseHook()
        print("Using ClickHouseHook via Airflow.")
        return clickhouse_hook  # Returns the ClickHouse connection from Airflow
    except Exception as e:
        print(f"ClickHouseHook failed, trying fallback ClickHouseClient: {e}")
        try:
            print("Trying ClickHouseClient...")
            clickhouse_client = ClickHouseClient()
            print("Using ClickHouseClient.")
            return clickhouse_client  # Returns the ClickHouse client
        except Exception as ex:
            print(f"ClickHouseClient also failed: {ex}")
            raise RuntimeError("Failed to initialize any ClickHouse client.")


# create table and schema in ClickHouse
class ClickHouseQueries:
    def __init__(self):
        self.clickhouse_client = get_clickhouse_client()

    def load_data_to_clickhouse(self, table_name: str, data: pd.DataFrame, is_to_truncate: bool=False) -> None:
        """
        Load data into ClickHouse table.
        raw_weather table has columns 'preciptype', 'stations' contain lists, but ClickHouse does not support list type.
        Therefore, we convert these columns to string before loading.
        if raw_weather table is to be created, set check_if_exists to True otherwise False.
        """
        try:
            if data.empty:
                raise ValueError("Data to be loaded is empty.")
            client = self.clickhouse_client
            if not client:
                raise ValueError("ClickHouse client is not initialized.")
        except ValueError as ve:
            print(f"Error: {ve}")

        print(f"Checking if table {table_name} exists...")
        # Check if the table exists
        if client.run_query(f"EXISTS TABLE {table_name}").loc[0, "result"] == 0: # if value is 0, then table does not exist
                raise ValueError(f"Table {table_name} does not exist in ClickHouse.")
        print(f"Table {table_name} exists in ClickHouse.")

        # Ensure the ClickHouse client is initialized
        try:
            print(f"Loading data into ClickHouse table {table_name}...")
            # convert preciptype and stations columns because clickhouse does not support list type
            for col in data.select_dtypes(include=["object"]).columns:
                data[col] = data[col].astype("string")

            if data.columns.isin(["geo_point_2d", "geo_shape"]).any(): # check whether at least one of the columns is in the DataFrame.columns Index
                # Need to convert data types for ClickHouse compatibility # transform type according to the target table in clickhousedb before loading it to.
                # for check_if_exists equals False, only if depcode table is input as data argument
                data["geo_point_2d"] = data["geo_point_2d"].apply(wkb_to_wkt)
                data["geo_shape"] = data["geo_shape"].apply(wkb_to_wkt)
                data["geo_shape"] = data["geo_shape"].apply(lambda w: wkt.loads(w).__geo_interface__ if w else None)  # Convert to GeoJSON format
                data["geo_shape"] = data["geo_shape"].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
                for col in ["reg_name", "reg_code", "dep_name_upper", "dep_current_code", "dep_status"]:
                    data[col] = data[col].apply(lambda x: str(x) if not pd.isna(x) else x)

            if is_to_truncate:
                client.get_conn().command(f"TRUNCATE TABLE {table_name}")  # Truncate the table if required
                client.get_conn().insert_df(table=table_name, df=data)
            # Insert data into ClickHouse table
            else:
                client.get_conn().insert_df(table=table_name, df=data)
            print(f"Data loaded into ClickHouse table {table_name} successfully.")
        except Exception as e:
            print(f"Error loading data to ClickHouse: {e}")

    def merge_daily_data(self, table_name: str, target_table_name: str) -> None:
        """
        Append daily data to the ArchivedData table in ClickHouse.
        args:
            table_name: str - Name of the ClickHouse table to append data from.
            target_table_name: str - Name of the ClickHouse table to append data to
            (data: pd.DataFrame - DataFrame containing the data to be appended).
        """
        client = self.clickhouse_client
        if not client:
            raise ValueError("ClickHouse client is not initialized.")

        if client.get_conn().query_df(query=f"SELECT * FROM {table_name}").empty:
            raise ValueError(f"Data from Table {table_name} to be appended is empty.")

        print(f"Appending data from ClickHouse table {table_name} to table {target_table_name}...")
        query = f"""
            SELECT * FROM {target_table_name}
            UNION ALL
            SELECT * FROM {table_name}
        """
        expected_query = textwrap.dedent(query).strip()
        client.get_conn().command(query=expected_query)
        print(f"Data appended to ClickHouse table {target_table_name} successfully.")

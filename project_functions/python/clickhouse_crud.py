import os
from project_functions.python.clickhouse_hook import ClickHouseHook

# Configuration for connecting to ClickHouse
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

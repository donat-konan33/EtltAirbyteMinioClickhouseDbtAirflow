

import os
import clickhouse_connect # for http connection to ClickHouse
conn_params = {
    'host': os.environ.get("CLICKHOUSE_HOST", "localhost"),
    'port': 8123,  # Default ClickHouse HTTP port), for http connection
    'username': os.environ.get("CLICKHOUSE_USER"),
    'password': os.environ.get("CLICKHOUSE_PASSWORD"),
    'database': os.environ.get("CLICKHOUSE_DB", "default")
}
# Configuration for connecting to ClickHouse, useful for testing and production environments.
class ClickHouseClient:
    def __init__(self):
        self.params = conn_params

    def get_conn(self): # it gives us clickhouse client
        return clickhouse_connect.get_client(
            host=self.params['host'],
            port=self.params['port'],
            username=self.params['username'],
            password=self.params['password'],
            database=self.params['database']
        )

    def run_query(self, sql):
        client = self.get_conn()
        result = client.query(sql)
        return result.result_rows

def create_clickhouse_table(sql_file_path: str, table_name: str):
    """
    Create a ClickHouse table with the specified schema.
    Args ``table_name`` should mandatorily be those defined in the SQL file.
    """
    with open(sql_file_path, 'r') as file:
        sql = file.read()
    try:
        ClickHouseClient().run_query(sql)
        print(f"Table {table_name} created successfully.")
    except Exception as e:
        print(f"Error creating table in ClickHouse: {e}")

# to store here : plugins/hooks/clickhouse_hook.py

from airflow.hooks.base import BaseHook #BaseHook centralizes the connection management
# and provides a common interface for all hooks in Airflow.
# This allows you to create custom hooks that can interact with external systems.

import clickhouse_connect # for http connection to ClickHouse

class ClickHouseHook(BaseHook):
    def __init__(self, conn_id='clickhouse_default'):
        super().__init__() # Initialize the base class and call parent constructor
        self.connection = self.get_connection(conn_id)

    def get_conn(self):
        return clickhouse_connect.get_client(
            host=self.connection.host,
            port=self.connection.port or 8123,
            username=self.connection.login,
            password=self.connection.password,
            database=self.connection.schema or 'default'
        )

    def run_query(self, sql):
        client = self.get_conn()
        result = client.query(sql)
        return result.result_rows

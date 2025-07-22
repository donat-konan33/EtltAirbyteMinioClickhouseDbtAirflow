import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from project_functions.python.clickhouse_crud import ClickHouseQueries, get_clickhouse_client
import textwrap
# fixtures for the tests : prepare resources for the tests

@pytest.fixture
def load_parquet_df():
    """
    retrieve data from real parquet file
    """
    def _load_parquet_df(path):
       return pd.read_parquet(path)
    return _load_parquet_df

@pytest.mark.parametrize("table_exists, should_raise",[
                        (1, False), # Table exists -> no raise
                        (0, True) # Table does not exist -> should raise ValueError
    ]
)
@patch("project_functions.python.clickhouse_crud.get_clickhouse_client")
def test_table_exists(mock_clickhouse_client, table_exists, should_raise):
    """
    Test if the ClickHouse table exists.
    """
    mock_client = MagicMock()
    mock_conn = MagicMock()
    # mock config
    mock_client.get_conn.return_value = mock_conn
    mock_client.run_query.return_value = pd.DataFrame([[table_exists]], columns=["result", ])
    mock_clickhouse_client.return_value = mock_client

    clickhouse_queries = ClickHouseQueries()
    if should_raise:
        with pytest.raises(ValueError, match="Table .* does not exist in ClickHouse."):
            clickhouse_queries.load_data_to_clickhouse(table_name="raw_weather", data=pd.DataFrame({"column1": [1, 2, 3]}))
    else:
        clickhouse_queries.load_data_to_clickhouse(table_name="raw_weather", data=pd.DataFrame({"column1": [1, 2, 3]}))
        mock_client.run_query.assert_called_once_with("EXISTS TABLE raw_weather")


@pytest.mark.parametrize("path, is_to_truncate, table_name",
                         [("tests/data/france_weather_2025-04-13.parquet", True, "raw_weather"),
                          ("tests/data/france_region_department96.parquet", False, "raw_depcode")]
)
@patch("project_functions.python.clickhouse_crud.get_clickhouse_client")
def test_load_data_to_clickhouse(mock_clickhouse_client, load_parquet_df, path, is_to_truncate, table_name):
    """
     Test load_data_to_clickhouse with multiple .parquet files and truncation options.
    """
    mock_client = MagicMock()
    mock_conn = MagicMock()
    # mock config
    mock_client.get_conn.return_value = mock_conn
    mock_client.run_query.return_value = pd.DataFrame([[1]], columns=["result", ]) # Simulate a response indicating the table exists, specifically for the check if table exists we get a DataFrame with a single value of 1 and column named "result"
    mock_clickhouse_client.return_value = mock_client

    # test different method of client
    clickhouse_queries = ClickHouseQueries()
    df = load_parquet_df(path)
    clickhouse_queries.load_data_to_clickhouse(table_name=table_name, data=df, is_to_truncate=is_to_truncate)

    # Check that the client was called with the correct parameters, after simulating loading data
    mock_client.run_query.assert_called_once_with(f"EXISTS TABLE {table_name}")

    if df.columns.isin(["geo_point_2d", "geo_shape"]).any():
        # Check that the conversion to string was applied to the appropriate columns
        assert list(df.columns) == ['geo_point_2d',
                              'geo_shape',
                            'reg_name',
                            'reg_code',
                            'dep_name_upper',
                            'dep_current_code',
                            'dep_status'
                            ], "DataFrame columns do not match expected depcode table."

    # Check that the truncate command was called if is_to_truncate is True
    if is_to_truncate:
        mock_conn.command.assert_called_once_with(f"TRUNCATE TABLE {table_name}")
        mock_conn.insert_df.assert_called_once_with(table=table_name, df=df)
    else:
        mock_conn.command.assert_not_called()
        mock_conn.insert_df.assert_called_once_with(table=table_name, df=df)


# test merge_daily_data method
@pytest.mark.parametrize("table_name, target_table_name",[
                             ("mart_newdata", "archived_data"),
                          ]
)
@patch("project_functions.python.clickhouse_crud.get_clickhouse_client")
def test_merge_daily_data(mock_clickhouse_client, table_name, target_table_name):
    """
    Test merge_daily_data method.
    """
    mock_client = MagicMock()
    mock_conn = MagicMock()
    # mock config
    mock_client.get_conn.return_value = mock_conn
    mock_clickhouse_client.return_value = mock_client
    mock_conn.query_df.return_value = pd.DataFrame([{"column1": "value1"}]) # Simulate a non-empty DataFrame for the query

    clickhouse_queries = ClickHouseQueries()
    clickhouse_queries.merge_daily_data(table_name=table_name, target_table_name=target_table_name)
    mock_conn.query_df.assert_called_once_with(query=f"SELECT * FROM {table_name}")
    # Check that the correct query was executed
    query = f"""
        SELECT * FROM {target_table_name}
        UNION ALL
        SELECT * FROM {table_name}
    """
    expected_query = textwrap.dedent(query).strip()
    mock_conn.command.assert_called_once_with(query=expected_query)

@patch("project_functions.python.clickhouse_crud.get_clickhouse_client")
def test_check_table_has_empty_data(mock_clickhouse_client):
    """Test check_table_has_empty_data method. raises ValueError if the table is empty."""
    # mock config
    mock_client = MagicMock()
    mock_conn = MagicMock()
    mock_client.get_conn.return_value = mock_conn
    mock_clickhouse_client.return_value = mock_client
    mock_conn.query_df.return_value = pd.DataFrame(columns=["column1"])  # 0 rows

    # instantiate ClickHouseQueries
    clickhouse_queries = ClickHouseQueries()

    with pytest.raises(ValueError, match="Data from Table .* to be appended is empty."):
        clickhouse_queries.merge_daily_data(table_name="mart_newdata", target_table_name="archived_data")
        mock_conn.query_df.assert_called_once_with("SELECT * FROM mart_newdata")

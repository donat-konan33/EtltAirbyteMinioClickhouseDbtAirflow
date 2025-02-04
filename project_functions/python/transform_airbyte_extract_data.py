import io
import pickle
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
from typing import List, Tuple
from google.cloud import storage


def get_name_of_blob(gs_path: List[str]) -> List[str]:
    """
    from gcs path find the name of file
    """
    new_list = [path.split("/")[-1] for path in gs_path]
    return new_list


def retrieve_diff_date(files_name: List[str]):
    """
    get different files according to the date of extraction
    """

    date_list = [file_name[:10] for file_name in files_name]
    different_date = set(date_list)
    return different_date


def concat_data_by_date(dates: str, client:storage.Client, bucket_name:str, prefix:str) -> pd.DataFrame:
    """
    date format like `YYYY_MM_DD`
    prefix like ``staging/weather_1/``
    goal: concatenate with pandas

    """
    bucket = client.bucket(bucket_name)
    list_of_blobs = bucket.list_blobs(prefix=prefix) # <google.api_core.page_iterator.HTTPIterator object at 0x7fe50d141d00>
    list_of_blobs = list(list_of_blobs)              # convert to list of blobs

    for date in dates:
        df = None
        for blob in list_of_blobs:
            if date in blob.name:
                df1 = read_parquet_from_blob(blob) # read blob from GCS
                if df is None:
                    df = df1
                else:
                    df = pd.concat([df, df1], ignore_index=True)
        # convert DataFrame Parquet format in memory (local storageless)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        # store staging data according to the date specified
        destination_blob_name = prefix + date.replace("_", "-") + "_airbyte.parquet"
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')
        print(f"✅ merging up ! new merged file recorded to: gs://{bucket_name}/{destination_blob_name}")

        if df is None:
            print(f"No files matching with date {date} specified.")
            return


def choose_first(address):
    """
    choose the first element from string separated by comma
    """
    # head,*tail = Iter.split(",") # or head = Iter.split(",")[0] for iterable using
    # let's vectorize this calculation task
    return np.vectorize(lambda x: x.split(',')[0])(address)

def read_parquet_from_blob(blob: storage.Blob) -> pd.DataFrame:
    """
    read parquet from a bucket blob and return dataframe
    """
    data = blob.download_as_bytes()
    table = pq.read_table(io.BytesIO(data))
    return table.to_pandas()


def create_table_from_airbyte_data(client: storage.Client, dates: set, bucket: str, source_prefix: str , staging_prefix: str) -> None:
    """
    create table from data extracted by airbyte that comes with json format

    """
    # connection to the bucket
    bucket = client.bucket(bucket)
    list_of_blobs = list(bucket.list_blobs(prefix=source_prefix))
    for date in dates:
        df = None
        for blob in list_of_blobs:
            date = date.replace("_", "-")
            if date in blob.name:
                df = read_parquet_from_blob(blob)
                df.drop(['_airbyte_additional_properties', '_airbyte_emitted_at'], axis=1, inplace=True)

                df_exploded = df.explode(column='days')
                df_days = pd.json_normalize(df_exploded['days'])
                df = df_exploded.drop(columns=["days",]).reset_index(drop=True).join(df_days)
                df["department"] = choose_first(df["resolvedAddress"].values)
                df.drop(columns=["_airbyte_additional_properties", ], inplace=True)
                output_path = f"{staging_prefix}france_weather_" + date + ".parquet"

                # convert DataFrame Parquet format in memory (local storageless)
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, index=False)
                parquet_buffer.seek(0)

                # store staging data according to the date specified
                blob = bucket.blob(output_path)
                blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')


def merge_gcs_files_by_date(diff_date: Tuple[str], client: storage.Client, bucket_name: str, prefix: str):
    """
    merge file with same format inplace.

    :param bucket_name: gcs bucket name
    :param destination_blob_name:
    goal : use a gcs object method named `.compose()`, it should help to combine (concatenate) the files with same formats and same structure
    """
    bucket = client.bucket(bucket_name)
    list_of_blobs = bucket.list_blobs(prefix=prefix)
    list_of_blobs = list(list_of_blobs)

    # retrieve existing dates from files name and retrieve object sources
    for date in diff_date:
        destination_blob_name = prefix + date + "-airbyte.parquet"
        destination_blob = bucket.blob(destination_blob_name)
        destination_blob.content_type = 'application/octet-stream'
        gather_date_blobs = [blob for blob in list_of_blobs if date in blob.name]

        destination_generation_match_precondition = 0

        # merge files
        destination_blob.compose(gather_date_blobs, if_generation_match=destination_generation_match_precondition)
        print(f"✅ merging up ! new file recorded : gs://{bucket_name}/{destination_blob_name}")

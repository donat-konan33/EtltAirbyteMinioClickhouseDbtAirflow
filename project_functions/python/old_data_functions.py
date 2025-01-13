# Importing necessary libraries

import os
import pathlib
import pandas as pd
from typing import List
#import fastparquet
#import aiofiles
#import asyncio
#import shutil

def transform_filename(filename:str ) -> str:
    """
    This function takes a filename in the format france_data_20250102-182912.csv  name file name like france_data_2025-01-02.csv
    """
    parts = filename.split("_")
    france_data = parts[0]
    date = parts[2].split("-")[0]
    year = date[:4]
    month = date[4:6]
    day = date[6:]
    new_filename = f"{france_data}_weather_{year}-{month}-{day}.csv"
    return new_filename


# load existing files from data/old_data
def get_existing_csv_paths() -> List[pathlib.Path]:
    """
    This function returns a list of paths to existing files in the data/old_data directory starting with france_data and ending with .csv
    """
    files_path = list(pathlib.Path("data/old_data").glob("france_data*.csv"))
    return files_path


def rename_filename(files_path: List[pathlib.Path]):
    """ modify file name from all filenamse given"""
    try:
        for filepath in files_path:
            filename = transform_filename(filepath.name)
            filename = filepath.with_name(filename)
            filepath.rename(filename)
    except Exception as e:
        print(f" Your file name doesn't match with this pattern `france_data_20250102-182912.csv`: {e}")
    finally:
        pass


# trannsform_catch data froom local
def tansform_data_to_parquet(file_path) -> None:
    """
    this function takes a csv file and transforms it to parquet
    """
    # Read the csv file
    df = pd.read_csv(file_path)
    # Save the parquet file
    parquet_file_name = file_path.with_suffix(".parquet")
    df.to_parquet(parquet_file_name, index=False)
    os.remove(file_path)


def read_csv_file(csv_path: List[pathlib.Path]) -> List[pd.DataFrame]:
    """
    create a list of dataframe objects for later concatenation data use
    """
    dataframes = [pd.read_csv(path) for path in csv_path]
    return dataframes


def write_data_from_old_data(input_file_dataframe: List[pd.DataFrame], output_file_path: str) -> None:
    """
    This funtion writes old data from old data into data/old_data and gathers them into a single file.
    file_path is here the path of file that should contain all data from data/old
    file_path should be like `data/old_data_transformed/test_file.parquet`
    return file that represents all data : where all old data are stored
    """
    file = pathlib.Path(output_file_path)
    if not file.exists():
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()
        columns = input_file_dataframe[0].columns
        df = pd.DataFrame(data=[], columns=columns)
        df.to_parquet(output_file_path, index=False)

    df = pd.read_parquet(output_file_path)
    df = pd.concat([df, *input_file_dataframe], ignore_index=True)
    df.to_parquet(output_file_path, index=False)



if __name__ == "__main__":

    """
    Concatenate all ol data to `data/old_data_transformed/france_weather_old_data.parquet`
    Once all files gathered, rename these one and change their format from `.csv` to `.parquet`
    """
    filespaths = get_existing_csv_paths()
    if filespaths != None:
        dataframes = read_csv_file(filespaths)
        final_data_path = pathlib.Path("data/old_data_transformed/france_weather_old_data.parquet")
        write_data_from_old_data(dataframes)
        rename_filename(filespaths)

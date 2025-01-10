# Importing necessary libraries
import aiofiles
import asyncio
import os
import pathlib
import shutil
import pandas as pd
from typing import Posix, List
import fastparquet

def transform_filename(filename) -> str:
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
def get_existing_paths() -> list[Posix]:
    """
    This function returns a list of paths to existing files in the data/old_data directory starting with france_data and ending with .csv
    """

    files_path = list(pathlib.Path("data/old_data").glob("france_data*.csv"))
    return files_path



# trannsform_catch data froom local
def tansform_data_to_parquet(file_path: Posix) -> None:
    """
    this function takes a csv file and transforms it to parquet
    """
    # Read the csv file
    df = pd.read_csv(file_path)
    # Save the parquet file
    parquet_file_name = file_path.with_suffix(".parquet")
    df.to_parquet(parquet_file_name, index=False)
    os.remove(file_path)


def write_data_from_old_data(file_path: Posix, content) -> None:
    """
    This funtion writes old data from old data into data/old_data and gathers them into a single file
    file_path is here the path of file with should contain all data from data/old
    file_path should be like `data/old_data_transformed/file`
    return file that represents all data : where all old data are stored
    """
    file = pathlib.Path(file_path)
    if not file.exists():
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()

    df = pd.read_parquet(file_path)




if __name__ == "__main__":
    # Test the function

    filename = "france_data_20250102-182912.csv"
    final_data_path = pathlib.Path("data/old_data_transformed/france_weather_old_data.parquet")
    print(transform_filename(filename))

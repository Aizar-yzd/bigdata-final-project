import pandas as pd
import os
import time
import logging
from sqlalchemy import create_engine
from config import DB_CONFIG

logging.basicConfig(
    filename="logs/elt_extract_load.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def extract_source_1():
    start_time = time.time()

    file_path = "raw/AB_US_2020.csv"
    df = pd.read_csv(file_path)

    file_size = os.path.getsize(file_path) / (1024 * 1024)

    logging.info(
        f"EXTRACT SOURCE 1 | File: AB_US_2020.csv | "
        f"Rows: {df.shape[0]} | Columns: {df.shape[1]} | "
        f"Size: {file_size:.2f} MB | "
        f"Time: {time.time() - start_time:.2f} sec"
    )

    return df

def extract_source_2():
    start_time = time.time()

    file_path = "raw/Airbnb_Open_Data.csv"
    df = pd.read_csv(file_path)

    file_size = os.path.getsize(file_path) / (1024 * 1024)

    logging.info(
        f"EXTRACT SOURCE 2 | File: Airbnb_Open_Data.csv | "
        f"Rows: {df.shape[0]} | Columns: {df.shape[1]} | "
        f"Size: {file_size:.2f} MB | "
        f"Time: {time.time() - start_time:.2f} sec"
    )

    return df

def get_engine():
    return create_engine(
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

def load_to_postgres(df, table_name):
    engine = get_engine()
    start_time = time.time()

    df.to_sql(
        name=table_name,
        schema="raw",
        con=engine,
        if_exists="replace",
        index=False
    )

    logging.info(
        f"LOAD SUCCESS | Table: raw.{table_name} | "
        f"Rows: {df.shape[0]} | "
        f"Time: {time.time() - start_time:.2f} sec"
    )

if __name__ == "__main__":
    df1 = extract_source_1()
    df2 = extract_source_2()

    load_to_postgres(df1, "ab_us_2020_raw")
    load_to_postgres(df2, "airbnb_open_data_raw")

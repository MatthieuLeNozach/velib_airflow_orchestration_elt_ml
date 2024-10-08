# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow import Dataset
import logging
import os
from minio import Minio
from pendulum import duration
from datetime import timedelta
import json


# ----------------------- #
# Configuration variables #
# ----------------------- #
MY_NAME = "Friend"
# MinIO connection config
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_IP = "host.docker.internal:9000"
ARCHIVE_BUCKET_NAME = "archive"

VELIB_BACKUP_BUCKET_NAME = "backup"
VELIB_RAW_BUCKET_NAME = "velib_raw"
VELIB_PROCESSED_BUCKET_NAME = "velib_processed"
ANALYTICS_BUCKET_NAME = 'analytics'
# Source file path climate data
TEMP_GLOBAL_PATH = f"{os.environ['AIRFLOW_HOME']}/include/climate_data/temp_global.csv"

# Datasets


DS_DUCKDB_IN_VELIB = Dataset("duckdb://in_velib")
DS_DUCKDB_REPORTING_VELIB = Dataset("duckdb://reporting_velib")

DS_START_VELIB = Dataset("start_velib")

# PostGreSQL config

# DuckDB config
DUCKDB_INSTANCE_NAME = json.loads(os.environ["AIRFLOW_CONN_DUCKDB_DEFAULT"])["host"]
CONN_ID_DUCKDB = "duckdb_default"

VELIB_GLOBAL_TABLE_NAME = "velib_global"
VELIB_REPORTING_TABLE_NAME = "velib_reporting"

# Pool
DUCKDB_POOL = "duckdb"

# get Airflow task logger
task_log = logging.getLogger("airflow.task")

# DAG default arguments
default_args = {
    "owner": MY_NAME,
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": duration(minutes=5),
    "execution_timeout": timedelta(minutes=180)  # Set execution timeout to 180 minutes
}


# utility functions
def get_minio_client():
    client = Minio(MINIO_IP, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    return client


STREAMLIT_COMMAND = "streamlit run streamlit_app.py --server.enableWebsocketCompression=false --server.enableCORS=false"





# ----------------------------------


# -------------------- #
# Enter your own info! #
# -------------------- #

MY_NAME = "Friend"
MY_CITY = "Portland"
WEATHER_BUCKET_NAME = "weather"
CLIMATE_BUCKET_NAME = "climate"

DS_START = Dataset("start")


DS_CLIMATE_DATA_MINIO = Dataset(f"minio://{CLIMATE_BUCKET_NAME}")
DS_WEATHER_DATA_MINIO = Dataset(f"minio://{WEATHER_BUCKET_NAME}")
DS_DUCKDB_IN_WEATHER = Dataset("duckdb://in_weather")
DS_DUCKDB_IN_CLIMATE = Dataset("duckdb://in_climate")
DS_DUCKDB_REPORTING = Dataset("duckdb://reporting")


WEATHER_IN_TABLE_NAME = "in_weather"
CLIMATE_TABLE_NAME = "temp_global_table"
REPORTING_TABLE_NAME = "reporting_table"


# default coordinates
default_coordinates = {"city": "No city provided", "lat": 0, "long": 0}


"""
# DAG default arguments
default_args = {
    "owner": MY_NAME,
    "city": MY_CITY,
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": duration(minutes=5),
    "execution_timeout": timedelta(minutes=180)  # Set execution timeout to 180 minutes
}
"""
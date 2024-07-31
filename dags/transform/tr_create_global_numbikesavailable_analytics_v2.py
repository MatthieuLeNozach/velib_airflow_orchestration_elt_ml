from airflow.decorators import dag, task
from pendulum import datetime
from include.global_variables import global_variables as gv
from include.custom_operators.minio import MinIODownloadOperator, MinIOUploadOperator, MinIOHook
from include.custom_operators.duckdb import DuckDBOperator
from include.sql_tools.loader import SQLLoader
import logging
import os

# Define the path for the DuckDB database file
DB_FILE_PATH = '/tmp/velib_database.db'

# Load SQL statements
sql_loader = SQLLoader('include/sql_transforms/global.sql')

@dag(
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=gv.default_args,
    description="Create Velib reporting table.",
    tags=["duckdb", "transform", "Astro SDK"],
)
def tr_create_global_numbikesavailable_analytics_v2():

    @task(pool="duckdb")
    def cleanup():
        if os.path.exists(DB_FILE_PATH):
            os.remove(DB_FILE_PATH)
            logging.info(f"Removed DuckDB database file: {DB_FILE_PATH}")
        else:
            logging.info(f"DuckDB database file not found: {DB_FILE_PATH}")

    download_merged_file = MinIODownloadOperator(
        task_id="download_merged_file",
        bucket_name=gv.ARCHIVE_BUCKET_NAME,
        object_name="merged_data.parquet",
        file_path="/tmp/merged_data.parquet",
        pool="duckdb"
    )

    load_parquet_to_duckdb = DuckDBOperator(
        task_id="load_parquet_to_duckdb",
        sql="""
        CREATE TABLE velib_global AS 
        SELECT * FROM read_parquet('/tmp/merged_data.parquet');
        """,
        database=DB_FILE_PATH,
        pool="duckdb"
    )

    create_stg_velib_global = DuckDBOperator(
        task_id="create_stg_velib_global",
        sql=sql_loader.get_statement('create_stg_velib_global'),
        database=DB_FILE_PATH,
        pool="duckdb"
    )

    create_int_velib_global = DuckDBOperator(
        task_id="create_int_velib_global",
        sql=sql_loader.get_statement('create_int_velib_global'),
        database=DB_FILE_PATH,
        pool="duckdb"
    )

    create_mart_global_numbikesavailable = DuckDBOperator(
        task_id="create_mart_global_numbikesavailable",
        sql=sql_loader.get_statement('create_mart_global_numbikesavailable'),
        database=DB_FILE_PATH,
        pool="duckdb"
    )

    export_to_parquet = DuckDBOperator(
        task_id="export_to_parquet",
        sql="""
        COPY (SELECT * FROM mart_global_numbikesavailable) 
        TO '/tmp/numbikesavailable.parquet' (FORMAT PARQUET)
        """,
        database=DB_FILE_PATH,
        pool="duckdb"
    )

    @task
    def create_analytics_bucket():
        minio_hook = MinIOHook()
        minio_hook.create_bucket(gv.ANALYTICS_BUCKET_NAME)
        return True

    create_bucket = create_analytics_bucket()

    upload_to_minio = MinIOUploadOperator(
        task_id="upload_to_minio",
        bucket_name=gv.ANALYTICS_BUCKET_NAME,
        object_name="analytics/global/numbikesavailable.parquet",
        file_path="/tmp/numbikesavailable.parquet",
        pool="duckdb"
    )

    # Task dependencies
    cleanup() >> download_merged_file >> load_parquet_to_duckdb >> create_stg_velib_global
    create_stg_velib_global >> create_int_velib_global >> create_mart_global_numbikesavailable
    create_mart_global_numbikesavailable >> export_to_parquet
    export_to_parquet >> create_bucket >> upload_to_minio >> cleanup()

tr_create_global_numbikesavailable_analytics_v2()
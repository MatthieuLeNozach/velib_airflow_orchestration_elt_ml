from airflow.decorators import dag, task
from pendulum import datetime
from include.global_variables import global_variables as gv
from include.custom_operators.minio import MinIODownloadOperator, MinIOUploadOperator, MinIOHook
from include.custom_operators.duckdb import DuckDBOperator
from include.sql_tools.loader import SQLLoader
import logging
import os

# Define constants
DB_FILE_PATH = '/tmp/velib_database.db'
PARQUET_FILE_PATH = '/tmp/merged_data.parquet'
EXPORT_PARQUET_PATH = '/tmp/numbikesavailable.parquet'

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
        file_path=PARQUET_FILE_PATH,
        pool="duckdb"
    )

    load_parquet_to_duckdb = DuckDBOperator(
        task_id="load_parquet_to_duckdb",
        sql=sql_loader.get_statement('load_parquet', target_table='velib_global', parquet_file_path=PARQUET_FILE_PATH),
        database=DB_FILE_PATH,
        pool="duckdb"
    )

    create_stg_velib_global = DuckDBOperator(
        task_id="create_stg_velib_global",
        sql=sql_loader.get_statement('create_table_from_select', target_table='stg_velib_global', select_statement=sql_loader.get_statement('stg_velib_global', source_table='velib_global')),
        database=DB_FILE_PATH,
        pool="duckdb"
    )

    create_int_velib_global = DuckDBOperator(
        task_id="create_int_velib_global",
        sql=sql_loader.get_statement('create_table_from_select', target_table='int_velib_global', select_statement=sql_loader.get_statement('int_velib_global', stg_table='stg_velib_global')),
        database=DB_FILE_PATH,
        pool="duckdb"
    )

    create_mart_global_numbikesavailable = DuckDBOperator(
        task_id="create_mart_global_numbikesavailable",
        sql=sql_loader.get_statement('create_table_from_select', target_table='mart_global_numbikesavailable', select_statement=sql_loader.get_statement('mart_global_numbikesavailable', int_table='int_velib_global')),
        database=DB_FILE_PATH,
        pool="duckdb"
    )

    @task(pool="duckdb")
    def log_row_count_before():
        import duckdb
        conn = duckdb.connect(DB_FILE_PATH)
        result = conn.execute("SELECT COUNT(*) FROM mart_global_numbikesavailable").fetchone()
        logging.info(f"Row count before removing outliers: {result[0]}")
        return result[0]

    remove_outliers = DuckDBOperator(
        task_id="remove_outliers",
        sql=sql_loader.get_statement('remove_outliers', table='mart_global_numbikesavailable', column='total_bikes'),
        database=DB_FILE_PATH,
        pool="duckdb"
    )

    @task(pool="duckdb")
    def log_row_count_after():
        import duckdb
        conn = duckdb.connect(DB_FILE_PATH)
        result = conn.execute("SELECT COUNT(*) FROM mart_global_numbikesavailable").fetchone()
        logging.info(f"Row count after removing outliers: {result[0]}")
        return result[0]

    export_to_parquet = DuckDBOperator(
        task_id="export_to_parquet",
        sql=f"""
        COPY (SELECT * FROM mart_global_numbikesavailable) 
        TO '{EXPORT_PARQUET_PATH}' (FORMAT PARQUET)
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
        file_path=EXPORT_PARQUET_PATH,
        pool="duckdb"
    )

    # Task dependencies
    cleanup() >> download_merged_file >> load_parquet_to_duckdb >> create_stg_velib_global
    create_stg_velib_global >> create_int_velib_global >> create_mart_global_numbikesavailable
    create_mart_global_numbikesavailable >> log_row_count_before() >> remove_outliers >> log_row_count_after() >> export_to_parquet
    export_to_parquet >> create_bucket >> upload_to_minio >> cleanup()

tr_create_global_numbikesavailable_analytics_v2()
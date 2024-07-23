from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.custom_operators.minio import MinIOUploadOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from include.global_variables import global_variables as gv
import duckdb
import os
import logging

@dag(
    schedule_interval="0 * * * *",  # Every hour
    start_date=days_ago(1),
    catchup=False,
    default_args=gv.default_args,
    description="Hourly backup of the stations table to MinIO with verification",
    tags=["backup", "minio", "postgres"],
)
def backup_stations():

    @task
    def extract_stations():
        """Extract data from stations table."""
        pg_hook = PostgresHook(postgres_conn_id='user_postgres')
        stations_query = "SELECT * FROM stations;"
        stations_df = pg_hook.get_pandas_df(stations_query)
        file_path = "/tmp/stations_backup.parquet"
        stations_df.to_parquet(file_path, index=False)
        return file_path

    @task
    def verify_backup(file_path):
        """Verify the backup by comparing row counts."""
        pg_hook = PostgresHook(postgres_conn_id='user_postgres')
        count_query = "SELECT COUNT(*) FROM stations;"
        pg_count = pg_hook.get_first(count_query)[0]

        con = duckdb.connect()
        parquet_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{file_path}')").fetchone()[0]

        # Log the row counts
        logging.info("\n\n")
        logging.info(f"PostgreSQL row count: {pg_count}")
        logging.info(f"Parquet file row count: {parquet_count}")
        logging.info("\n\n")

        if pg_count != parquet_count:
            raise ValueError(f"Row count mismatch: PostgreSQL({pg_count}) != Parquet({parquet_count})")
        return file_path

    def upload_to_minio(file_path):
        """Upload the file to MinIO."""
        upload_task = MinIOUploadOperator(
            task_id="upload_to_minio",
            bucket_name=gv.VELIB_BACKUP_BUCKET_NAME,
            object_name="stations_backup.parquet",
            file_path=file_path
        )
        upload_task.execute({})

    file_path = extract_stations()
    verified_file_path = verify_backup(file_path)

    upload_task = PythonOperator(
        task_id='upload_to_minio_task',
        python_callable=upload_to_minio,
        op_args=[verified_file_path]
    )

    verified_file_path >> upload_task

backup_stations()
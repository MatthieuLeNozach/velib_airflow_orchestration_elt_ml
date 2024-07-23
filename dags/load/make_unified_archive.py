from airflow.decorators import dag, task_group, task
from airflow.utils.dates import days_ago
from include.custom_operators.minio import MinIODownloadOperator, MinIOUploadOperator
import duckdb
import pandas as pd
import os
from airflow.operators.python import PythonOperator
from include.global_variables import global_variables as gv

@dag(
    schedule_interval="@daily",  # Adjust as needed
    start_date=days_ago(1),
    catchup=False,
    default_args=gv.default_args,
    description="Process backup files and create unified data file",
    tags=["process", "minio", "duckdb"],
)
def process_backup_files():

    @task_group
    def process_day():
        @task
        def download_files():
            """Download backup files from MinIO."""
            stations_file = MinIODownloadOperator(
                task_id="download_stations",
                bucket_name=gv.BACKUP_BUCKET_NAME,
                object_name="stations_backup.parquet",
                file_path="/tmp/stations_backup.parquet"
            )
            locations_file = MinIODownloadOperator(
                task_id="download_locations",
                bucket_name=gv.BACKUP_BUCKET_NAME,
                object_name="locations_backup.parquet",
                file_path="/tmp/locations_backup.parquet"
            )
            return stations_file, locations_file

        @task
        def transform_data(files):
            """Transform data using DuckDB."""
            stations_file, locations_file = files
            con = duckdb.connect()
            con.execute(f"CREATE TABLE stations AS SELECT * FROM read_parquet('{stations_file}')")
            con.execute(f"CREATE TABLE locations AS SELECT * FROM read_parquet('{locations_file}')")
            query = """
                SELECT * FROM stations
                JOIN locations ON stations.stationcode = locations.stationcode
                ORDER BY record_timestamp, stationcode
            """
            result_df = con.execute(query).df()
            file_path = "/tmp/velib_unified.parquet"
            result_df.to_parquet(file_path, index=False)
            return file_path

        def upload_to_minio(file_path):
            """Upload the file to MinIO."""
            upload_task = MinIOUploadOperator(
                task_id="upload_to_minio",
                bucket_name=gv.ARCHIVE_BUCKET_NAME,
                object_name="velib_unified.parquet",
                file_path=file_path
            )
            upload_task.execute({})

        files = download_files()
        file_path = transform_data(files)

        upload_task = PythonOperator(
            task_id='upload_to_minio_task',
            python_callable=upload_to_minio,
            op_args=[file_path]
        )

        file_path >> upload_task

    process_day()

process_backup_files()
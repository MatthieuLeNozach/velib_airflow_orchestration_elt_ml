from airflow.decorators import dag, task
from pendulum import datetime
import include.global_variables.global_variables as gv
from include.custom_operators.minio import MinIODownloadOperator, MinIOUploadOperator
import pandas as pd
import logging
import os
import duckdb

@dag(
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    description="Extract data from MinIO and write each day's data to its corresponding file",
    tags=["extract", "minio"],
    default_args=gv.default_args,
    max_active_tasks=1
)
def lo_archive_daily_files_from_backup_file():
    
    @task(pool='duckdb')
    def download_files():
        logger = logging.getLogger("airflow.task")
        logger.info("Downloading backup files from MinIO.")
        stations_file_path = "/tmp/stations_migration.parquet"
        locations_file_path = "/tmp/locations_backup.parquet"
        
        stations_file = MinIODownloadOperator(
            task_id="download_stations",
            bucket_name=gv.VELIB_BACKUP_BUCKET_NAME,
            object_name="stations_migration.parquet",
            file_path=stations_file_path
        )
        locations_file = MinIODownloadOperator(
            task_id="download_locations",
            bucket_name=gv.VELIB_BACKUP_BUCKET_NAME,
            object_name="locations_backup.parquet",
            file_path=locations_file_path
        )
        
        stations_file.execute(context={})
        locations_file.execute(context={})
        
        return stations_file_path, locations_file_path

    @task(pool='duckdb')
    def extract_and_write_files(files):
        logger = logging.getLogger("airflow.task")
        logger.info("Extracting data and writing each day's data to its corresponding file.")
        stations_file_path, locations_file_path = files
        
        logger.info("Checking if files exist.")
        if not os.path.exists(stations_file_path):
            raise FileNotFoundError(f"File not found: {stations_file_path}")
        
        if not os.path.exists(locations_file_path):
            raise FileNotFoundError(f"File not found: {locations_file_path}")
        
        logger.info("Creating a DuckDB connection.")
        con = duckdb.connect(database=':memory:')
        
        logger.info("Reading the data using DuckDB.")
        con.execute(f"CREATE TABLE stations AS SELECT * FROM read_parquet('{stations_file_path}')")
        con.execute(f"CREATE TABLE locations AS SELECT * FROM read_parquet('{locations_file_path}')")
        
        logger.info("Merging the data.")
        merged_query = """
        SELECT s.*, l.name, l.latitude, l.longitude
        FROM stations s
        LEFT JOIN locations l ON s.stationcode = l.stationcode
        """
        merged_df = con.execute(merged_query).fetchdf()
        
        logger.info("Ensuring the column names and types match the live data.")
        merged_df['record_timestamp'] = pd.to_datetime(merged_df['record_timestamp'], format='%Y-%m-%dT%H:%M:%S.%f%z')
        
        logger.info("Extracting unique dates.")
        unique_dates = merged_df['record_timestamp'].dt.date.unique()
        
        logger.info("Writing each day's data to its corresponding file.")
        for date in unique_dates:
            date_str = date.isoformat()
            daily_query = f"""
            SELECT *
            FROM merged_df
            WHERE DATE_TRUNC('day', record_timestamp) = DATE '{date_str}'
            """
            daily_df = con.execute(daily_query).fetchdf()
            daily_file_path = f"/tmp/{date}.parquet"
            daily_df.to_parquet(daily_file_path, index=False)
            
            logger.info(f"Determining year and week for the file path for date {date}.")
            year = date.year
            week = date.isocalendar()[1]
            object_name = f"archive/{year}/week_{week}/{date}.parquet"
            
            logger.info(f"Uploading {object_name} to MinIO.")
            upload_task = MinIOUploadOperator(
                task_id=f"upload_{date}",
                bucket_name=gv.ARCHIVE_BUCKET_NAME,
                object_name=object_name,
                file_path=daily_file_path
            )
            upload_task.execute(context={})
            logger.info(f"Uploaded {object_name} to MinIO")
        
        logger.info("Closing the DuckDB connection.")
        con.close()

    files = download_files()
    extract_and_write_files(files)

lo_archive_daily_files_from_backup_file()
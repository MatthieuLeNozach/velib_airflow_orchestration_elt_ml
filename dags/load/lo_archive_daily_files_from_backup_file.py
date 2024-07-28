from airflow.decorators import dag, task
from pendulum import datetime
import include.global_variables.global_variables as gv
from include.custom_operators.minio import MinIODownloadOperator, MinIOUploadOperator
import pandas as pd
import logging
from datetime import timedelta

@dag(
    schedule_interval="@once",  # Trigger manually or set a schedule
    start_date=datetime(2023, 1, 1),
    catchup=False,
    description="Extract data from MinIO and write each day's data to its corresponding file",
    tags=["extract", "minio"],
    default_args=gv.default_args
)
def lo_archive_daily_files_from_backup_file():
    
    @task
    def download_files():
        """Download backup files from MinIO."""
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

    @task
    def extract_and_write_files(files):
        """Extract data and write each day's data to its corresponding file."""
        logger = logging.getLogger("airflow.task")
        stations_file_path, locations_file_path = files
        
        # Read the data
        stations_df = pd.read_parquet(stations_file_path)
        locations_df = pd.read_parquet(locations_file_path)
        
        # Merge the data
        merged_df = pd.merge(stations_df, locations_df, on="stationcode")
        
        # Ensure the column names and types match the live data
        merged_df['record_timestamp'] = pd.to_datetime(merged_df['record_timestamp'], format='%Y-%m-%dT%H:%M:%S.%f%z')
        
        # Extract unique dates
        unique_dates = merged_df['record_timestamp'].dt.date.unique()
        
        # Write each day's data to its corresponding file
        for date in unique_dates:
            daily_df = merged_df[merged_df['record_timestamp'].dt.date == date]
            daily_file_path = f"/tmp/{date}.parquet"
            daily_df.to_parquet(daily_file_path, index=False)
            
            # Determine year and week for the file path
            year = date.year
            week = date.isocalendar()[1]
            object_name = f"archive/{year}/week_{week}/{date}.parquet"
            
            # Upload to MinIO
            upload_task = MinIOUploadOperator(
                task_id=f"upload_{date}",
                bucket_name=gv.ARCHIVE_BUCKET_NAME,
                object_name=object_name,
                file_path=daily_file_path
            )
            upload_task.execute(context={})
            logger.info(f"Uploaded {object_name} to MinIO")

    files = download_files()
    extract_and_write_files(files)

lo_archive_daily_files_from_backup_file()
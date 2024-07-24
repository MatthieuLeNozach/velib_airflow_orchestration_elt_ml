from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from include.custom_operators.minio import MinIODownloadOperator, MinIOUploadOperator, MinIOHook
import pandas as pd
import os
import logging
from include.global_variables import global_variables as gv

@dag(
    schedule_interval="@once",  # Trigger manually or set a schedule
    start_date=days_ago(1),
    catchup=False,
    description="Merge all parquet files in archive/archive and save as merged_data.parquet",
    tags=["merge", "minio"],
    default_args=gv.default_args
)
def merge_all_parquet_files():

    @task
    def list_parquet_files():
        """List all parquet files in the archive/archive directory."""
        logging.info("Starting to list parquet files.")
        minio_hook = MinIOHook()
        bucket_name = gv.ARCHIVE_BUCKET_NAME
        prefix = "archive/"
        objects = minio_hook.list_objects(bucket_name, prefix)
        
        # Log the objects to inspect their structure
        for obj in objects:
            logging.info(f"Object found: {obj}")
        
        parquet_files = [obj for obj in objects if obj.endswith('.parquet')]
        logging.info(f"Parquet files found: {parquet_files}")
        return parquet_files

    @task
    def download_parquet_files(parquet_files):
        """Download all listed parquet files."""
        logging.info("Starting to download parquet files.")
        local_paths = []
        for parquet_file in parquet_files:
            local_path = f"/tmp/{os.path.basename(parquet_file)}"
            logging.info(f"Downloading {parquet_file} to {local_path}.")
            download_task = MinIODownloadOperator(
                task_id=f"download_{os.path.basename(parquet_file)}",
                bucket_name=gv.ARCHIVE_BUCKET_NAME,
                object_name=parquet_file,
                file_path=local_path
            )
            download_task.execute(context={})
            local_paths.append(local_path)
        logging.info(f"Downloaded files: {local_paths}")
        return local_paths

    @task
    def merge_files(local_paths):
        """Merge all downloaded parquet files."""
        logging.info("Starting to merge parquet files.")
        if not local_paths:
            logging.error("No local paths provided for merging.")
            raise ValueError("No local paths provided for merging.")
        
        dataframes = [pd.read_parquet(path) for path in local_paths]
        merged_df = pd.concat(dataframes).sort_values(by='record_timestamp')
        merged_file_path = "/tmp/merged_data.parquet"
        merged_df.to_parquet(merged_file_path, index=False)
        logging.info(f"Merged file saved to {merged_file_path}.")
        return merged_file_path

    @task
    def upload_merged_file(merged_file_path):
        """Upload the merged parquet file to MinIO."""
        logging.info(f"Starting to upload merged file {merged_file_path} to MinIO.")
        upload_task = MinIOUploadOperator(
            task_id="upload_merged_file",
            bucket_name=gv.ARCHIVE_BUCKET_NAME,
            object_name="merged_data.parquet",
            file_path=merged_file_path
        )
        upload_task.execute(context={})
        logging.info(f"Uploaded merged_data.parquet to MinIO")

    parquet_files = list_parquet_files()
    local_paths = download_parquet_files(parquet_files)
    merged_file_path = merge_files(local_paths)
    upload_merged_file(merged_file_path)

merge_all_parquet_files()
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from include.custom_operators.minio import MinIODownloadOperator, MinIOUploadOperator, MinIOHook
import pandas as pd
import os
from include.global_variables import global_variables as gv
import logging

@dag(
    schedule_interval="@weekly",  # Run weekly
    start_date=days_ago(1),
    catchup=False,
    default_args=gv.default_args,
    description="Merge all parquet files in archive/archive and save as merged_data.parquet",
    tags=["merge", "minio"],
)
def merge_parquet_files():

    @task
    def list_parquet_files():
        """List all parquet files in the archive/archive directory."""
        minio_hook = MinIOHook()
        bucket_name = gv.ARCHIVE_BUCKET_NAME
        prefix = "archive/"
        objects = minio_hook.list_objects(bucket_name, prefix)
        parquet_files = [obj for obj in objects if obj.endswith('.parquet')]
        return parquet_files

    @task
    def download_parquet_files(parquet_files):
        """Download all listed parquet files."""
        local_paths = []
        for parquet_file in parquet_files:
            local_path = f"/tmp/{os.path.basename(parquet_file)}"
            download_task = MinIODownloadOperator(
                task_id=f"download_{os.path.basename(parquet_file)}",
                bucket_name=gv.ARCHIVE_BUCKET_NAME,
                object_name=parquet_file,
                file_path=local_path
            )
            download_task.execute(context={})
            local_paths.append(local_path)
        return local_paths

    @task
    def merge_files(local_paths):
        """Merge all downloaded parquet files."""
        dataframes = [pd.read_parquet(path) for path in local_paths]
        merged_df = pd.concat(dataframes).sort_values(by='record_timestamp')
        merged_file_path = "/tmp/merged_data.parquet"
        merged_df.to_parquet(merged_file_path, index=False)
        return merged_file_path

    @task
    def upload_merged_file(merged_file_path):
        """Upload the merged parquet file to MinIO."""
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

merge_parquet_files()
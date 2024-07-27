```py

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from include.custom_operators.minio import MinIODownloadOperator, MinIOUploadOperator, MinIOHook
import pandas as pd
import os
import logging
import shutil
import json
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
    def purge_temp_folder():
        """Purge the /tmp folder."""
        logging.info("Purging /tmp folder.")
        temp_folder = "/tmp"
        for filename in os.listdir(temp_folder):
            file_path = os.path.join(temp_folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                logging.error(f"Failed to delete {file_path}. Reason: {e}")

    @task(pool="duckdb")
    def download_index_file():
        """Download the index file from MinIO."""
        logging.info("Starting to download index file.")
        index_file_path = "/tmp/index.json"
        download_task = MinIODownloadOperator(
            task_id="download_index_file",
            bucket_name=gv.ARCHIVE_BUCKET_NAME,
            object_name="archive/index.json",
            file_path=index_file_path
        )
        download_task.execute(context={})
        logging.info(f"Downloaded index file to {index_file_path}.")
        return index_file_path

    @task(pool="duckdb")
    def list_parquet_files(index_file_path):
        """List all parquet files from the index file."""
        logging.info("Reading index file.")
        with open(index_file_path, 'r') as f:
            objects_list = json.load(f)
        
        parquet_files = [obj['object_name'] for obj in objects_list if obj['object_name'].endswith('.parquet')]
        logging.info(f"Parquet files found: {parquet_files}")
        return parquet_files

    @task(pool="duckdb")
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

    @task(pool="duckdb")
    def merge_files(local_paths):
        """Merge all downloaded parquet files using pandas."""
        logging.info("Starting to merge parquet files.")
        if not local_paths:
            logging.error("No local paths provided for merging.")
            raise ValueError("No local paths provided for merging.")
        
        merged_file_path = "/tmp/merged_data.parquet"
        first_file = True

        for local_path in local_paths:
            logging.info(f"Merging file {local_path}")
            df = pd.read_parquet(local_path)
            if first_file:
                df.to_parquet(merged_file_path, index=False)
                first_file = False
            else:
                df.to_parquet(merged_file_path, index=False, mode='a', append=True)
        
        logging.info(f"Merged file saved to {merged_file_path}.")
        return merged_file_path

    @task(pool="duckdb")
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

    # Define the DAG structure
    purge_start = purge_temp_folder()
    index_file_path = download_index_file()
    parquet_files = list_parquet_files(index_file_path)
    local_paths = download_parquet_files(parquet_files)
    merged_file_path = merge_files(local_paths)
    upload_merged_file_task = upload_merged_file(merged_file_path)
    purge_end = purge_temp_folder()

    # Set task dependencies
    purge_start >> index_file_path
    index_file_path >> parquet_files
    parquet_files >> local_paths
    local_paths >> merged_file_path
    merged_file_path >> upload_merged_file_task
    upload_merged_file_task >> purge_end

merge_all_parquet_files()

--------------------------------------------

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from include.custom_operators.minio import MinIODownloadOperator, MinIOUploadOperator, MinIOHook
import duckdb
import os
import logging
import shutil
import json
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
    def purge_temp_folder():
        """Purge the /tmp folder."""
        logging.info("Purging /tmp folder.")
        temp_folder = "/tmp"
        for filename in os.listdir(temp_folder):
            file_path = os.path.join(temp_folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                logging.error(f"Failed to delete {file_path}. Reason: {e}")

    @task(pool="duckdb")
    def download_index_file():
        """Download the index file from MinIO."""
        logging.info("Starting to download index file.")
        index_file_path = "/tmp/index.json"
        download_task = MinIODownloadOperator(
            task_id="download_index_file",
            bucket_name=gv.ARCHIVE_BUCKET_NAME,
            object_name="archive/index.json",
            file_path=index_file_path
        )
        download_task.execute(context={})
        logging.info(f"Downloaded index file to {index_file_path}.")
        return index_file_path

    @task(pool="duckdb")
    def list_parquet_files(index_file_path):
        """List all parquet files from the index file."""
        logging.info("Reading index file.")
        with open(index_file_path, 'r') as f:
            objects_list = json.load(f)
        
        parquet_files = [obj['object_name'] for obj in objects_list if obj['object_name'].endswith('.parquet')]
        logging.info(f"Parquet files found: {parquet_files}")
        return parquet_files

    @task(pool="duckdb")
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

    @task(pool="duckdb")
    def merge_files(local_paths):
        """Merge all downloaded parquet files using DuckDB."""
        logging.info("Starting to merge parquet files.")
        if not local_paths:
            logging.error("No local paths provided for merging.")
            raise ValueError("No local paths provided for merging.")
        
        # Initialize DuckDB connection
        con = duckdb.connect(database=':memory:')
        
        # Read and merge parquet files using DuckDB
        parquet_files_str = "', '".join(local_paths)
        query = f"""
        SELECT * FROM read_parquet(['{parquet_files_str}'])
        ORDER BY record_timestamp
        """
        merged_df = con.execute(query).fetchdf()
        merged_file_path = "/tmp/merged_data.parquet"
        merged_df.to_parquet(merged_file_path, index=False)
        logging.info(f"Merged file saved to {merged_file_path}.")
        
        # Close DuckDB connection
        con.close()
        
        return merged_file_path

    @task(pool="duckdb")
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

    # Define the DAG structure
    purge_start = purge_temp_folder()
    index_file_path = download_index_file()
    parquet_files = list_parquet_files(index_file_path)
    local_paths = download_parquet_files(parquet_files)
    merged_file_path = merge_files(local_paths)
    upload_merged_file_task = upload_merged_file(merged_file_path)
    purge_end = purge_temp_folder()

    # Set task dependencies
    purge_start >> index_file_path
    index_file_path >> parquet_files
    parquet_files >> local_paths
    local_paths >> merged_file_path
    merged_file_path >> upload_merged_file_task
    upload_merged_file_task >> purge_end

merge_all_parquet_files()





            # Log initial memory usage
            initial_memory_usage = con.execute("SELECT * FROM duckdb_memory();").fetchdf()
            logging.info(f"Initial memory usage: {initial_memory_usage}")

            # Log final memory usage
            final_memory_usage = con.execute("SELECT * FROM duckdb_memory();").fetchdf()
            logging.info(f"Final memory usage: {final_memory_usage}")
```
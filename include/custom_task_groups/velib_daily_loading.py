# include/custom_task_groups/velib_daily_loading.py

from airflow.decorators import task_group, task
from include.custom_operators.minio import MinIOUploadOperator
import pandas as pd
from datetime import datetime as dt

@task_group
def process_and_archive_data(data_json, target_date, bucket_name):
    @task
    def transform_data(data_json):
        df = pd.read_json(data_json)
        df['record_timestamp'] = pd.to_datetime(df['record_timestamp'], format='%Y-%m-%dT%H:%M:%S.%f%z')
        
        # Validate that all timestamps belong to the target_date
        if not all(df['record_timestamp'].dt.date == target_date):
            raise ValueError("Some records have timestamps that do not belong to the target date")
        
        parquet_data = df.to_parquet()
        return parquet_data.decode('ISO-8859-1')  # Convert bytes to string

    @task
    def upload_to_minio(parquet_data_str, target_date, bucket_name):
        parquet_data = parquet_data_str.encode('ISO-8859-1')  # Convert string back to bytes
        year = target_date.year
        week = target_date.isocalendar()[1]
        object_name = f"archive/{year}/week_{week}/{target_date}.parquet"
        
        # Save to a temporary file
        temp_file_path = f"/tmp/{target_date}.parquet"
        with open(temp_file_path, "wb") as f:
            f.write(parquet_data)
        
        # Upload to MinIO
        upload_task = MinIOUploadOperator(
            task_id="upload_to_minio",
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=temp_file_path
        )
        upload_task.execute(context={})
        print(f"Uploaded {object_name} to MinIO")

    parquet_data_str = transform_data(data_json)
    upload_to_minio(parquet_data_str, target_date, bucket_name)
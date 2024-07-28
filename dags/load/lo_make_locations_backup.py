from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.custom_operators.minio import MinIOUploadOperator
from airflow.operators.python import PythonOperator
from include.global_variables import global_variables as gv
import pandas as pd
import os

@dag(
    schedule_interval="@once",  # Run once
    start_date=days_ago(1),
    catchup=False,
    default_args=gv.default_args,
    description="One-time backup of the locations table to MinIO",
    tags=["backup", "minio", "postgres"],
)
def lo_make_locations_backup():

    @task
    def extract_locations():
        """Extract data from locations table."""
        pg_hook = PostgresHook(postgres_conn_id='user_postgres')
        locations_query = "SELECT * FROM locations;"
        locations_df = pg_hook.get_pandas_df(locations_query)
        file_path = "/tmp/locations_backup.parquet"
        locations_df.to_parquet(file_path, index=False)
        return file_path

    def upload_to_minio(file_path):
        """Upload the file to MinIO."""
        upload_task = MinIOUploadOperator(
            task_id="upload_to_minio",
            bucket_name=gv.VELIB_BACKUP_BUCKET_NAME,
            object_name="locations_backup.parquet",
            file_path=file_path
        )
        upload_task.execute({})

    file_path = extract_locations()

    upload_task = PythonOperator(
        task_id='upload_to_minio_task',
        python_callable=upload_to_minio,
        op_args=[file_path]
    )

    file_path >> upload_task

lo_make_locations_backup()
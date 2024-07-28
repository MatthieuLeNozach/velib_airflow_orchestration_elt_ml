from airflow.decorators import dag, task, task_group
from pendulum import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.custom_operators.minio import MinIOUploadOperator
from include.custom_operators.minio import MinIOHook
import include.global_variables.global_variables as gv
import duckdb
import pandas as pd
import logging
import os

@dag(
    schedule_interval="0 1 * * *",  # Run daily at 1 AM
    start_date=datetime(2023, 1, 1),
    catchup=False,
    description="Daily processing and archiving of stations and locations data",
    tags=["daily", "minio", "postgres"],
    default_args=gv.default_args
)
def lo_archive_past_days():
    @task
    def initial_cleanup():
        temp_files = ["/tmp/all_dates_data.parquet"]
        for temp_file in temp_files:
            if os.path.exists(temp_file):
                os.remove(temp_file)
                print(f"Deleted temporary file: {temp_file}")

    @task(pool='duckdb')
    def get_dates_to_process():
        pg_hook = PostgresHook(postgres_conn_id='user_postgres')
        minio_hook = MinIOHook()
        
        # Get all dates from Postgres
        query = """
        SELECT DISTINCT DATE(record_timestamp::timestamp) as record_date
        FROM stations
        """
        dates_df = pg_hook.get_pandas_df(query)
        all_dates = set(dates_df['record_date'].tolist())
        
        # Get all dates already archived in MinIO
        archived_dates = set(minio_hook.list_dates_in_bucket(gv.ARCHIVE_BUCKET_NAME))
        
        # Dates to process are those in Postgres but not in MinIO
        dates_to_process = all_dates - archived_dates
        return [date.isoformat() for date in dates_to_process]

    @task(pool='duckdb')
    def extract_data(dates_to_process):
        pg_hook = PostgresHook(postgres_conn_id='user_postgres')
        query = """
        SELECT s.*, l.name, l.latitude, l.longitude, DATE(s.record_timestamp::timestamp) as record_date
        FROM stations s
        JOIN locations l ON s.stationcode = l.stationcode
        WHERE DATE(s.record_timestamp::timestamp) = ANY(%s::date[])
        """
        df = pg_hook.get_pandas_df(query, parameters=(dates_to_process,))
        
        # Save to a temporary file
        temp_file_path = "/tmp/all_dates_data.parquet"
        df.to_parquet(temp_file_path, index=False)
        return temp_file_path

    @task_group
    def process_and_archive_data(temp_file_path, target_date, bucket_name):
        @task(pool='duckdb')
        def transform_data(temp_file_path, target_date):
            con = duckdb.connect(database=':memory:')
            query = f"""
            SELECT * FROM read_parquet('{temp_file_path}')
            WHERE record_date = '{target_date}'
            """
            df = con.execute(query).fetchdf()
            con.close()
            
            # Validate that all timestamps belong to the target_date
            df['record_timestamp'] = pd.to_datetime(df['record_timestamp'], format='%Y-%m-%dT%H:%M:%S.%f%z')
            target_date_dt = pd.to_datetime(target_date).date()
            mismatched_records = df[df['record_timestamp'].dt.date != target_date_dt]
            if not mismatched_records.empty:
                logging.error(f"Mismatched records: {mismatched_records}")
                raise ValueError("Some records have timestamps that do not belong to the target date")
            
            parquet_data = df.to_parquet()
            return parquet_data.decode('ISO-8859-1')  # Convert bytes to string

        @task(pool='duckdb')
        def upload_to_minio(parquet_data_str, target_date, bucket_name):
            parquet_data = parquet_data_str.encode('ISO-8859-1')  # Convert string back to bytes
            
            # Convert target_date to datetime
            target_date = pd.to_datetime(target_date).date()
            
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
            return temp_file_path

        @task
        def cleanup_temp_files(temp_file_path):
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
                print(f"Deleted temporary file: {temp_file_path}")

        parquet_data_str = transform_data(temp_file_path, target_date)
        temp_file_path = upload_to_minio(parquet_data_str, target_date, bucket_name)
        cleanup_temp_files(temp_file_path)

    initial_cleanup()
    dates_to_process = get_dates_to_process()
    temp_file_path = extract_data(dates_to_process)
    
    process_and_archive_data.partial(temp_file_path=temp_file_path, bucket_name=gv.ARCHIVE_BUCKET_NAME).expand(target_date=dates_to_process)

lo_archive_past_days()
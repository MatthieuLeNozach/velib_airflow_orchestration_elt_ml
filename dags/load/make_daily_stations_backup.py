from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime, duration
import pandas as pd
import io
from include.custom_operators.minio import MinIOUploadOperator, MinIOHook
from include.global_variables import global_variables as gv

@dag(
    schedule_interval="0 1 * * *",  # Run daily at 1 AM
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=gv.default_args,
    description="Daily backup of stations and locations data to MinIO",
    tags=["backup", "minio", "postgres"],
)
def daily_stations_backup():

    @task
    def extract_and_upload_data():
        # Connect to PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='user_postgres')
        
        # Get yesterday's date
        yesterday = datetime.utcnow().subtract(days=1).date()
        
        # Extract data
        query = f"""
        SELECT s.*, l.name, l.lat, l.lon
        FROM stations s
        JOIN locations l ON s.stationcode = l.stationcode
        WHERE DATE(s.record_timestamp::timestamp) = '{yesterday}'
        """
        df = pg_hook.get_pandas_df(query)
        
        # Convert to parquet
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer)
        parquet_buffer.seek(0)
        
        # Prepare the object name
        year = yesterday.year
        week = yesterday.isocalendar()[1]
        object_name = f"archive/{year}/week_{week}/{yesterday}.parquet"
        
        # Save to a temporary file
        temp_file_path = f"/tmp/{yesterday}.parquet"
        with open(temp_file_path, "wb") as f:
            f.write(parquet_buffer.getvalue())
        
        # Upload to MinIO
        upload_task = MinIOUploadOperator(
            task_id="upload_to_minio",
            bucket_name=gv.ARCHIVE_BUCKET_NAME,
            object_name=object_name,
            file_path=temp_file_path
        )
        upload_task.execute(context={})
        
        print(f"Uploaded {object_name} to MinIO")

    @task
    def cleanup_old_data():
        pg_hook = PostgresHook(postgres_conn_id='user_postgres')
        cleanup_query = """
        DELETE FROM stations
        WHERE record_timestamp::timestamp < NOW() - INTERVAL '7 days';
        """
        pg_hook.run(cleanup_query)
        print("Cleaned up data older than 7 days")

    extract_and_upload_data() >> cleanup_old_data()

daily_stations_backup()
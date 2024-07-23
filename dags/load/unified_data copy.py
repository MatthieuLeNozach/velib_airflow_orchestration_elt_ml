from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.global_variables import global_variables as gv
from include.custom_operators.minio import MinIOUploadOperator
import pandas as pd
import os

@dag(
    schedule_interval="0 */6 * * *",  # Every 6 hours
    start_date=days_ago(1),
    catchup=False,
    default_args=gv.default_args,
    description="Create a unified data file from locations and stations tables and upload to MinIO",
    tags=["load", "minio", "postgres"],
)
def unified_data():

    @task
    def extract_data():
        """Extract data from locations and stations tables."""
        pg_hook = PostgresHook(postgres_conn_id='user_postgres')
        locations_query = "SELECT * FROM locations;"
        stations_query = "SELECT * FROM stations;"
        
        locations_df = pg_hook.get_pandas_df(locations_query)
        stations_df = pg_hook.get_pandas_df(stations_query)
        
        # Convert DataFrames to JSON strings
        locations_json = locations_df.to_json(orient='split')
        stations_json = stations_df.to_json(orient='split')
        
        return locations_json, stations_json

    @task
    def transform_data(data):
        """Join locations and stations data on stationcode."""
        locations_json, stations_json = data
        
        # Convert JSON strings back to DataFrames
        locations_df = pd.read_json(locations_json, orient='split')
        stations_df = pd.read_json(stations_json, orient='split')
        
        unified_df = pd.merge(locations_df, stations_df, on="stationcode")
        file_path = "/tmp/unified_data.csv"
        unified_df.to_csv(file_path, index=False)
        return file_path

    data = extract_data()
    file_path = transform_data(data)

    upload_to_minio = MinIOUploadOperator(
        task_id="upload_to_minio",
        bucket_name=gv.ARCHIVE_BUCKET_NAME,
        object_name="unified_data.csv",
        file_path=file_path
    )

    # Define task dependencies
    file_path >> upload_to_minio

unified_data()
# dags/load/archive_past_days.py

from airflow.decorators import dag, task
from pendulum import datetime
from datetime import timedelta
from include.custom_task_groups.velib_daily_archive import process_and_archive_data
import include.global_variables.global_variables as gv
from include.custom_operators.minio import MinIOHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    schedule_interval="0 1 * * *",  # Run daily at 1 AM
    start_date=datetime(2023, 1, 1),
    catchup=False,
    description="Daily processing and archiving of stations and locations data",
    tags=["daily", "minio", "postgres"],
    default_args=gv.default_args
)
def archive_past_days():
    @task
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
        return [{"target_date": date.isoformat(), "bucket_name": gv.ARCHIVE_BUCKET_NAME} for date in dates_to_process]

    dates_to_process = get_dates_to_process()
    
    process_and_archive_data.expand_kwargs(dates_to_process)

archive_past_days()

# dags/load/archive_past_day.py

from airflow.decorators import dag, task
from pendulum import datetime
from datetime import timedelta
from include.custom_task_groups.velib_daily_loading import process_and_archive_data
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
def archive_past_day():
    @task
    def ensure_folders_exist_task(target_date):
        minio_hook = MinIOHook()
        minio_hook.create_folder_structure(gv.ARCHIVE_BUCKET_NAME, target_date)

    @task
    def extract_data(target_date):
        pg_hook = PostgresHook(postgres_conn_id='user_postgres')
        query = f"""
        SELECT s.*, l.name, l.latitude, l.longitude
        FROM stations s
        JOIN locations l ON s.stationcode = l.stationcode
        WHERE DATE(s.record_timestamp::timestamp) = '{target_date}'
        """
        df = pg_hook.get_pandas_df(query)
        return df.to_json()

    from datetime import datetime as dt
    target_date = dt.utcnow().date() - timedelta(days=1)

    ensure_folders_exist_task(target_date)
    data_json = extract_data(target_date)
    process_and_archive_data(data_json, target_date, gv.ARCHIVE_BUCKET_NAME)

archive_past_day()
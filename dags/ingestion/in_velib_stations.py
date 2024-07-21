from airflow.decorators import dag
from airflow.utils.dates import days_ago
from include.custom_task_groups.velib_data_ingestion import VelibDataIngestion
from datetime import timedelta

# Define the DAG
@dag(
    schedule_interval="*/2 * * * *",  # Every 2 minutes
    start_date=days_ago(1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="Fetch Velib data and store in PostgreSQL stations table",
    tags=["ingestion", "postgres"],
)
def in_velib_stations():

    # Define the task group for Velib data ingestion
    velib_data_ingestion_tg = VelibDataIngestion(
        task_id="velib_data_ingestion",
        api_url="https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&timezone=Europe/Paris&rows=2000",
        table_name="stations",
        check_query="SELECT 1 FROM stations WHERE record_timestamp = %s LIMIT 1",
        insert_query="""
        INSERT INTO stations (record_timestamp, stationcode, ebike, mechanical, duedate, numbikesavailable, numdocksavailable, capacity, is_renting, is_installed, is_returning)
        VALUES (%(record_timestamp)s, %(stationcode)s, %(ebike)s, %(mechanical)s, %(duedate)s, %(numbikesavailable)s, %(numdocksavailable)s, %(capacity)s, %(is_renting)s, %(is_installed)s, %(is_returning)s)
        ON CONFLICT (stationcode, record_timestamp) DO NOTHING;
        """,
        table_type="stations"
    )

    velib_data_ingestion_tg

# Instantiate the DAG
in_velib_stations()
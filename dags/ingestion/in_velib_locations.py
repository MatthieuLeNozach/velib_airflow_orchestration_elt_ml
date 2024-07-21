from airflow.decorators import dag
from airflow.utils.dates import days_ago
from include.custom_task_groups.velib_data_ingestion import VelibDataIngestion
from datetime import timedelta

# Define the DAG
@dag(
    schedule="@once",  # Run once
    start_date=days_ago(1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="Fetch Velib station data and store in PostgreSQL locations table",
    tags=["ingestion", "postgres"],
)
def in_velib_locations():

    # Define the task group for Velib data ingestion
    velib_data_ingestion_tg = VelibDataIngestion(
        task_id="velib_data_ingestion",
        api_url="https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&timezone=Europe/Paris&rows=2000",
        table_name="locations",
        check_query="SELECT COUNT(*) FROM locations",
        insert_query="""
        INSERT INTO locations (stationcode, name, latitude, longitude, nom_arrondissement_communes)
        VALUES (%(stationcode)s, %(name)s, %(latitude)s, %(longitude)s, %(nom_arrondissement_communes)s)
        ON CONFLICT (stationcode) DO NOTHING;
        """,
        table_type="locations"
    )

    velib_data_ingestion_tg

# Instantiate the DAG
in_velib_locations()
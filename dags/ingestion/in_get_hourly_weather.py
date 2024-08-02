from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime, now
from include.global_variables import global_variables as gv
from include.meterology_utils import get_weather_for_timestamp
import logging

@dag(
    schedule_interval="0 * * * *",  # Run every hour
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=gv.default_args,
    description="Hourly ingestion of weather data from Open-Meteo API",
    tags=["weather", "ingestion", "postgres"],
)
def in_get_hourly_weather():
    @task
    def fetch_and_store_weather(city: str, timestamp: datetime):
        weather_data = get_weather_for_timestamp(city, timestamp)
        pg_hook = PostgresHook(postgres_conn_id='user_postgres')
        insert_query = """
        INSERT INTO weather (record_timestamp, city, temperature, rain)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (record_timestamp) DO NOTHING;
        """
        pg_hook.run(insert_query, parameters=(timestamp, city, weather_data["temperature"], weather_data["rain"]))

    fetch_and_store_weather('Paris', now('UTC'))

in_get_hourly_weather()
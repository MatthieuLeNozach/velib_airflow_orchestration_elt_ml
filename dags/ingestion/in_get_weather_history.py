from airflow.decorators import dag, task
from pendulum import datetime as pendulum_datetime
from datetime import datetime
from include.global_variables import global_variables as gv
from include.meterology_utils import get_weather_for_timerange
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.custom_operators.minio import MinIODownloadOperator
import json
import logging

@dag(
    schedule_interval=None,
    start_date=pendulum_datetime(2023, 1, 1),
    catchup=False,
    default_args=gv.default_args,
    description="Ingest historical weather data from Open-Meteo API",
    tags=["weather", "ingestion", "postgres"],
)
def in_get_weather_history():
    logging.info("Starting in_get_weather_history DAG")

    download_index_file = MinIODownloadOperator(
        task_id="download_index_file",
        bucket_name=gv.ARCHIVE_BUCKET_NAME,
        object_name="archive/index.json",
        file_path="/tmp/index.json"
    )

    @task
    def create_weather_table_if_not_exists():
        pg_hook = PostgresHook(postgres_conn_id='user_postgres')
        create_table_query = """
        CREATE TABLE IF NOT EXISTS weather (
            record_timestamp TIMESTAMP PRIMARY KEY,
            city VARCHAR(255),
            temperature FLOAT,
            rain FLOAT
        );
        """
        pg_hook.run(create_table_query)
        logging.info("Created weather table if it didn't exist")

    @task
    def get_time_range_from_index(index_file_path: str):
        logging.info(f"Reading index file from {index_file_path}")
        with open(index_file_path, 'r') as f:
            index_data = json.load(f)
        
        parquet_files = [obj for obj in index_data if obj["object_name"].endswith('.parquet')]
        
        if not parquet_files:
            raise ValueError("No parquet files found in the index")
        
        dates = [datetime.strptime(obj["object_name"].split('/')[-1].split('.')[0], '%Y-%m-%d') for obj in parquet_files]
        
        oldest_date = min(dates)
        newest_date = max(dates)
        
        start_timestamp = oldest_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_timestamp = newest_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        logging.info(f"Time range: {start_timestamp} to {end_timestamp}")
        return start_timestamp.isoformat(), end_timestamp.isoformat()

    @task
    def fetch_and_store_historical_weather(city: str, time_range: tuple):
        start_timestamp, end_timestamp = [datetime.fromisoformat(ts) for ts in time_range]
        logging.info(f"Fetching weather data for {city} from {start_timestamp} to {end_timestamp}")
        
        weather_data = get_weather_for_timerange(city, start_timestamp, end_timestamp)
        logging.info(f"Retrieved weather data: {len(weather_data['time'])} records")
        
        pg_hook = PostgresHook(postgres_conn_id='user_postgres')
        insert_query = """
        INSERT INTO weather (record_timestamp, city, temperature, rain)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (record_timestamp) DO NOTHING;
        """
        
        inserted_rows = 0
        for i, timestamp in enumerate(weather_data["time"]):
            try:
                result = pg_hook.run(insert_query, parameters=(
                    timestamp, city, weather_data["temperature_2m"][i], weather_data["rain"][i]
                ))
                if result:
                    inserted_rows += 1
            except Exception as e:
                logging.error(f"Error inserting data for {timestamp}: {str(e)}")
        
        logging.info(f"Finished storing weather data. Inserted {inserted_rows} new rows.")

    create_table = create_weather_table_if_not_exists()
    time_range = get_time_range_from_index("/tmp/index.json")
    fetch_and_store_historical_weather_task = fetch_and_store_historical_weather('Paris', time_range)

    download_index_file >> create_table >> time_range >> fetch_and_store_historical_weather_task

    logging.info("DAG execution complete")

in_get_weather_history()
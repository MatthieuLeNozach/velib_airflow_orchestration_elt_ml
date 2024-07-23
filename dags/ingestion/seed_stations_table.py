from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
from include.global_variables import global_variables as gv
import logging
import os
import pandas as pd

@dag(
    start_date=datetime(2023, 1, 1),
    schedule="@once",  # Run once
    catchup=False,
    default_args=gv.default_args,
    description="Seed the stations table from a MinIO Parquet file and trigger the Velib DAG",
    tags=["seed", "minio", "postgres"],
)
def seed_stations_table():

    @task
    def download_parquet_file():
        """Download the Parquet file from MinIO."""
        logging.info("Starting download of Parquet file from MinIO.")
        minio_client = gv.get_minio_client()
        minio_client.fget_object(gv.VELIB_BACKUP_BUCKET_NAME, "stations_backup.parquet", "/tmp/stations_backup.parquet")
        logging.info("Parquet file downloaded successfully.")
        # Verify the Parquet file
        df = pd.read_parquet("/tmp/stations_backup.parquet")
        logging.info(f"Parquet file content (first 5 rows):\n{df.head()}")

    @task
    def drop_temp_table_if_exists():
        """Drop the temporary table if it exists."""
        logging.info("Dropping temporary table if it exists.")
        pg_hook = PostgresHook(postgres_conn_id='user_postgres')
        drop_temp_table_query = "DROP TABLE IF EXISTS temp_stations;"
        pg_hook.run(drop_temp_table_query)
        logging.info("Temporary table dropped if it existed.")

    @task
    def restore_to_temp_table():
        """Restore the Parquet file to a temporary table."""
        logging.info("Creating temporary table and restoring Parquet file.")
        pg_hook = PostgresHook(postgres_conn_id='user_postgres')
        create_temp_table_query = "CREATE TABLE temp_stations (LIKE stations INCLUDING ALL);"
        pg_hook.run(create_temp_table_query)
        logging.info("Temporary table created.")

        # Load the Parquet file into a DataFrame
        df = pd.read_parquet("/tmp/stations_backup.parquet")

        # Insert DataFrame into the temporary table with conflict handling in batches
        batch_size = 1000
        insert_query = """
        INSERT INTO temp_stations (record_timestamp, stationcode, ebike, mechanical, duedate, numbikesavailable, numdocksavailable, capacity, is_renting, is_installed, is_returning)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (record_timestamp, stationcode) DO NOTHING;
        """
        for start in range(0, len(df), batch_size):
            batch = df.iloc[start:start + batch_size]
            rows = [tuple(x) for x in batch.to_numpy()]
            pg_hook.run(insert_query, parameters=rows)
            logging.info(f"Inserted batch {start // batch_size + 1}")

    @task
    def validate_temp_table():
        """Ensure the temporary table is not empty or just consists of one row."""
        logging.info("Validating temporary table.")
        pg_hook = PostgresHook(postgres_conn_id='user_postgres')
        count_query = "SELECT COUNT(*) FROM temp_stations;"
        temp_count = pg_hook.get_first(count_query)[0]
        logging.info(f"Temporary table row count: {temp_count}")

        if temp_count <= 1:
            raise ValueError("Temporary table is empty or has only one row. Validation failed.")

    @task
    def merge_tables():
        """Merge the temporary table into the original stations table."""
        logging.info("Merging temporary table into the original stations table.")
        pg_hook = PostgresHook(postgres_conn_id='user_postgres')
        merge_query = """
        INSERT INTO stations (SELECT * FROM temp_stations WHERE NOT EXISTS (
            SELECT 1 FROM stations WHERE stations.timestamp = temp_stations.timestamp AND stations.stationcode = temp_stations.stationcode
        ));
        """
        pg_hook.run(merge_query)
        logging.info("Tables merged successfully.")

        # Count rows in the PostgreSQL table
        count_query = "SELECT COUNT(*) FROM stations;"
        pg_count = pg_hook.get_first(count_query)[0]
        logging.info(f"PostgreSQL row count after merge: {pg_count}")

    @task
    def drop_temp_table():
        """Drop the temporary table."""
        logging.info("Dropping temporary table.")
        pg_hook = PostgresHook(postgres_conn_id='user_postgres')
        drop_temp_table_query = "DROP TABLE temp_stations;"
        pg_hook.run(drop_temp_table_query)
        logging.info("Temporary table dropped.")

    trigger_velib_dag = TriggerDagRunOperator(
        task_id="trigger_velib_dag",
        trigger_dag_id="in_velib_stations"
    )

    download_parquet_file() >> drop_temp_table_if_exists() >> restore_to_temp_table() >> validate_temp_table() >> merge_tables() >> drop_temp_table() >> trigger_velib_dag

seed_stations_table()
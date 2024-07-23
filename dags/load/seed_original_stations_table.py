from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
from include.global_variables import global_variables as gv
import logging
import os
import duckdb
import shutil

@dag(
    start_date=datetime(2023, 1, 1),
    schedule="@once",  # Run once
    catchup=False,
    default_args=gv.default_args,
    description="Populate the original_db_stations table from a MinIO Parquet file",
    tags=["seed", "minio", "postgres"],
)
def populate_original_db_stations():

    @task(pool=gv.DUCKDB_POOL)
    def clean_temp_folder():
        logging.info("Cleaning the /tmp folder.")
        folder = '/tmp'
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                logging.error(f'Failed to delete {file_path}. Reason: {e}')
        logging.info("Temp folder cleaned.")

    @task(pool=gv.DUCKDB_POOL)
    def download_parquet_file():
        logging.info("Starting download of Parquet file from MinIO.")
        minio_client = gv.get_minio_client()
        minio_client.fget_object(gv.VELIB_BACKUP_BUCKET_NAME, "stations_backup.parquet", "/tmp/stations_backup.parquet")
        logging.info("Parquet file downloaded successfully.")
        return ["/tmp/stations_backup.parquet"]  # Return as a list to be JSON serializable

    @task(pool=gv.DUCKDB_POOL)
    def count_rows(parquet_file_path):
        con = duckdb.connect()
        row_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_file_path[0]}')").fetchone()[0]
        logging.info(f"Row count in Parquet file: {row_count}")
        return row_count

    @task(pool=gv.DUCKDB_POOL)
    def drop_and_create_stations_table():
        logging.info("Dropping and creating the stations table.")
        pg_hook = PostgresHook(postgres_conn_id='user_postgres')
        drop_table_query = "DROP TABLE IF EXISTS stations CASCADE;"
        create_table_query = """
        CREATE TABLE stations (
            id SERIAL PRIMARY KEY,
            record_timestamp VARCHAR(255),
            stationcode VARCHAR(255),
            ebike INTEGER,
            mechanical INTEGER,
            duedate VARCHAR(255),
            numbikesavailable INTEGER,
            numdocksavailable INTEGER,
            capacity INTEGER,
            is_renting VARCHAR(255),
            is_installed VARCHAR(255),
            is_returning VARCHAR(255),
            UNIQUE (record_timestamp, stationcode)
        );
        """
        pg_hook.run(drop_table_query)
        pg_hook.run(create_table_query)
        logging.info("Stations table dropped and created successfully.")

    @task(pool=gv.DUCKDB_POOL)
    def validate_original_db_stations(parquet_file_path, original_row_count):
        logging.info("Validating original_db_stations table.")
        pg_hook = PostgresHook(postgres_conn_id='user_postgres')
        count_query = "SELECT COUNT(*) FROM stations;"
        db_count = pg_hook.get_first(count_query)[0]
        
        logging.info(f"original_db_stations table row count: {db_count}")
        logging.info(f"Parquet file row count: {original_row_count}")

        if db_count != original_row_count:
            raise ValueError(f"Row count mismatch: original_db_stations({db_count}) != Parquet({original_row_count})")

    @task(pool=gv.DUCKDB_POOL)
    def create_offsets(original_row_count, batch_size):
        return list(range(0, original_row_count, batch_size))

    @task(pool=gv.DUCKDB_POOL)
    def insert_batch(parquet_file_path, offset, batch_size):
        logging.info(f"Inserting batch starting at offset {offset}")
        con = duckdb.connect()
        batch = con.execute(f"""
            SELECT 

            FROM read_parquet('{parquet_file_path[0]}') 
            LIMIT {batch_size} OFFSET {offset}
        """).df()

        pg_hook = PostgresHook(postgres_conn_id='user_postgres')

        # Check for missing stationcodes
        stationcodes = tuple(batch['stationcode'].unique())
        check_query = f"SELECT stationcode FROM locations WHERE stationcode IN {stationcodes}"
        existing_stationcodes = pg_hook.get_pandas_df(check_query)['stationcode'].tolist()
        missing_stationcodes = set(stationcodes) - set(existing_stationcodes)

        if missing_stationcodes:
            logging.error(f"Missing stationcodes in locations table: {missing_stationcodes}")
            raise ValueError(f"Missing stationcodes in locations table: {missing_stationcodes}")

        insert_query = """
        INSERT INTO stations (record_timestamp, stationcode, ebike, mechanical, duedate, numbikesavailable, numdocksavailable, capacity, is_renting, is_installed, is_returning)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (record_timestamp, stationcode) DO NOTHING;
        """
        
        rows = [
            (
                row['record_timestamp'],  # record_timestamp
                row['stationcode'],       # stationcode
                row['ebike'],             # ebike
                row['mechanical'],        # mechanical
                row['duedate'],           # duedate
                row['numbikesavailable'], # numbikesavailable
                row['numdocksavailable'], # numdocksavailable
                row['capacity'],          # capacity
                row['is_renting'],        # is_renting
                row['is_installed'],      # is_installed
                row['is_returning']       # is_returning
            )
            for _, row in batch.iterrows()
        ]

        try:
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany(insert_query, rows)
                    conn.commit()
            logging.info(f"Inserted batch starting at offset {offset}")
        except Exception as e:
            logging.error(f"Error inserting batch: {e}")
            raise

    def create_insert_batch_task_group(parquet_file_path, offsets, batch_size):
        with TaskGroup(group_id="insert_batches", prefix_group_id=False) as insert_batches:
            insert_batch.expand(
                parquet_file_path=[parquet_file_path],
                offset=offsets,
                batch_size=[batch_size]
            )
        return insert_batches

    clean_temp_folder_start = clean_temp_folder()
    drop_and_create_stations_table_task = drop_and_create_stations_table()
    parquet_file_path = download_parquet_file()
    original_row_count = count_rows(parquet_file_path)
    batch_size = 100000
    offsets = create_offsets(original_row_count, batch_size)

    insert_batches = create_insert_batch_task_group(parquet_file_path, offsets, batch_size)

    clean_temp_folder_start >> drop_and_create_stations_table_task >> parquet_file_path >> offsets >> insert_batches
    insert_batches >> validate_original_db_stations(parquet_file_path, original_row_count) >> clean_temp_folder()

populate_original_db_stations()
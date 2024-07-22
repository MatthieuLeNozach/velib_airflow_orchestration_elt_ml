from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime
from include.global_variables import global_variables as gv

@dag(
    start_date=datetime(2023, 1, 1),
    schedule="@once",  # Run once
    catchup=False,
    default_args=gv.default_args,
    description="Seed the stations table from a MinIO dump file and trigger the Velib DAG",
    tags=["seed", "minio", "postgres"],
)
def seed_stations_table():

    @task
    def download_dump_file():
        """Download the dump file from MinIO."""
        minio_client = gv.get_minio_client()
        minio_client.fget_object("backup", "stations_backup.dump", "/tmp/stations_backup.dump")
        # Verify the dump file
        with open("/tmp/stations_backup.dump", "rb") as file:
            content = file.read(500)  # Read the first 500 bytes for verification
            print("Dump file content (first 500 bytes):", content)

    seed_stations = BashOperator(
        task_id="seed_stations",
        bash_command="""
        PGPASSWORD=velib_password psql -h postgres_host -p postgres_port -U velib_user -d velib -f /tmp/stations_backup.dump > /tmp/psql_output.log 2>&1
        cat /tmp/psql_output.log
        """
    )

    trigger_velib_dag = TriggerDagRunOperator(
        task_id="trigger_velib_dag",
        trigger_dag_id="in_velib_stations"
    )

    download_dump_file() >> seed_stations >> trigger_velib_dag

seed_stations_table()
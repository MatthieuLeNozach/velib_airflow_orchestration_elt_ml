from airflow.decorators import dag
from pendulum import datetime
from include.custom_task_groups.list_archive_files import list_archive_files
from include.global_variables import global_variables as gv

@dag(
    schedule_interval="10 0 * * *",  # Run daily at 00:10 AM
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=gv.default_args,
    description="List all files in the archive bucket and create an index file",
    tags=["list", "minio"],
)
def lo_update_index_file():
    # Task group to list files and create an index file
    list_archive_files_task_group = list_archive_files()

lo_update_index_file()
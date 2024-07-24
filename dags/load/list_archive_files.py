from airflow.decorators import dag, task
from pendulum import datetime
from include.custom_operators.minio import MinIOHook
import include.global_variables.global_variables as gv
import logging

@dag(
    schedule_interval="@once",  # Trigger manually or set a schedule
    start_date=datetime(2023, 1, 1),
    catchup=False,
    description="List all files in the archive bucket",
    tags=["list", "minio"],
    default_args=gv.default_args
)
def list_archive_files():
    
    @task
    def list_files():
        """List all files in the archive bucket."""
        logging.info("Starting to list files in the archive bucket.")
        minio_hook = MinIOHook()
        bucket_name = gv.ARCHIVE_BUCKET_NAME
        prefix = "archive/"
        objects = minio_hook.list_objects(bucket_name, prefix)
        
        # Extract relevant information from the objects
        objects_list = [{"object_name": obj.object_name, "size": obj.size, "last_modified": obj.last_modified} for obj in objects]
        
        # Log the objects to inspect their structure
        for obj in objects_list:
            logging.info(f"Object found: {obj}")
        
        return objects_list

    files = list_files()
    logging.info(f"Files in archive bucket: {files}")

list_archive_files()
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import duckdb
import logging

class DuckDBOperator(BaseOperator):
    @apply_defaults
    def __init__(self, sql: str, database: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.database = database

    def execute(self, context):
        logging.info(f"Executing SQL: {self.sql}")
        con = duckdb.connect(database=self.database)
        con.execute(self.sql)
        con.close()
        logging.info("SQL execution completed.")
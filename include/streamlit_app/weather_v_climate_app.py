import streamlit as st
import duckdb
import pandas as pd
import os
import altair as alt
from include.global_variables import global_variables as gv
from include.sql_tools.loader import SQLLoader

# Get the Parquet file path from environment variables
parquet_file_path = os.environ.get("parquet_file_path", "analytics/global/numbikesavailable.parquet")

# MinIO client configuration using the utility function
minio_client = gv.get_minio_client()

# Local path to save the downloaded Parquet file
local_parquet_file_path = "/tmp/numbikesavailable.parquet"

# Download the Parquet file from MinIO
def download_parquet_file(bucket_name, object_name, local_file_path):
    minio_client.fget_object(bucket_name, object_name, local_file_path)

# Load SQL statements
sql_loader = SQLLoader('include/sql_transforms/global.sql')

# Query the database
def get_data(parquet_file_path):
    conn = duckdb.connect(database=':memory:')
    sql_query = sql_loader.get_statement('create_mart_global_numbikesavailable')
    df = conn.execute(sql_query).fetchdf()
    conn.close()
    return df

# Download the Parquet file
download_parquet_file(gv.ANALYTICS_BUCKET_NAME, parquet_file_path, local_parquet_file_path)

# Get the data
data = get_data(local_parquet_file_path)

# Convert record_timestamp to datetime
data['record_timestamp'] = pd.to_datetime(data['record_timestamp'])

# Get the min and max dates from the data
min_date = data['record_timestamp'].min()
max_date = data['record_timestamp'].max()

# Add date input widgets for user to select date range
st.sidebar.title("Filter Options")
start_date = st.sidebar.date_input("Start date", min_date, min_value=min_date, max_value=max_date)
end_date = st.sidebar.date_input("End date", max_date, min_value=min_date, max_value=max_date)

# Filter data based on user input
filtered_data = data[(data['record_timestamp'] >= pd.to_datetime(start_date)) & (data['record_timestamp'] <= pd.to_datetime(end_date))]

# Plot the data
st.title("Global Number of Bikes Available")
chart = alt.Chart(filtered_data).mark_line().encode(
    x='record_timestamp:T',
    y='total_bikes:Q'
)
st.altair_chart(chart, use_container_width=True)
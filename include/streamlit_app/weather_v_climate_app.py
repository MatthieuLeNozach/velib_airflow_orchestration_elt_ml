import sys
import os

# Add the /include directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import streamlit as st
import duckdb
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

# Load SQL statements using an absolute path
sql_file_path = os.path.join(os.path.dirname(__file__), '../sql_transforms/global.sql')
sql_loader = SQLLoader(sql_file_path)

# Query the database
def get_data(parquet_file_path, start_date, end_date, granularity):
    conn = duckdb.connect(database=':memory:')
    
    # Load the Parquet file into DuckDB
    conn.execute(f"CREATE TABLE velib_global AS SELECT * FROM read_parquet('{parquet_file_path}');")
    
    # Apply date filtering and granularity
    sql_query = sql_loader.get_statement('filtered_mart_global_numbikesavailable').format(
        int_table='velib_global',
        start_date=start_date,
        end_date=end_date,
        granularity=granularity
    )
    
    df = conn.execute(sql_query).fetchdf()
    conn.close()
    return df

# Download the Parquet file
download_parquet_file(gv.ANALYTICS_BUCKET_NAME, parquet_file_path, local_parquet_file_path)

# Sidebar for navigation
st.sidebar.title("Navigation")
tabs = ["Global"]
selected_tab = st.sidebar.radio("Select a tab", tabs)

if selected_tab == "Global":
    st.sidebar.title("Filter Options")
    
    # Get the data to determine min and max dates
    conn = duckdb.connect(database=':memory:')
    conn.execute(f"CREATE TABLE velib_global AS SELECT * FROM read_parquet('{local_parquet_file_path}');")
    min_date = conn.execute("SELECT MIN(record_timestamp) FROM velib_global;").fetchone()[0]
    max_date = conn.execute("SELECT MAX(record_timestamp) FROM velib_global;").fetchone()[0]
    conn.close()
    
    start_date = st.sidebar.date_input("Start date", min_date, min_value=min_date, max_value=max_date)
    end_date = st.sidebar.date_input("End date", max_date, min_value=min_date, max_value=max_date)
    granularity = st.sidebar.selectbox("Granularity", ["minute", "hour", "day"], index=1)
    
    if st.sidebar.button("Launch Visualization"):
        # Convert start_date and end_date to string format
        start_date = start_date.strftime('%Y-%m-%d')
        end_date = end_date.strftime('%Y-%m-%d')
        
        # Get the filtered data
        filtered_data = get_data(local_parquet_file_path, start_date, end_date, granularity)
        
        # Plot the data
        st.title("Global Number of Bikes Available")
        chart = alt.Chart(filtered_data).mark_line().encode(
            x='record_timestamp:T',
            y='total_bikes:Q'
        )
        st.altair_chart(chart, use_container_width=True)
import sys
import os
import logging

# Add the /include directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import streamlit as st
import duckdb
import altair as alt
from include.global_variables import global_variables as gv
from include.sql_tools.loader import SQLLoader

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get the Parquet file path from environment variables
parquet_file_path = os.environ.get("parquet_file_path", "analytics/global/numbikesavailable.parquet")

# MinIO client configuration using the utility function
minio_client = gv.get_minio_client()

# Local path to save the downloaded Parquet file
local_parquet_file_path = "/tmp/numbikesavailable.parquet"

# Download the Parquet file from MinIO
def download_parquet_file(bucket_name, object_name, local_file_path):
    logger.info(f"Downloading Parquet file from b<ucket: {bucket_name}, object: {object_name}")
    minio_client.fget_object(bucket_name, object_name, local_file_path)
    logger.info(f"Parquet file downloaded to: {local_file_path}")

# Load SQL statements using an absolute path
sql_file_path = os.path.join(os.path.dirname(__file__), '../sql_transforms/global.sql')
sql_loader = SQLLoader(sql_file_path)


def get_data(parquet_file_path, start_date, end_date, granularity, remove_outliers):
    conn = duckdb.connect(database=':memory:')
    
    conn.execute(f"CREATE TABLE velib_global AS SELECT * FROM read_parquet('{parquet_file_path}');")
    
    sql_query = sql_loader.get_statement('filtered_mart_global_numbikesavailable', 
                                         int_table='velib_global',
                                         start_date=start_date,
                                         end_date=end_date,
                                         granularity=granularity)
    
    logging.info(f"Initial SQL query: {sql_query}")
    
    if remove_outliers:
        remove_outliers_query = sql_loader.get_statement('remove_outliers', table=f'({sql_query})')
        sql_query = remove_outliers_query
        logging.info(f"SQL query with outliers removed: {sql_query}")
    
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
    st.title("Global")
    st.subheader("Total Bikes Aggregation")

    # Create two columns for input parameters and plot
    col1, col2 = st.columns([1, 2])

    with col1:
        st.subheader("Input Parameters")
        
        # Get the data to determine min and max dates
        logger.info(f"Connecting to DuckDB in-memory database to determine min and max dates")
        conn = duckdb.connect(database=':memory:')
        conn.execute(f"CREATE TABLE velib_global AS SELECT * FROM read_parquet('{local_parquet_file_path}');")
        min_date = conn.execute("SELECT MIN(record_timestamp) FROM velib_global;").fetchone()[0]
        max_date = conn.execute("SELECT MAX(record_timestamp) FROM velib_global;").fetchone()[0]
        conn.close()
        logger.info(f"Min date: {min_date}, Max date: {max_date}")
        
        start_date = st.date_input("Start date", min_date, min_value=min_date, max_value=max_date)
        end_date = st.date_input("End date", max_date, min_value=min_date, max_value=max_date)
        granularity = st.selectbox("Granularity", ["10min", "15min", "30min", "60min"], index=3)
        remove_outliers = st.radio("Remove outliers", ["Yes", "No"], index=1) == "Yes"
        
        if st.button("Launch Visualization"):
            # Convert start_date and end_date to string format
            start_date = start_date.strftime('%Y-%m-%d')
            end_date = end_date.strftime('%Y-%m-%d')
            
            logger.info(f"Fetching data with start_date: {start_date}, end_date: {end_date}, granularity: {granularity}, remove_outliers: {remove_outliers}")
            
            # Get the filtered data
            filtered_data = get_data(local_parquet_file_path, start_date, end_date, granularity, remove_outliers)
            
            with col2:
                st.subheader("Visualization")
                # Plot the data
                chart = alt.Chart(filtered_data).mark_line().encode(
                    x='record_timestamp:T',
                    y='total_bikes:Q'
                )
                st.altair_chart(chart, use_container_width=True)
                logger.info(f"Visualization launched successfully")
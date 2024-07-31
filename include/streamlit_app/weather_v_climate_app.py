# include/streamlit_app/weather_v_climate_app.py

import streamlit as st
import duckdb
import pandas as pd
import os
import altair as alt

# Get the Parquet file path from environment variables
parquet_file_path = os.environ.get("parquet_file_path", "analytics/global/numbikesavailable.parquet")

# Query the database
def get_data(parquet_file_path):
    conn = duckdb.connect(database=':memory:')
    query = f"""
        SELECT record_timestamp, total
        FROM read_parquet('{parquet_file_path}')
    """
    df = conn.execute(query).fetchdf()
    conn.close()
    return df

# Get the data
data = get_data(parquet_file_path)

# Plot the data
st.title("Global Number of Bikes Available")
chart = alt.Chart(data).mark_line().encode(
    x='record_timestamp:T',
    y='total:Q'
)
st.altair_chart(chart, use_container_width=True)
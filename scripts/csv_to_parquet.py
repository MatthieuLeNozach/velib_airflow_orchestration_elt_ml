import pandas as pd
import sys
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_stationcode(stationcode):
    """Remove non-numeric characters from stationcode and return as string."""
    return ''.join(filter(str.isdigit, str(stationcode)))

def csv_to_parquet(csv_file_path):
    # Check if the file exists
    if not os.path.isfile(csv_file_path):
        logging.error(f"File {csv_file_path} does not exist.")
        return

    # Read the CSV file
    try:
        logging.info(f"Reading CSV file: {csv_file_path}")
        df = pd.read_csv(csv_file_path, dtype={'stationcode': str})
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
        return

    # Clean the stationcode column
    if 'stationcode' in df.columns:
        logging.info("Cleaning stationcode column")
        df['stationcode'] = df['stationcode'].apply(clean_stationcode)
    else:
        logging.warning("stationcode column not found in the CSV file")

    # Define the Parquet file path
    parquet_file_path = os.path.splitext(csv_file_path)[0] + '.parquet'

    # Convert to Parquet
    try:
        logging.info(f"Converting to Parquet: {parquet_file_path}")
        df.to_parquet(parquet_file_path, index=False)
        logging.info(f"Successfully converted {csv_file_path} to {parquet_file_path}")
    except Exception as e:
        logging.error(f"Error converting to Parquet: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logging.error("Usage: python csv_to_parquet.py <csv_file_path>")
    else:
        csv_file_path = sys.argv[1]
        csv_to_parquet(csv_file_path)
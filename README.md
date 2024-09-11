Overview
========

This Airflow pipeline will:
- Ingest data from the Vélib API a public API into a Postgres DB (1460+ new rows rows every ~2mn, 1 row per station per api call)
- Load and transform the data in `parquet` files, stored in a datalake-like set of [MinIO](https://min.io/) buckets.
- Perform transforms with DuckDB engine, and write the refined data into parquet files 


-------------------------------

How to use this repository
==========================

## Setting up


1. Run `git clone https://github.com/MatthieuLeNozach/velib_airflow_orchestration_elt_ml.git`.
2. Install the Astro CLI by following the steps in the [Astro CLI documentation](https://docs.astronomer.io/astro/cli/install-cli). The main prerequisite is Docker Desktop/Docker Engine.
3. (Optional) Install python dependencies (`poetry install`) and activate virtual env (`poetry shell`)
4. Run `astro dev start` in your cloned repository.
5. After your Astro project has started. View the Airflow UI at `localhost:8080`. Thanks to `Astro`, login credentials are logged in the terminal after a successful startup. (default should be admin / admin)

![diagram](/assets/readme/startu.png)


## Run the project


1. Register the postgres database in airflow: on the top, go to `Admin/Connections` and fill the info (user/password in the 'description' field)

![diagram](/assets/readme/db_connection.png)


2. Run `start` DAG to create a single-thread execution pool for DuckDB
3. Unpause `in_*` DAGs to fetch data from Velib API / weather API and store it in the PostgreSQL DB
4. Unpause `lo_*` DAGs to load the data from PostgreSQL DB and populate the data lake with daily parquet files 
5. Unpause `tr_*` DAGS to perform transformations on aggregated parquet files and store the refined datasets in `ml` and `report` buckets 



-------------------------------

How it works
============

## Components and infrastructure


5 Docker containers will be created and relevant ports will be forwarded:

- The Airflow scheduler
- The Airflow webserver
- The Airflow metastore
- The Airflow triggerer
- THe Airflow artifact store (`postgres`)
- A Postgres instance for API data ingestion (`user-postgres`)
- A PgAdmin instance to access the user-postgres DB
- A MinIO instance as data lake
- A MLFlow instance to track ML experiments

## Astro's Airflow distribution


Supporting files are located in the `include` folder with the `include/global_variables/global_variables.py` containing the variables names and configurations.


## The DAGs

### Overview

![diagram](/assets/readme/ovw.png)  

  


#### An ELT Pipeline:   
![diagram](/assets/readme/velib.drawio.png)

### Ingest

Schema of the data retrieved from Weather and Vélib APIs
![db](/assets/readme/velib_postgres.png)


#### in_velib_stations / in_velib_locations
These DAGs are responsible for data ingestion:
- Collect data about bike-sharing stations and their locations from external sources.
- 1460+ Vélib stations
- 1 api call every 2mn
- Querying the Postgres DB on millions of rows has proven tricky, hence the need to choose a file based storage, see under

### Load

#### lo_make_daily_stations_backup
This DAG creates the daily parquet files:
- Creates daily backups of the station data.
- The archive bucket has a file structure `year/week/dayoftheweek.parquet`


#### lo_merge_all_parquet_files
- Merges all available daily files into a single dataset.
- DuckDB helps with **larger-than-memory** data manipulations
- The merged data is likely used as input for the machine learning models defined in `project/inference/ml_models/schemas.py`, such as the TemperatureModel.

### Transform

Transforms are applied via SQL macros via the DuckDB engine ([agg examples for the global dataset](/include/sql_transforms/global.sql))

#### tr_create_global_numbikesavailable_analytics_v2
Generates analytics on bike availability:
- Process the consolidated data to create insights on the number of bikes available across the network.

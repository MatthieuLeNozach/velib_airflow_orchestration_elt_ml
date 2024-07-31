-- name: load_parquet
CREATE TABLE {{ target_table }} AS 
SELECT * FROM read_parquet('{{ parquet_file_path }}');

-- name: create_table_from_select
CREATE TABLE {{ target_table }} AS 
{{ select_statement }};

-- name: stg_velib_global
SELECT record_timestamp, stationcode, numbikesavailable, mechanical, ebike
FROM {{ source_table }};

-- name: int_velib_global
SELECT record_timestamp, stationcode, SUM(numbikesavailable) as total_bikes
FROM {{ stg_table }}
GROUP BY record_timestamp, stationcode;

-- name: mart_global_numbikesavailable
SELECT record_timestamp, total_bikes
FROM {{ int_table }};
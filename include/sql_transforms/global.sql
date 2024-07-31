-- name: load_parquet
CREATE TABLE {{ target_table }} AS 
SELECT record_timestamp, stationcode, numbikesavailable 
FROM read_parquet('{{ parquet_file_path }}');

-- name: create_table_from_select
CREATE TABLE {{ target_table }} AS 
{{ select_statement }};

-- name: stg_velib_global
SELECT record_timestamp, stationcode, numbikesavailable
FROM {{ source_table }};

-- name: int_velib_global
SELECT record_timestamp, SUM(numbikesavailable) as total_bikes
FROM {{ stg_table }}
GROUP BY record_timestamp;

-- name: mart_global_numbikesavailable
SELECT record_timestamp, total_bikes
FROM {{ int_table }};

-- name: filtered_mart_global_numbikesavailable
SELECT
    CASE
        WHEN '{{ granularity }}' = '10min' THEN DATE_TRUNC('minute', record_timestamp) + (INTERVAL '10 minutes' * (CAST(FLOOR(EXTRACT(EPOCH FROM record_timestamp) / 600) AS INTEGER)))
        WHEN '{{ granularity }}' = '15min' THEN DATE_TRUNC('minute', record_timestamp) + (INTERVAL '15 minutes' * (CAST(FLOOR(EXTRACT(EPOCH FROM record_timestamp) / 900) AS INTEGER)))
        WHEN '{{ granularity }}' = '30min' THEN DATE_TRUNC('minute', record_timestamp) + (INTERVAL '30 minutes' * (CAST(FLOOR(EXTRACT(EPOCH FROM record_timestamp) / 1800) AS INTEGER)))
        WHEN '{{ granularity }}' = '60min' THEN DATE_TRUNC('minute', record_timestamp) + (INTERVAL '60 minutes' * (CAST(FLOOR(EXTRACT(EPOCH FROM record_timestamp) / 3600) AS INTEGER)))
        ELSE DATE_TRUNC('minute', record_timestamp)
    END AS record_timestamp,
    SUM(total_bikes) AS total_bikes
FROM {{ int_table }}
WHERE record_timestamp BETWEEN '{{ start_date }}' AND '{{ end_date }}'
GROUP BY 1
ORDER BY 1;

-- name: remove_outliers
WITH stats AS (
    SELECT 
        PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY {{ column }}) AS Q1,
        PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY {{ column }}) AS Q3
    FROM {{ table }}
)
SELECT *
FROM {{ table }}
WHERE {{ column }} BETWEEN 
    (SELECT Q1 - 1.5 * (Q3 - Q1) FROM stats) AND 
    (SELECT Q3 + 1.5 * (Q3 - Q1) FROM stats);
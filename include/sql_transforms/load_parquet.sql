-- name: load_parquet
CREATE TABLE {{ target_table }} AS 
SELECT * FROM read_parquet('{{ parquet_file_path }}');
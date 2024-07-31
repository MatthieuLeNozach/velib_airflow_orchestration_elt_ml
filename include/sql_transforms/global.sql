-- name: create_stg_velib_global
CREATE TABLE stg_velib_global AS
SELECT record_timestamp, stationcode, numbikesavailable, mechanical, ebike
FROM velib_global;

-- name: create_int_velib_global
CREATE TABLE int_velib_global AS
SELECT record_timestamp, stationcode, SUM(numbikesavailable) as total_bikes
FROM stg_velib_global
GROUP BY record_timestamp, stationcode;

-- name: create_mart_global_numbikesavailable
CREATE TABLE mart_global_numbikesavailable AS
SELECT record_timestamp, total_bikes
FROM int_velib_global;
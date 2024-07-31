# /include/sql_transforms/global.py


class StagingTransforms:
    create_stg_velib_global = """
    CREATE TABLE stg_velib_global AS
    SELECT record_timestamp, stationcode, numbikesavailable, mechanical, ebike
    FROM velib_global
    """

class IntermediateTransforms:
    create_int_total_bikes_global = """
    CREATE TABLE int_total_bikes_global AS
    SELECT stationcode, SUM(numbikesavailable) as total_bikes
    FROM stg_velib_global
    GROUP BY stationcode
    """

class MartTransforms:
    create_mart_global_numbikesavailable = """
    CREATE TABLE mart_global_numbikesavailable AS
    SELECT stationcode, total_bikes
    FROM int_total_bikes_global
    """
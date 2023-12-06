from src.spark.spark_utils import start_spark, get_spark_logger, get_folder_content
from pyspark import SparkConf, SparkContext
from os import getenv
from delta import DeltaTable
import subprocess


RAW_PATH = getenv("RAW_PATH")
CREATE_DB = "create database if not exists {db_name}"
DROP_DB = "drop database if exists {db_name} cascade"
CREATE_EXTERNAL_TABLE = """
create external table if not exists {table_name} (
    {dtypes}
)
using delta
{partitions}
location '{loc}'
"""
CREATE_TABLE = """
create table if not exists {table_name}
using delta
location '{loc}'
"""

DROP_TABLE = "drop table if exists {table_name}"

CONF = [
    ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
    ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
    ("spark.sql.legacy.createHiveTableByDefault.enabled", "true"),
    ('hive.exec.dynamic.partition', 'true'),
    ('hive.exec.max.dynamic.partitions', '2048'),
    ('hive.exec.dynamic.partition.mode', 'nonstrict'),
]


def main():
    conf = CONF  # SparkConf().setAll(CONF)

    with start_spark(
        app_name='hive_tables',
        config=conf,
    ) as spark:
        logger = get_spark_logger(spark)

        databases = get_folder_content(spark, RAW_PATH)
        # databases = [i for i in databases if i == "account"]
        print('databases = ', databases)

        for db in databases:
            create_db_sql = CREATE_DB.format(db_name=f"raw_{db}")
            print(create_db_sql)
            # spark.sql(create_db_sql)
            # drop_db_sql = DROP_DB.format(db_name=f"raw_{db}")
            # print(drop_db_sql)
            # spark.sql(drop_db_sql)

        # for db in databases:
        #     for table in get_folder_content(spark, f"{RAW_PATH}/{db}"):
        #         path = f"{RAW_PATH}/{db}/{table}"
        #         delta_params = (spark, path,)
        #
        #         if DeltaTable.isDeltaTable(*delta_params):
        #             # dt = DeltaTable.forPath(*delta_params)
        #             create_table_sql = CREATE_TABLE.format(
        #                 table_name=f"raw_{db}.{table}",
        #                 loc=path,
        #             )
        #
        #             print(create_table_sql)
        #             spark.sql(create_table_sql)


if __name__ == '__main__':
    main()

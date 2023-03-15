from src.spark.spark_utils import start_spark, get_spark_logger, get_folder_content
from pyspark import SparkConf, SparkContext
from os import getenv
from delta import DeltaTable
import subprocess


# RAW_PATH = getenv("RAW_PATH")
RAW_PATH = "/data/raw/api"
CREATE_DB = "create database if not exists {db_name}"
CREATE_TABLE = """
create external table if not exists {table_name} (
    {dtypes}
)
location '{loc}'
"""
CONF = [
    ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
    ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
    ("spark.sql.legacy.createHiveTableByDefault.enabled", "true"),
    ("spark.databricks.delta.retentionDurationCheck.enabled", "false"),
    ('hive.exec.dynamic.partition', 'true'),
    ('hive.exec.max.dynamic.partitions', '2048'),
    ('hive.exec.dynamic.partition.mode', 'nonstrict'),
]


def main():
    conf = SparkConf().setAll(CONF)

    with start_spark(
        app_name='instrument_data',
        config=conf,
        use_delta=True,
        use_hive=True,
    ) as spark:

        databases = get_folder_content(spark, RAW_PATH)
        # databases = [i for i in databases if i == "api"]

        db_tables = dict()
        for db in databases:
            db_tables[db] = get_folder_content(spark, f"{RAW_PATH}/{db}")

        for k, v in db_tables.items():
            print("k, v = ", k, v)




        # for db, tables in db_tables.items():
        #     for table in tables:
        #         path = f"{RAW_PATH}/{db}/{table}"
        #         delta_params = (spark, path,)
        #         if DeltaTable.isDeltaTable(*delta_params):
        #             dt = DeltaTable.forPath(*delta_params)
        #             dt.vacuum(24)


if __name__ == '__main__':
    main()

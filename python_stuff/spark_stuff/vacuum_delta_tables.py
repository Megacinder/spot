from src.spark.spark_utils import start_spark, get_spark_logger, get_folder_content
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from os import getenv
from delta import DeltaTable
import subprocess
from pathlib import Path
from pandas import DataFrame, read_parquet

RAW_PATH = getenv("RAW_PATH")
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


def get_folder_content1(spark, path: str) -> list:
    sc = spark.sparkContext
    hadoop = sc._jvm.org.apache.hadoop
    fs = hadoop.fs.FileSystem
    conf = hadoop.conf.Configuration()
    hadoop_path = hadoop.fs.Path(path)
    content = [str(f.getPath()) for f in fs.get(conf).listStatus(hadoop_path)]
    return content


def main():
    conf = SparkConf().setAll(CONF)

    with start_spark(
        app_name='instrument_data',
        config=conf,
        instance='local'
    ) as spark:

        databases = get_folder_content(spark, RAW_PATH)
        # databases = [i for i in databases if i == "api"]

        # print(get_folder_content1(spark, RAW_PATH))

        for i in get_folder_content1(spark, RAW_PATH):
            if 'parquet' in i:
                print(i)
                df = spark.read.parquet(i).toPandas()
                print(df)

        # db_tables = dict()
        # for db in databases:
        #     db_tables[db] = get_folder_content(spark, f"{RAW_PATH}/{db}")
        #
        # for k, v in db_tables.items():
        #     print("k, v = ", k, v)




        # for db, tables in db_tables.items():
        #     for table in tables:
        #         path = f"{RAW_PATH}/{db}/{table}"
        #         delta_params = (spark, path,)
        #         if DeltaTable.isDeltaTable(*delta_params):
        #             dt = DeltaTable.forPath(*delta_params)
        #             dt.vacuum(24)


if __name__ == '__main__':
    main()

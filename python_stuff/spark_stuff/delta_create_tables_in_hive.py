from argparse import Namespace
from delta import DeltaTable
from pathlib import Path
from pyspark.sql import SparkSession

from src.spark.spark_utils import (
    start_spark, get_spark_logger,
    get_hdfs_folder_content, ArgsBuilder,
)

CREATE_DB = "create database if not exists {db_name}"
CREATE_TABLE = """
create table if not exists {table_name}
using delta
location '{loc}'
"""


def create_databases_and_tables(spark: SparkSession, root_path: str) -> None:
    sc = spark.sparkContext
    logger = get_spark_logger(spark)

    databases = get_hdfs_folder_content(sc, root_path)

    for db in databases:
        db_path = str(Path(root_path, db))
        create_db_sql = CREATE_DB.format(db_name=f"{Path(root_path).name}_{db}")
        logger.info(create_db_sql)
        spark.sql(create_db_sql)

        for table in get_hdfs_folder_content(sc, db_path):
            path = str(Path(db_path, table))
            logger.info(f"path to create the table: {path}")
            if DeltaTable.isDeltaTable(spark, path):
                create_table_sql = CREATE_TABLE.format(
                    table_name=f"raw_{db}.{table}",
                    loc=path,
                )
                logger.info(create_table_sql)
                spark.sql(create_table_sql)


def main(args: Namespace):
    with start_spark(app_name='create_delta_tables_in_hive') as spark:
        create_databases_and_tables(spark, args.root_path)


if __name__ == '__main__':
    args = (
        ArgsBuilder("data_path_params")
        .with_arg('--root-path', type=str, required=True)
        .build()
    )
    main(args)

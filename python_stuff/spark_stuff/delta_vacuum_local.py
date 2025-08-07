from argparse import Namespace
from delta import DeltaTable
from pathlib import PurePath
from pyspark.sql import SparkSession

from src.spark.spark_utils import (
    start_spark, get_spark_logger,
    get_hdfs_folder_content, ArgsBuilder,
)


def perform_vacuum(spark: SparkSession, root_path: str, retention_hours: int) -> None:
    sc = spark.sparkContext
    logger = get_spark_logger(spark)

    databases = get_hdfs_folder_content(sc, root_path)

    for db in databases:
        db_path = str(PurePath(root_path, db))
        for table in get_hdfs_folder_content(sc, db_path):
            path = str(PurePath(db_path, table))
            logger.info(f"path to the table for vacuuming: {path}")
            delta_params = (spark, path,)
            if DeltaTable.isDeltaTable(*delta_params):
                dt = DeltaTable.forPath(*delta_params)
                dt.vacuum(retention_hours)


def main(args: Namespace):
    with start_spark(app_name='vacuum_delta_tables') as spark:
        perform_vacuum(spark, args.root_path, args.retention_hours)


if __name__ == '__main__':
    args = (
        ArgsBuilder("vacuum_path_params")
        .with_arg('--root-path', type=str, required=True)
        .with_arg('--retention-hours', type=int, required=True)
        .build()
    )
    main(args)

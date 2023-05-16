from argparse import Namespace
from datetime import timedelta
from pyspark import SparkConf
from pyspark.sql.types import ArrayType, DateType
from pyspark.sql.functions import to_date, col
from src.spark.spark_utils import start_spark, get_spark_logger


def main():
    with start_spark(
        app_name="currency",
        # enable_hive_support=True,
        # instance='remote_spark_shell',
    ) as spark:

        df = spark.sql("show databases")

        df.show()
        for i in spark.sparkContext.getConf().getAll():
            print(i)


if __name__ == '__main__':
    main()

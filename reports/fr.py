from delta import DeltaTable
from pyspark import SparkConf
from pyspark.sql.functions import col, lit
from src.spark.spark_utils import start_spark
from src.utils import get_filename

from secret.sql.fr_sql import SQL_COUNT_OPER, SQL_COUNT_PROFIT, SQL_SUM_PROFIT, SQL_SUM_VOLUME


def main():
    with start_spark(
        app_name=get_filename(__file__),
        # instance='local',
    ) as spark:
    #     for i in spark.sparkContext.getConf().getAll():
    #         print(i)
    #     # df = DeltaTable.forPath(spark, "/data/raw/reports/mt4_users").toDF()
    #     # print("before row_count:", df.count())
    #     # # df = (
    #     # #     df.filter(
    #     # #         ~df["LOGIN"].isin(EXCLUDED_LOGINS)
    #     # #     )
    #     # # )
        df = spark.sql(SQL_COUNT_OPER)
        df.show(3)
        df = spark.sql(SQL_COUNT_PROFIT)
        df.show(3)
        df = spark.sql(SQL_SUM_PROFIT)
        df.show(3)
        df = spark.sql(SQL_SUM_VOLUME)
        df.show(3)

    #     # print("after row_count", df.count())
    #     # spark.sql("show databases").show()
    #     # spark.sql("select * from raw_account.user").show(3)


if __name__ == "__main__":
    main()

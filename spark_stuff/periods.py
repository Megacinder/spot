from argparse import Namespace
from datetime import timedelta
from pyspark.sql.types import ArrayType, DateType
from pyspark.sql.functions import to_date, col
from src.spark.spark_utils import start_spark, get_spark_logger


# UDF
def generate_date_series(start, stop):
    return [start + timedelta(days=x) for x in range(0, (stop - start).days + 1)]


def main():
    with start_spark(
        app_name="periods",
        master='local',
        instance='local',
    ) as spark:

        start_date, end_date = "2023-01-01", "2023-01-15"

        logger = get_spark_logger(spark)
        logger.info("Register UDF for later usage: generate_date_series")
        spark.udf.register("generate_date_series", generate_date_series, ArrayType(DateType()))

        df = (
            spark
            .createDataFrame([(start_date, end_date)], ("start_date", "end_date"))
            .withColumn("start_date", to_date(col("start_date"), "yyyy-MM-dd"))
            .withColumn("end_date", to_date(col("end_date"), "yyyy-MM-dd"))
        )

        logger.info("df is a DataFrame with columns `start_date` and `end_date` of type DateType()")
        df.createOrReplaceTempView("data")
        logger.info("generating date series and saving them to delta table")

        df = (
            spark
            .sql("select explode(sequence(date'2000-01-01', date'2050-01-01'))  as date")
            .union(spark.sql("select date'1970-01-01' as date"))
            .selectExpr(
                "date",
                "date_format(date,'yyyy-MM') as year_month",
                "dayofyear(date) day_of_year",
                "month(date) as month",
                "quarter(date) as quarter",
                "year(date) as year",
                "extract(dayofweek_iso from date) as day_of_week",
                "extract(week from date) as week_of_year",
                "case when extract(dayofweek_iso from date) = 1 then 1 else 0 end as first_day_of_week",
                "case when extract(dayofweek_iso from date) = 7 then 1 else 0 end as las_day_of_week",
                "case when date = trunc(date,'month') then 1 else 0 end as first_day_of_mont",
                "case when date = last_day(date) then 1 else 0 end as last_day_of_mont"
            )
        )

        df.show()

        print(df.rdd.getNumPartitions())


if __name__ == '__main__':
    main()


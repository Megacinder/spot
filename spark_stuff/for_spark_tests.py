from argparse import Namespace
from datetime import timedelta
from pyspark.sql.types import ArrayType, DateType, StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import to_date, col, lit, when, avg, min, max, round, explode, split, regexp_replace, sum
from src.spark.spark_utils import start_spark, get_spark_logger
from pyspark.sql import SparkSession


def main():
    with (
        SparkSession
        .builder.master("local[*]")
        .appName('PySpark_Tutorial')
        .getOrCreate()
    ) as spark:

        # schema = StructType(
        #     StructField()
        # )

        a = ["year", "acousticness", "danceability", "energy", "liveness", "speechiness", "valence"]

        options = {
            "delimiter": ",",
            "header": True,
            "escape": '"',
        }

        data = (
            spark.read
            .options(**options)
            .csv("data.csv")
        )

        #for i in data.dtypes:
        #    print(i)

        df = data
        for icol in a[1:]:
            df = (
                df
                .withColumn(
                    icol,
                    df[icol].cast(DoubleType())
                )
            )

        df1 = (
            df
            .select(a)
            .groupBy(a[0])
            .agg(
                round(avg(a[1]), 2).alias(a[1]),
                round(avg(a[2]), 2).alias(a[2]),
                round(avg(a[3]), 2).alias(a[3]),
                round(avg(a[4]), 2).alias(a[4]),
                round(avg(a[5]), 2).alias(a[5]),
                round(avg(a[6]), 2).alias(a[6]),
            )
            .sort(a[0])
        )


        # for icol in a[1:]:
        #     df1 = (
        #         df1
        #         .withColumn(
        #             icol,
        #             round(icol, 2)
        #         )
        #     )

        # df1.show(5)

        schema = StructType([
            StructField("id", StringType(), nullable=True),
            StructField("name", StringType(), True),
            StructField("artists", StringType(), True),
            StructField("duration_ms", DoubleType(), True),
            StructField("release_date", DateType(), True),
            StructField("year", IntegerType(), True),
            StructField("acousticness", StringType(), True),
            StructField("danceability", StringType(), True),
            StructField("energy", StringType(), True),
            StructField("instrumentalness", StringType(), True),
            StructField("liveness", StringType(), True),
            StructField("loudness", StringType(), True),
            StructField("speechiness", StringType(), True),
            StructField("tempo", StringType(), True),
            StructField("valence", StringType(), True),
            StructField("mode", StringType(), True),
            StructField("key", StringType(), True),
            StructField("popularity", DoubleType(), True),
            StructField("explicit", StringType(), True),
        ])

        data = (
            spark.read.option("delimiter", ",")
            .option("header", True)
            .option("escape", '"')
            .schema(schema)
            .csv("data.csv")
        )

        df = data

        df = df.withColumn(
            "to_unnest",
            split(
                regexp_replace(col("artists"), r"[\[\]\']", ""),
                ", ",
           ),
        )

        df = df.withColumn(
            "unnest",
            explode(col("to_unnest")),
        )

        df_art = (
            df.select(
                "unnest",
                "id",
                "popularity",
            )
            .groupBy("unnest")
            .agg(
                sum(lit(1)).alias("cnt"),
                avg(col("popularity")).alias("avg_pop"),
            )
        )

        df_art = df_art.filter(col("cnt") >= 200).sort(col("avg_pop").desc())
        df_art.select(col("unnest").alias("artist")).show(5)
        #print(df.filter(col("artists").rlike("'Drake'")).count())


if __name__ == '__main__':
    main()


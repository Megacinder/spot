from src.spark.spark_utils import start_spark, get_spark_logger, get_folder_content
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
from delta import DeltaTable

RAW_PATH = os.getenv("RAW_PATH")
CREATE_DB = "create database if not exists {db_name}"
CREATE_TABLE = """
create external table if not exists {table_name} (
    {dtypes}
)
location '{loc}'
"""


def main():
    CONF = [
        ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
        ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
        ('hive.exec.dynamic.partition', 'true'),
        ('hive.exec.max.dynamic.partitions', '2048'),
        ('hive.exec.dynamic.partition.mode', 'nonstrict'),
    ]

    conf = SparkConf().setAll(CONF)

    with start_spark(
        app_name='instrument_data',
        config=conf,
        use_delta=True,
        use_hive=True,
    ) as spark:
        databases = get_folder_content(spark, RAW_PATH)
        print('databases = ', databases)

        for db in databases:
            print(CREATE_DB.format(db_name=f"raw_{db}"))

        folders = dict()
        for db in databases:
            folders[db] = get_folder_content(spark, f"{RAW_PATH}/{db}")

        print('folders = ', folders)

        for db, tables in folders.items():
            for table in tables:
                path = f"{RAW_PATH}/{db}/{table}"
                delta_params = (spark, path,)
                # descr = spark.sql(f"describe detail '{str(path)}'").collect()[0].asDict()

                if DeltaTable.isDeltaTable(*delta_params):
                    df = DeltaTable.forPath(*delta_params).toDF()
                    fields = [f"{i[0]} {i[1]}" for i in df.dtypes]
                    fields = ", \n    ".join(fields)
                    create_sql = CREATE_TABLE.format(
                        table_name=f"raw_{db}.{table}",
                        dtypes=fields,
                        loc=path,
                    )
                    print(create_sql)

                # print(f"{db}.{table}: {descr}")


        # spark.sql("create database if not exists ttt")
        # df1 = spark.sql("show databases")
        # df1.show()

        # co = spark.sparkContext.getConf().getAll()

        # spark.sql("create table ma1 (id int)")


if __name__ == '__main__':
    main()

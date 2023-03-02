from src.spark.spark_utils import start_spark, get_spark_logger, get_folder_content
from pyspark import SparkConf, SparkContext
from os import getenv
from delta import DeltaTable

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
    ('hive.exec.dynamic.partition', 'true'),
    ('hive.exec.max.dynamic.partitions', '2048'),
    ('hive.exec.dynamic.partition.mode', 'nonstrict'),
]


def main():
    conf = SparkConf().setAll(CONF)

    with start_spark(
        app_name='instrument_data',
        config=conf,
        # use_delta=True,
        use_hive=True,
    ) as spark:
        logger = get_spark_logger(spark)

        databases = get_folder_content(spark, RAW_PATH)
        databases = [i for i in databases if i == "api"]
        print('databases = ', databases)

        for db in databases:
            create_db_sql = CREATE_DB.format(db_name=f"raw_{db}")
            print(create_db_sql)
            # spark.sql(create_db_sql)
            for i in range(10):
                logger.info("test_log_" + str(i))
            logger.info(str(spark.sparkContext.getConf().getAll()))
            spark.sql("select * from test.instrument_data limit 10")
        #
        # folders = dict()
        # for db in databases:
        #     folders[db] = get_folder_content(spark, f"{RAW_PATH}/{db}", logger)
        #
        # print('folders = ', folders)
        #
        # for db, tables in folders.items():
        #     for table in tables:
        #         path = f"{RAW_PATH}/{db}/{table}"
        #         delta_params = (spark, path,)
        #         # descr = spark.sql(f"describe detail '{str(path)}'").collect()[0].asDict()
        #
        #         if DeltaTable.isDeltaTable(*delta_params):
        #             df = DeltaTable.forPath(*delta_params).toDF()
        #             fields = [f"{i[0]} {i[1]}" for i in df.dtypes]
        #             fields = ", \n    ".join(fields)
        #             create_table_sql = CREATE_TABLE.format(
        #                 table_name=f"raw_{db}.{table}",
        #                 dtypes=fields,
        #                 loc=path,
        #             )
        #             print(create_table_sql)
        #             spark.sql(create_table_sql)


if __name__ == '__main__':
    main()

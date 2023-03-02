import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip


class Log4j:
    def __init__(self, spark: SparkSession):
        conf = spark.sparkContext.getConf()
        app_id = conf.get('spark.app.id')
        app_name = conf.get('spark.app.name')

        log4j = spark._jvm.org.apache.log4j
        message_prefix = '<' + app_name + ' ' + app_id + '>'
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def error(self, message: str):
        self.logger.error(message)

    def warn(self, message: str):
        self.logger.warn(message)

    def info(self, message: str):
        self.logger.info(message)


def start_spark(
    app_name: str,
    master: str = None,
    config: SparkConf = None,
    use_delta: bool = None,
    use_hive: bool = None,
) -> SparkSession:

    spark_builder = SparkSession.builder.appName(app_name)

    if config:
        spark_builder = spark_builder.config(conf=config)

    if master:
        spark_builder = spark_builder.master(master)

    if use_delta:
        spark_builder = configure_spark_with_delta_pip(spark_builder)

    if use_hive:
        spark_builder = spark_builder.enableHiveSupport()

    spark = spark_builder.getOrCreate()

    return spark


def get_spark_logger(spark: SparkSession) -> Log4j:
    spark_logger = Log4j(spark)

    spark_logger.info("command line args" + str(sys.argv))
    spark_logger.info("Spark session created")

    return spark_logger


def get_folder_content(spark:SparkSession, path: str) -> list:
    sc = spark.sparkContext
    hadoop = sc._jvm.org.apache.hadoop
    fs = hadoop.fs.FileSystem
    conf = hadoop.conf.Configuration()
    path = hadoop.fs.Path(path)

    content = [str(f.getPath()).split('/')[-1] for f in fs.get(conf).listStatus(path)]
    return content

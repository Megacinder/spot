from argparse import ArgumentParser, Namespace
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pathlib import Path
from typing import Any


def unzip_packages():
    from zipfile import ZipFile
    from os import getcwd

    cur_dir = getcwd()
    with ZipFile(f'{cur_dir}/packages.zip', mode='r') as ar:
        ar.extractall()


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
    instance: str = None,
    enable_unzip: bool = False,
    enable_hive_support: bool = False,
) -> SparkSession:

    spark_builder = (
        SparkSession
        .builder
        .appName(app_name)
    )

    # when remote spark can't find modules in packages.zip
    # if enable_unzip:
    #     unzip_packages()

    if instance == 'remote_spark_shell':
        from src.connection.envs import envs
        envs = envs()


        # pkg = ["io.delta:delta-core_2.12:2.3.0"]
        conf = [
        #     # ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
        #     # ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
        #     # ("spark.sql.legacy.createHiveTableByDefault.enabled", "true"),
        #     # ("spark.master", "cluster"),
        #     ("spark.sql.uris", f"thrift://{envs['HADOOP_SERV']}:9083"),
        #     ("hive.metastore.uris", f"thrift://{envs['HADOOP_SERV']}:9083"),
            ("spark.sql.warehouse.dir", f"hdfs://{envs['HADOOP_SERV']}:9000{envs['HIVE_WH']}"),
        #     ('spark.sql.catalogImplementation', 'hive'),
        #     ('spark.shell.deployMode', 'cluster'),
        #     ('spark.jars.packages', 'io.delta:delta-core_2.12:2.3.0'),
        ]

        conf = SparkConf().setAll(conf)
        spark_builder = spark_builder.config(conf=conf)

        from delta.pip_utils import configure_spark_with_delta_pip
        spark_builder = configure_spark_with_delta_pip(spark_builder)

    if enable_hive_support:
        spark_builder = spark_builder.enableHiveSupport()

    if config:
        spark_builder = spark_builder.config(conf=config)

    if master:
        spark_builder = spark_builder.master(master)

    spark = spark_builder.getOrCreate()

    return spark


def get_spark_logger(spark: SparkSession) -> Log4j:
    spark_logger = Log4j(spark)

    spark_logger.info("command line args" + str(sys.argv))
    spark_logger.info("Spark session created")

    return spark_logger


def get_folder_content(spark: SparkSession, path: str) -> list:
    sc = spark.sparkContext
    hadoop = sc._jvm.org.apache.hadoop
    fs = hadoop.fs.FileSystem
    conf = hadoop.conf.Configuration()
    hadoop_path = hadoop.fs.Path(path)
    content = [Path(str(f.getPath())).name for f in fs.get(conf).listStatus(hadoop_path)]
    return content


class ArgsBuilder:
    def __init__(self, description: str = ''):
        self._parser = ArgumentParser(description=description)

    def with_arg(self, name: str, type: Any, **kwargs):
        self._parser.add_argument(name, type=type, **kwargs)
        return self

    def build(self) -> Namespace:
        return self._parser.parse_args()


def get_hdfs_folder_content(sc: SparkContext, path: str) -> list:
    hadoop = sc._jvm.org.apache.hadoop
    fs = hadoop.fs.FileSystem
    conf = hadoop.conf.Configuration()
    path = hadoop.fs.Path(path)

    content = [Path(str(f.getPath())).name for f in fs.get(conf).listStatus(path)]
    return content

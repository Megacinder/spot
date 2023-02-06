from requests import get
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, DataFrame, Column, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType, ArrayType, DataType, LongType
import json
import pendulum
from typing import Generator


JSON_PATH = "https://data.forex4you.com/instrument-data.json"

re = get("https://data.forex4you.com/instrument-data.json").json()

print(re)

# JSON_PATH = "./ignore/nested_stuff/tickers_test.json"
data = re

print()
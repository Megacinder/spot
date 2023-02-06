from requests import get
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
import pendulum
from typing import Generator
import json


def get_data(url):
    return get(url).json() if 'https' in url else json.load(open(url))


def get_spark():
    config = SparkConf().setAll([
        ('spark.executor.memory', '4g'),
        ('spark.driver.memory', '4g'),
    ])
    return SparkSession.builder.config(conf=config).appName('spark_session').getOrCreate()


def get_schema():
    return StructType([
        StructField('server', StringType(), False),
        StructField('market', StringType(), False),
        StructField('symbol_type', StringType(), True),
        StructField('symbol', StringType(), True),
        StructField('param', StringType(), True),
        StructField('indicator', StringType(), True),
        StructField('value', StringType(), True),
        StructField('insert_time', LongType(), True),
    ])


def get_insert_time(data):
    return int(pendulum.from_timestamp(data["_metadata"]["generated"]).format("YYYYMMDDHHmmss"))


def iterate_dict_pairs(dict_obj, depth) -> Generator:
    depth -= 1
    for key, value in dict_obj.items():
        if key == "_metadata":
            continue
        if isinstance(value, dict) and depth != 0:
            for pair in iterate_dict_pairs(value, depth):
                yield key, *pair
        elif not isinstance(value, dict) and depth != 0:
            yield key, *[None for _ in range(depth)], None if not value else value
        else:
            yield key, str(value)


def get_full_dict(data, schema, insert_time):
    rows = []
    for line in iterate_dict_pairs(dict_obj=data, depth=len(schema) - 2):
        table_values = list(line)
        table_values.append(insert_time)
        row = dict(zip(schema.names, table_values))
        rows.append(row)
    return rows


def main(path):
    data = get_data(path)
    schema = get_schema()
    insert_time = get_insert_time(data)
    rows = get_full_dict(data, schema, insert_time)
    for i in rows:
        print(i)


if __name__ == '__main__':
    JSON_PATH = "../ignore/nested_stuff/tickers_test.json"
    main(JSON_PATH)


# df = spark.createDataFrame(rows, schema)
# df.show(10)
# spark.stop()

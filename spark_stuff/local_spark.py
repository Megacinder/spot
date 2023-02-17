from json import load
from pendulum import from_timestamp
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, LongType
from requests import get
from typing import Generator, Union

# from src.spark.dependencies.utils import start_spark, get_spark_logger


def get_data(url: str) -> dict:
    if 'https' in url:
        return get(url).json()
    else:
        return load(open(url))


def get_insert_time(data: dict, logger) -> int:
    if '_metadata' in data:
        val = from_timestamp(data['_metadata']['generated']).format('YYYYMMDDHHmmss')
        try:
            val = int(val)
        except (ValueError, TypeError):
            if logger:
                logger.info('metadata.generated is not int type')
            else:
                print('metadata.generated is not int type')
        return val
    return -1


def iterate_dict_pairs(dict_obj: dict, depth: int) -> Generator:
    depth -= 1
    for key, value in dict_obj.items():
        if key == '_metadata':
            continue
        if depth == 0:
            yield key, str(value)
        elif not isinstance(value, dict):
            yield (key, *[None for _ in range(depth)], str(value) if value else None,)
        else:
            for pair in iterate_dict_pairs(value, depth):
                yield (key, *pair,)


def get_list_of_rows(data: dict, schema: StructType, last_field: Union[str, int, float]) -> list:
    rows = []
    for line in iterate_dict_pairs(dict_obj=data, depth=len(schema) - 2):
        row = list(line)
        row.append(last_field)
        rows.append(row)
    return rows


def get_table(path: str, schema: StructType, logger=None) -> list:
    data = get_data(path)
    insert_time = get_insert_time(data, logger)
    table = get_list_of_rows(data, schema, insert_time)
    return table


def main():
    SOURCE_JSON_PATH = 'https://data.pr.eglobal.app/instrument-data.json'
    # SOURCE_JSON_PATH = '../ignore/nested_stuff/tickers_test.json'
    TARGET_PARQUET_PATH = '/data/raw/api/instrument_data'

    schema = StructType([
        StructField('server', StringType(), False),
        StructField('market', StringType(), False),
        StructField('symbol_type', StringType(), True),
        StructField('symbol', StringType(), True),
        StructField('param', StringType(), True),
        StructField('indicator', StringType(), True),
        StructField('value', StringType(), True),
        StructField('insert_time', LongType(), True),
    ])

    # with start_spark(app_name='instrument_data') as spark:
    #    logger = get_spark_logger(spark)

    table = get_table(SOURCE_JSON_PATH, schema)
    for i in table:
        print(i)
    #    df = spark.createDataFrame(table).repartition(1)
    #    df.write.format('delta').mode('overwrite').save(TARGET_PARQUET_PATH)


if __name__ == '__main__':
    main()

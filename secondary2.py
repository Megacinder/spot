from json import load
from logging import getLogger
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pendulum import from_timestamp
from requests import get
from typing import Generator, Union

logger = getLogger("instrument_data")


def get_data(url: str) -> dict:
    if 'https' in url:
        return get(url).json()
    else:
        return load(open(url))


def get_spark() -> SparkSession:
    config = SparkConf().setAll([
        ('spark.pyspark.python', './ENV/bin/python'),
        ('spark.pyspark.driver.python', './ENV/bin/python'),
        ('spark.executor.memory', '10g'),
        ('spark.driver.memory', '10g'),
        ('spark.shuffle.io.connectionTimeout', '60'),
    ])
    return (
        SparkSession
        .builder
        .config(conf=config)
        .appName('mihaahre_session')
        .getOrCreate()
    )


def get_schema() -> StructType:
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


def get_insert_time(data: dict) -> int:
    if "_metadata" in data:
        val = from_timestamp(data["_metadata"]["generated"]).format("YYYYMMDDHHmmss")
        try:
            val = int(val)
        except (ValueError, TypeError):
            logger.info("metadata.generated is not int type")
        return val
    return -1


def iterate_dict_pairs(dict_obj: dict, depth: int) -> Generator:
    depth -= 1
    for key, value in dict_obj.items():
        if key == "_metadata":
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
        table_values = list(line)
        table_values.append(last_field)
        # row = dict(zip(schema.names, table_values))
        row = tuple(table_values)
        rows.append(row)
    return rows


def get_table_and_schema(path: str) -> tuple:
    data = get_data(path)
    schema = get_schema()
    insert_time = get_insert_time(data)
    table = get_list_of_rows(data, schema, insert_time)
    return table, schema


def write_table(mode, what):
    TARGET_PARQUET_PATH = "/tmp/instrument_data.parquet"

    if what == 'see':
        SOURCE_JSON_PATH = "/Users/mi.ahre/PycharmProjects/spot/secret/json/tickers_test.json"
        table, _ = get_table_and_schema(SOURCE_JSON_PATH)
        table = table[:10]
        for i in table:
            print(i)
        return

    with get_spark() as spark:
        if what == 'json':
            # SOURCE_JSON_PATH = "https://data.forex4you.com/instrument-data.json"
            SOURCE_JSON_PATH = "https://data.pr.eglobal.app/instrument-data.json"
            table, schema = get_table_and_schema(SOURCE_JSON_PATH)
            # table = table[:10]
            # print(table)
            df = spark.createDataFrame(table, schema)
        else:
            table = [
                {'server': 'CENT', 'market': 'FX', 'symbol_type': 'MAJORS', 'symbol': 'EUR/USD', 'param': 'spotPrice', 'indicator': 'buy', 'value': '1.0825', 'insert_time': 'asdas'},
                {'server': 'CENT', 'market': 'FX', 'symbol_type': 'MAJORS', 'symbol': 'EUR/USD', 'param': 'spotPrice', 'indicator': 'sell', 'value': '1.0823', 'insert_time': 'asdas'},
                {'server': 'CENT', 'market': 'FX', 'symbol_type': 'MAJORS', 'symbol': 'EUR/USD', 'param': 'tradingHours', 'indicator': 'open', 'value': '00:00', 'insert_time': 'asdas'},
                {'server': 'CENT', 'market': 'FX', 'symbol_type': 'MAJORS', 'symbol': 'EUR/USD', 'param': 'tradingHours', 'indicator': 'close', 'value': '23:00', 'insert_time': 'asdas'},
                {'server': 'CENT', 'market': 'FX', 'symbol_type': 'MAJORS', 'symbol': 'EUR/USD', 'param': 'tradingHours', 'indicator': 'break', 'value': '23:00-23:10, 23:00-23:10', 'insert_time': 'asdas'},
                {'server': 'CENT', 'market': 'FX', 'symbol_type': 'MAJORS', 'symbol': 'EUR/USD', 'param': 'mappedInstrumentName', 'indicator': None, 'value': 'SILVER', 'insert_time': 'asdas'},
                {'server': 'CENT', 'market': 'FX', 'symbol_type': 'MAJORS', 'symbol': 'EUR/USD', 'param': 'swap', 'indicator': 'buy', 'value': "{'points': -2.329, 'pips': {'USD': -23.29, 'EUR': -21.5169998, 'RUB': 0}}", 'insert_time': 'asdas'},
                {'server': 'CENT', 'market': 'FX', 'symbol_type': 'MAJORS', 'symbol': 'EUR/USD', 'param': 'swap', 'indicator': 'sell', 'value': "{'points': 0.141, 'pips': {'USD': 1.41, 'EUR': 1.3026608, 'RUB': 0}}", 'insert_time': 'asdas'},
                {'server': 'CENT', 'market': 'FX', 'symbol_type': 'ENERGY CASH', 'symbol': None, 'param': None, 'indicator': None, 'value': None, 'insert_time': 'asdas'},
                {'server': 'CENT', 'market': 'STOCK', 'symbol_type': 'RUS', 'symbol': None, 'param': None, 'indicator': None, 'value': None, 'insert_time': 'asdas'},
                {'server': 'CENT', 'market': 'STOCK', 'symbol_type': 'ADR', 'symbol': None, 'param': None, 'indicator': None, 'value': None, 'insert_time': 'asdas'},
                {'server': 'CENT', 'market': 'STOCK', 'symbol_type': 'US', 'symbol': '..C/USD', 'param': 'spotPrice', 'indicator': 'buy', 'value': '49.94', 'insert_time': 'asdas'},
                {'server': 'CENT', 'market': 'STOCK', 'symbol_type': 'US', 'symbol': '..C/USD', 'param': 'spotPrice', 'indicator': 'sell', 'value': '49.9', 'insert_time': 'asdas'},
                {'server': 'CENT_NDD', 'market': 'FX_NDD', 'symbol_type': 'MAJORS', 'symbol': 'GBP/USD', 'param': 'spotPrice', 'indicator': 'buy', 'value': '1.21964', 'insert_time': 'asdas'},
                {'server': 'CENT_NDD', 'market': 'FX_NDD', 'symbol_type': 'MAJORS', 'symbol': 'GBP/USD', 'param': 'spotPrice', 'indicator': 'sell', 'value': '1.21954', 'insert_time': 'asdas'},
                {'server': 'CENT_NDD', 'market': 'FX_NDD', 'symbol_type': 'MAJORS', 'symbol': 'GBP/USD', 'param': 'tradingHours', 'indicator': 'open', 'value': '00:00', 'insert_time': 'asdas'},
                {'server': 'CENT_NDD', 'market': 'FX_NDD', 'symbol_type': 'MAJORS', 'symbol': 'GBP/USD', 'param': 'tradingHours', 'indicator': 'close', 'value': '23:00', 'insert_time': 'asdas'},
                {'server': 'CENT_NDD', 'market': 'FX_NDD', 'symbol_type': 'MAJORS', 'symbol': 'GBP/USD', 'param': 'tradingHours', 'indicator': 'break', 'value': '23:00-23:10, 23:00-23:10', 'insert_time': 'asdas'},
            ]
            df = spark.createDataFrame(table)

        df.write.parquet(TARGET_PARQUET_PATH, mode=mode)


def main():
    write_table('overwrite', 'see')


if __name__ == '__main__':
    main()

# from requests import get
from functools import reduce

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, DataFrame, Column, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType, ArrayType, DataType
from pyspark.sql.functions import explode, col, explode_outer, map_keys, lit, when, array
import json
from pandas.io.json import json_normalize


config = SparkConf().setAll([
    ('spark.executor.memory', '4g'),
    ('spark.driver.memory', '4g'),
])
spark = SparkSession.builder.config(conf=config).appName('spark_session').getOrCreate()

empty_rdd = spark.sparkContext.emptyRDD()
schema = StructType([
    StructField('server', StringType(), False),
    StructField('market', StringType(), False),
    StructField('symbol_type', StringType(), True),
    StructField('symbol', StringType(), True),
    StructField('data', ArrayType(StringType()), True),
    StructField('load_dt', IntegerType(), True),
])
empty_df = spark.createDataFrame(empty_rdd, schema)

JSON_PATH = "../ignore/nested_stuff/tickers_test.json"
df = spark.read.json(path=JSON_PATH, multiLine=True)


# empty_df.select(lit(df.columns[0])s(empty_df.columns[0]), [i for i in empty_df.columns if i != empty_df.columns[0]]).show()


def flatten_test(df, sep="_"):
    complex_fields = dict(
        [
            (field.name, field.dataType)
            for field in df.schema.fields
            if type(field.dataType) == ArrayType
            or type(field.dataType) == StructType
            or type(field.dataType) == MapType
        ]
    )

    i = 0
    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        # print("Processing :" + col_name + " Type : "+str(type(complex_fields[col_name])))

        # if StructType then convert all sub element to columns.
        # i.e. flatten structs
        if type(complex_fields[col_name]) == StructType:
            expanded = [
                col("`" + col_name + "`.`" + k + "`").alias(str(i) + "_" + col_name + sep + k)
                for k in [n.name for n in complex_fields[col_name]]
            ]
            print('expanded = ', expanded)
            df = df.select("*", *expanded).drop(col_name)

        # if ArrayType then add the Array Elements as Rows using the explode function
        # i.e. explode Arrays
        elif type(complex_fields[col_name]) == ArrayType:
            df = df.withColumn(col_name, explode_outer(col_name))

        # if MapType then convert all sub element to columns.
        # i.e. flatten
        elif type(complex_fields[col_name]) == MapType:
            keys_df = df.select(explode_outer(map_keys(col(col_name)))).distinct()
            keys = list(map(lambda row: row[0], keys_df.collect()))
            key_cols = list(
                map(
                    lambda f: col(col_name).getItem(f).alias(str(col_name + sep + f)),
                    keys,
                )
            )
            drop_column_list = [col_name]
            df = df.select(
                [
                    col_name
                    for col_name in df.columns
                    if col_name not in drop_column_list
                ]
                + key_cols
            )

        # recompute remaining Complex Fields in Schema
        if len(complex_fields) == 1:
            complex_fields = dict(
                [
                    (field.name, field.dataType)
                    for field in df.schema.fields
                    if type(field.dataType) == ArrayType
                    or type(field.dataType) == StructType
                    or type(field.dataType) == MapType
                ]
            )
            i += 1

    return df

df_flat = flatten_test(df)
df_flat.printSchema()
df_flat.show()






list_of_df = []

col0 = df.schema.fields[0]
df1 = df.select(
    f"`{col0.name}`.*",
    lit(col0.name).alias("server"),
)
df1.show(truncate=False)

col1 = df1.schema.fields[1]
df2 = df1.select(
    f"`{col1.name}`.*" if type(col1.dataType) == StructType else f"`{col1.name}`",
    f"`{df1.schema.fields[-1]}`",
)
df2.show(truncate=False)


col2 = df2.schema.fields[0]
df3 = df2.select(
    f"`{col2.name}`.*" if type(col2.dataType) == StructType else f"`{col2.name}`",
)
df3.show(truncate=False)





for col0 in [i for i in df.schema.fields if i.name != "_metadata"]:
    df_loop = df.select(
        f"{col0.name}.*",
        lit(col0.name).alias("server"),
    )
    df_loop.show(truncate=False)

    for col1 in [i for i in df_loop.schema.fields if i.name != "server"]:
        df_loop = df_loop.select(
            f"{col1.name}.*" if type(col1.dataType) == StructType else f"{col1.name}",
        )
        df_loop.show(truncate=False)

    df_loop = df_loop.select(
        f"{df_loop.columns[0]}.*",
        col(df_loop.columns[1]),
        lit(df_loop.columns[0]).alias("market"),
    )

    df_loop.show()

    print(type(df_loop.schema.fields[0].dataType) == ArrayType)
    print(df_loop.schema.fields[0].json())
    print(df_loop.schema.fields[0].jsonValue())

    df_loop = df_loop.select(
        f"{df_loop.columns[0]}.*",
        # col(df_loop.columns[0]),
        col(df_loop.columns[1]),
        col(df_loop.columns[2]),
        lit(df_loop.columns[0]).alias("symbol_type"),
    )

    df_loop.show()

    print(type(df_loop.schema.fields[0].dataType) == ArrayType)
    print(df_loop.schema.fields[0].json())
    print(df_loop.schema.fields[0].jsonValue())

    # .withColumn(
    #     "is_empty",
    #     when(col(df_loop.columns[0]) == array(), "1")
    #     .otherwise("0")
    # ).show()


    df_loop = df_loop.select(
        # f"{df_loop.columns[0]}.*" if type(df_loop.columns[0]) == StructType else lit(None),
        col(df_loop.columns[0]),
        col(df_loop.columns[1]),
        col(df_loop.columns[2]),
        col(df_loop.columns[3]),
        lit(df_loop.columns[0]).alias("symbol"),
    )

    list_of_df.append(df_loop)

df_union = reduce(DataFrame.unionAll, list_of_df)


spark.stop()

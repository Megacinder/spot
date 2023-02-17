from pyspark.sql import SparkSession
from pyspark import SparkConf
from requests import get
spark = SparkSession.builder.appName('spark_session').getOrCreate()
# spark.sql("select 1  as field1").show()

# dc = [{"id": 1, "text": "fisrt"}, {"id": 2, "text": "second"}]

config = SparkConf().setAll([
    ('spark.pyspark.python', './ENV/bin/python'),
    ('spark.pyspark.driver.python', './ENV/bin/python'),
    ('spark.executor.memory', '10g'),
    ('spark.driver.memory', '10g'),
    ('spark.shuffle.io.connectionTimeout', '60'),
])

url = "https://data.pr.eglobal.app/instrument-data.json"


dc = get(url).json()

df = spark.createDataFrame(dc)
df.write.parquet("/tmp/ma_test.parquet", mode='overwrite')

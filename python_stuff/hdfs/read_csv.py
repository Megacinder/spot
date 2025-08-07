from pandas import read_csv
from hdfs import InsecureClient
from src.connection.envs import envs

envs = envs()

client_hdfs = InsecureClient(f"http://{envs['HADOOP_SERV']}:9870")
with client_hdfs.read(f"{envs['RAW_PATH']}/xe/currency_rates.csv", encoding='utf-8') as reader:
    df = read_csv(reader)

print(df)

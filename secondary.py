import pandas as pd
import json
import pendulum

JSON_PATH = "ignore/nested_stuff/tickers_test.json"
TABLE_SCHEMA = ["server", "type", "market", "pair", "param", "indicator", "value", "insert_time"]


class DataNode:
    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.children = [DataNode(k, v) for k, v in value.items() if isinstance(v, dict)]

    def iterate(self, insert_time, rows):
        if not self.value or not isinstance(self.value, dict):
            return
        for key, value in self.value.items():
            table_values = [
                self.key,
                key,
                value,
                insert_time,
            ]
            row = dict(zip(TABLE_SCHEMA, table_values))
            rows.append(row)
        for child in self.children:
            child.iterate(insert_time, rows)


data = json.load(open(JSON_PATH))
insert_time = int(pendulum.from_timestamp(data.pop("_metadata")["generated"]).format("YYYYMMDDHHmmss"))

rows = []
root = DataNode("", data)
root.iterate(insert_time, rows)

# df = pd.DataFrame(rows)
# print(df)
for i in rows:
    print(i)
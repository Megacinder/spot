import pandas as pd
import json
import pendulum

JSON_PATH = "ignore/nested_stuff/tickers_test.json"
TABLE_SCHEMA = [
    "server",
    "type",
    "market",
    "pair",
    "param",
    "indicator",
    "value",
    "insert_time",
]

data = json.load(open(JSON_PATH))
insert_time = int(pendulum.from_timestamp(data["_metadata"]["generated"]).format("YYYYMMDDHHmmss"))
data.pop("_metadata")
rows = []
row = dict()

a = []


for k, v in data.items():
    a.append(k)
    print(k, v)

print(a)

# for key1, value1 in data.items():
#     if key1 == "_metadata":
#         continue
#     for key2, value2 in value1.items():
#         if not value2:
#             continue
#         for key3, value3 in value2.items():
#             if not value3:
#                 continue
#             for key4, value4 in value3.items():
#                 if not value4:
#                     continue
#                 for key5, value5 in value4.items():
#                     if not value5 or not isinstance(value5, dict):
#                         continue
#                     for key6, value6 in value5.items():
#                         table_values = [
#                             key1,
#                             key2,
#                             key3,
#                             key4,
#                             key5,
#                             key6,
#                             value6,
#                             insert_time,
#                         ]
#                         row = dict(zip(TABLE_SCHEMA, table_values))
#                         rows.append(row)
#
# # df = pd.DataFrame(rows)
# # print(df)
# for i in rows:
#     print(i)

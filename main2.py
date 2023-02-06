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
#    "insert_time",
]

data = json.load(open(JSON_PATH))
insert_time = int(pendulum.from_timestamp(data["_metadata"]["generated"]).format("YYYYMMDDHHmmss"))
data.pop("_metadata")
rows = []
row = dict()

stack = [data]

i = 0
table_schema = TABLE_SCHEMA.copy()[::-1]

while stack:
    col_name = TABLE_SCHEMA[i]
    stack_item = stack.pop()
    i += 1
    if type(stack_item) == dict:
        for k, v in stack_item.items():
            if type(v) == dict:
                stack.append(v)
            row.update({col_name: k})
            if i == len(TABLE_SCHEMA) - 1:
                val = TABLE_SCHEMA[i]
                row.update({val: v})
                print("row = ", row)
                row1 = row.copy()
                rows.append(row1)

for i in rows:
    print("i =", i)

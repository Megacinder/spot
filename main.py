import pandas as pd
import json
import pendulum

JSON_PATH = "ignore/nested_stuff/tickers_test.json"
df = pd.read_json(JSON_PATH)

json1 = json.loads(open(JSON_PATH).read())

# print(df.columns)
# print(df.index)
#
# print(df.values)

cols = [i for i in df.columns if i != "_metadata"]
rows = [i for i in df.index if i != "generated"]



df2 = pd.DataFrame(cols, columns=["server"])
df3 = pd.DataFrame(rows, columns=["market"])

# print(df2)
# print(df3)
# print(df2.merge(df3, how="cross"))
#


df4 = pd.json_normalize(json1)


pd.set_option('display.max_colwidth', df.shape[1])

# for i in cols:
#     print(df.index)
#     df5 = pd.DataFrame(df[i].values[0].keys(), columns=["type"])
#     print(df5)

# print(df)


# for i in df.columns:
#     print('name =', i)
#     print(df[i].values)



# Sample dataframes
df1 = pd.DataFrame({'key': ['A', 'B', 'C', 'D'],
                    'value1': [1, 2, 3, 4]})

df2 = pd.DataFrame({'key': ['B', 'D', 'E', 'F'],
                    'value2': [5, 6, 7, 8]})

# Left join the two dataframes on the 'key' column
result = pd.merge(df1, df2, on='key', how='left')

# print(result)




# Load the JSON file into a dictionary
with open(JSON_PATH) as json_file:
    data = json.load(json_file)

# Extract the relevant data and store it in a list of dictionaries
rows = []
insert_time = int(pendulum.from_timestamp(data["_metadata"]["generated"]).format("YYYYMMDDHHmmss"))

table_schema = [
    "type",
    "market",
    "pair",
    "param",
    "indicator",
    "value",
    "insert_time"
]


for key1, value1 in data.items():
    if key1 == "_metadata":
        continue
    for key2, value2 in value1.items():
        for key3, value3 in value2.items():
            if not value3:
                continue
            for key4, value4 in value3.items():
                if not value4:
                    continue
                for key5, value5 in value4.items():
                    if not value5 or not isinstance(value5, dict):
                        continue
                    for key6, value6 in value5.items():
                        table_values = [
                            key2,
                            key3,
                            key4,
                            key5,
                            key6,
                            value6,
                            insert_time,
                        ]
                        row = dict(zip(table_schema, table_values))
                        rows.append(row)

# df = pd.DataFrame(rows)
# print(df)
for i in rows:
    print(i)



stack = [(data, [])]
while stack:
    current, keys = stack.pop()
    for key, value in current.items():
        if key == "_metadata":
            continue
        if isinstance(value, dict):
            stack.append((value, keys + [key]))
        elif keys and isinstance(value, (int, float)):
            table_values = [keys[i] for i in (0, 2, 4, 6)] + [value, insert_time]
            row = dict(zip(table_schema, table_values))
            rows.append(row)
import json
data_set = '{"par1_txt": "text", "par2_in": 1}'
print(data_set)
# data_set = json.dumps(data_set)
data_set = json.loads(data_set)

print(data_set["par1_txt"])
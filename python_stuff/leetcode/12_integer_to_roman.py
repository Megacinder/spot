dict1 = {
    "I": 1,
    "V": 5,
    "X": 10,
    "L": 50,
    "C": 100,
    "D": 500,
    "M": 1000,
}
dict1 = {k: v for k, v in sorted(dict1.items(), key=lambda i: i[1], reverse=True)}
list1 = [[i, k, v] for i, (k, v,) in enumerate(dict1.items())]

a = 3749
b = ''

for i, k, v in list1:
    print(a, i, k, v)
    if str(a)[0] in ('4', '9') and i + 1 < len(list1):
        ad = list1[i + 1][1] + k
        a -= (a // list1[i + 1][2]) * v
        print("ad1", ad)
    else:
        ad = k * (a // v)
        a -= (a // v) * v
        print("ad2", ad)
    b += ad
    print(b)
    # a -= (a // v) * v


# print(b)

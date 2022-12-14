a = [(1, 1), (1, 2), (1, 3)]


def ye():
    yield a

print(tuple(ye()))

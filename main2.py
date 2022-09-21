# Program to add two matrices
# using zip()

X = [[1, 2, 3],
     [4, 5, 6],
     [7, 8, 9]]

Y = [[9, 8, 7],
     [6, 5, 4],
     [3, 2, 1]]

result = [map(sum, *t) for t in zip(X, Y)]

for i in zip(X, Y):
    print(*i)
    print(str(map(sum, *i)))

print(result)
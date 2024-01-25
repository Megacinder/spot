import numpy as np

x = np.array([
    [1, 1,   0.3],
    [1, 0.4, 0.5],
    [1, 0.7, 0.8],
])

y = np.array([1, 1, 0])
w = np.array([0, 0, 0])

# print(x)

perfect = False

for i, e in enumerate(x):
    print('e: ', e)
    predict = 1 if w.T.dot(e) > 0 else 0
    if predict != y[i]:
        w = w + e if predict == 0 else w - e

print(*w.round(2), sep=',')

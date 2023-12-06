import numpy as np

a = np.array([[10, 60], [7, 50], [12, 75]])

X = np.array([[1, 60], [1, 50], [1, 75]])
Y = np.array([10, 7, 12])

step1 = X.T.dot(X)
step2 = np.linalg.inv(step1)
step3 = step2.dot(X.T)
b = step3.dot(Y.T)

print(b)

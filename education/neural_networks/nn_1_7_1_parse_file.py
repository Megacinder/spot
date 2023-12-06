import urllib
from urllib import request
import numpy as np

# fname = input()  # read file name from stdin
# f = urllib.request.urlopen(fname)  # open file from URL
with open('/home/mihaahre/PycharmProjects/spot/files/boston_houses.csv') as f:
    data = np.loadtxt(f, delimiter=',', skiprows=1)

y = data[:, :1]
x = data.copy()
x[:, 0] = np.ones(1, dtype=int)

step1 = x.T.dot(x)
step2 = np.linalg.inv(step1)
step3 = step2.dot(x.T)
b = step3.dot(y)

b_arr = np.hstack(b)
b_arr = b_arr.astype(str)
print(' '.join(b_arr))

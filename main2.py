N = 10

a = [True] * N
print(a)
x = 2

while x * x < len(a):
    i = 2
    while i < len(a):
        print('x = ', x, ', i = ', i)
        if i % x == 0:
            print('this is False: x = ', x, ', i = ', i)
            a[x] = False
        i += 1
    x += 1

print(a)


a = [0, 1, 5, 2, 4, 7]
b = [2, 2, 2, 8, 7, 1, 0]

a, b = sorted(a), sorted(b)
print(a, b)

t = []
i, j = 0, 0
q = None
while i < len(a) and j < len(b):
    print('a = ', a[i], ', b = ', b[j])
    if q and (q == a[i] or q == b[j]):
        t.append([q, q])
        if a[i] == q:
            i += 1
        else:
            j += 1
    else:
        eq = a[i] == b[j]
        if eq:
            t.append([a[i], b[j]])
            q = a[i]
            i += 1
            j += 1
        elif a[i] > b[j]:
            j += 1
        else:
            i += 1

print(t)
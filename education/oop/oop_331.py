class Vector:
    def __init__(self, *args):
        self.values = [x for x in args if isinstance(x, int) and not isinstance(x, bool)]
        self.values = sorted(self.values)

    def __str__(self):
        if self.values:
            s = "Вектор{vec}".format(vec=str(tuple(self.values)))
        else:
            s = "Пустой вектор"
        return s

    def __add__(self, other):
        if isinstance(other, int) and not isinstance(other, bool):
            return Vector(*[x + other for x in self.values])
        if isinstance(other, Vector):
            if len(self.values) == len(other.values):
                return Vector(*[sum(x) for x in zip(self.values, other.values)])
            else:
                print("Сложение векторов разной длины недопустимо")
        else:
            print("Вектор нельзя сложить с {val}".format(val=other))

    def __mul__(self, other):
        if isinstance(other, int) and not isinstance(other, bool):
            return Vector(*[x * other for x in self.values])
        if isinstance(other, Vector):
            if len(self.values) == len(other.values):
                return Vector(*[x[0] * x[1] for x in zip(self.values, other.values)])
            else:
                print("Умножение векторов разной длины недопустимо")
        else:
            print("Вектор нельзя умножать с {val}".format(val=other))

v1 = Vector(1,2,3)
print(v1) # печатает "Вектор(1, 2, 3)"

v2 = Vector(3,4,5)
print(v2) # печатает "Вектор(3, 4, 5)"
v3 = v1 + v2
print(v3) # печатает "Вектор(4, 6, 8)"
v4 = v3 + 5
print(v4) # печатает "Вектор(9, 11, 13)"
v5 = v1 * 2
print(v5) # печатает "Вектор(2, 4, 6)"
v5 + 'hi' # печатает "Вектор нельзя сложить с hi"

v6 = v5 + 'hello'
v7 = v5 + [3, 4]
# print(v6)
# print(v7)

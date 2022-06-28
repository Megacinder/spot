class TriangleChecker:
    def __init__(self, a, b, c):
        self.a = a
        self.b = b
        self.c = c

    def is_triangle(self):
        is_instance = (
            isinstance(self.a, (float, int))
            and isinstance(self.b, (float, int))
            and isinstance(self.c, (float, int))
        )
        is_less_than_zero = is_instance and (
            self.a > 0
            and self.b > 0
            and self.c > 0
        )

        is_triangle = is_instance and (
            self.a + self.b > self.c
            and self.a + self.c > self.b
            and self.b + self.c > self.a
        )

        if not is_instance or not is_less_than_zero:
            return 1
        elif not is_triangle:
            return 2
        else:
            return 3


# a, b, c = map(int, input().split())
a, b, c = '1', 2, 3
tr = TriangleChecker(a, b, c)
print(tr.is_triangle())



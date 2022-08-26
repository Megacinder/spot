from math import sqrt


class Line:
    def __init__(self, x1, y1, x2, y2):
        self.x1 = x1
        self.y1 = y1
        self.x2 = x2
        self.y2 = y2

    def __len__(self):
        return int(sqrt(pow(self.x1 - self.x2, 2) + pow(self.y1 - self.y2, 2)))

line = Line(complex(1.4 + 1j), complex(2.5 + 3j), complex(1.8), complex(2.8))
print(len(line))
# print(bool(line))
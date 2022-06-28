import random


class Line:
    def __init__(self, a, b, c, d):
        self.sp = (a, b)
        self.ep = (c, d)


class Rect:
    def __init__(self, a, b, c, d):
        self.sp = (a, b)
        self.ep = (c, d)


class Ellipse:
    def __init__(self, a, b, c, d):
        self.sp = (a, b)
        self.ep = (c, d)


elements = []
for i in range(217):
    elems = (Line, Rect, Ellipse)
    the_chosen_one = random.choice(elems)
    elements.append(the_chosen_one(
        random.randint(0, i),
        random.randint(0, i),
        random.randint(0, i),
        random.randint(0, i),
    ))

for i in elements:
    if isinstance(i, Line):
        i.sp = (0, 0)
        i.ep = (0, 0)

class Integer:
    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, instance, owner):
        return instance.__dict__[self.name]

    def __set__(self, instance, value):
        if isinstance(value, (int, float)):
            instance.__dict__[self.name] = value
        else:
            raise ValueError("int or float, bitch!")


class Box3D:
    width = Integer()
    height = Integer()
    depth = Integer()

    def __init__(self, width, height, depth):
        self.width = width
        self.height = height
        self.depth = depth

    def __do(self, operation, other, new_class=False):
        is_class = isinstance(other, Box3D)
        for var in self.__dict__:
            exec(f"{var} = self.{var} {operation} other{'.' + var if is_class else ''}")
        if new_class:
            return Box3D(locals()["width"], locals()["height"], locals()["depth"])

    def __add__(self, other):
        return self.__do("+", other, True)

    def __mul__(self, other):
        return self.__do("*", other, True)

    def __rmul__(self, other):
        return self * other

    def __sub__(self, other):
        return self.__do("-", other, True)

    def __floordiv__(self, other):
        return self.__do("//", other, True)

    def __mod__(self, other):
        return self.__do("%", other, True)


box1 = Box3D(1, 2, 3)
box2 = Box3D(2, 3, 4)
box = box1 * box2
print(box1.width, box1.height, box1.depth)
print(box2.width, box2.height, box2.depth)
print(box.width, box.height, box.depth)

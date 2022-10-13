def integer_params_decorated(func):
    def wrapper(arg1, *args):
        if not all([isinstance(v, int) for v in args]):
            raise TypeError("аргументы должны быть целыми числами")
        return func(arg1, *args)
    return wrapper


def integer_params(cls):
    methods = {k: v for k, v in cls.__dict__.items() if callable(v)}
    for k, v in methods.items():
        setattr(cls, k, integer_params_decorated(v))

    return cls


@integer_params
class Vector:
    def __init__(self, *args):
        self.__coords = list(args)

    def __getitem__(self, item):
        return self.__coords[item]

    def __setitem__(self, key, value):
        self.__coords[key] = value

    def set_coords(self, *coords, reverse=False):
        c = list(coords)
        self.__coords = c if not reverse else c[::-1]


vector = Vector(1, 2)
print({k: v for k, v in Vector.__dict__.items() if callable(v)})
print(vector[1])
vector[1] = 20.4 # TypeError
print(vector[1])

# cls = Vector
# methods = {k: v for k, v in cls.__dict__.items() if callable(v)}
# print(methods)
# for k, v in methods.items():
#     # setattr(cls, k, integer_params_decorated(v))
#     print(k, ': ', v)

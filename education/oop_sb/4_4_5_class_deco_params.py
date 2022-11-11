def class_log(log_list):
    def deco(cls):
        # print('cls = ', cls)
        methods = {k: v for k, v in cls.__dict__.items() if callable(v)}
        # print('methods = ', methods)
        for k, v in methods.items():
            setattr(cls, k, under_deco(v))
        return cls

    def under_deco(func):
        def wrapper(*args, **kwargs):
            log_list.append(func.__name__)
            return func(*args, **kwargs)
        return wrapper
    return deco


vector_log = []


@class_log(vector_log)
class Vector:
    def __init__(self, *args):
        self.__coords = list(args)

    def __getitem__(self, item):
        return self.__coords[item]

    def __setitem__(self, key, value):
        self.__coords[key] = value


v = Vector(1, 2, 3)
v[0] = 10
print(vector_log)

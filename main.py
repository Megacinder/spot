from functools import wraps


def print_file_name_if_exception(func):
    @wraps(func)
    def new_func(*args, **kwargs):
        try:
            ret = func(*args, **kwargs)
            return ret
        except Exception as e:
            print(f"ERROR in the function: <{func.__name__}>")
            raise e
    return new_func


@print_file_name_if_exception
def fu():
    return 1/0


print(fu())

# def tmp_wrap(func):
#     @wraps(func)
#     def tmp(*args, **kwargs):
#         print func.__name__
#         return func(*args, **kwargs)
#     return tmp
class Singleton(type):
    _instance = None

    def __call__(cls, *args, **kwargs):
        if not cls._instance:
            print("Вы подключились к базе данных")
            cls._instance = super().__call__(*args, **kwargs)
        else:
            print("Существует активное подключение")
        return cls._instance


class DbConnection(metaclass=Singleton):
    pass


a = DbConnection()
b = DbConnection()
print(id(a), id(b))

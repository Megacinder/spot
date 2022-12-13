from typing import Union


class CellException(Exception):
    def return_message(self, typ):
        s = None
        if typ in (int, float):
            s = 'значение выходит за допустимый диапазон'
        elif typ == str:
            s = 'длина строки выходит за допустимый диапазон'
        return s


class CellIntegerException(CellException):
    def __str__(self):
        return self.return_message(int)


class CellFloatException(CellException):
    def __str__(self):
        return self.return_message(float)


class CellStringException(CellException):
    def __str__(self):
        return self.return_message(str)


class Cell:
    def __init__(
        self,
        min_value: int = 0,
        max_value: int = 0,
        min_length: int = 0,
        max_length: int = 0,
        value: Union[int, float, str] = None,
    ):
        self._min_value = min_value
        self._max_value = max_value
        self._min_length = min_length
        self._max_length = max_length
        self._value = value

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    def __setattr__(self, key, value):
        if key == '_value':
            if type(value) == int and not self._min_value <= value <= self._max_value:
                raise CellIntegerException

            if type(value) == float and not self._min_value <= value <= self._max_value:
                raise CellFloatException

            if type(value) == str and not self._min_length <= len(value) <= self._max_length:
                raise CellStringException

        object.__setattr__(self, key, value)

    def __str__(self):
        return self.value


class CellInteger(Cell):
    def __init__(self, min_value, max_value, value=None):
        super().__init__(min_value=min_value, max_value=max_value, value=value)


class CellFloat(Cell):
    def __init__(self, min_value, max_value, value=None):
        super().__init__(min_value=min_value, max_value=max_value, value=value)


class CellString(Cell):
    def __init__(self, min_length, max_length, value=None):
        super().__init__(min_length=min_length, max_length=max_length, value=value)


class TupleData:
    def __init__(self, *args):
        self.args = list(args)
        self.i = -1

    def __getitem__(self, item):
        return self.args[item].value

    def __setitem__(self, key, value):
        self.args[key].value = value

    def __len__(self):
        return len(self.args)

    def __iter__(self):
        return self

    def __next__(self):
        if self.i < len(self.args) - 1:
            self.i += 1
            return self.args[self.i].value
        else:
            raise StopIteration


ld = TupleData(CellInteger(0, 10), CellInteger(11, 20), CellFloat(-10, 10), CellString(1, 100))


res = len(ld) # возвращает общее число элементов (ячеек) в объекте ld
# print(a.__dict__)
print(ld[1])
for d in ld:  # перебирает значения ячеек объекта ld (значения, а не объекты ячеек)
    print(d)
# print(res)


try:
    ld[0] = 1
    ld[1] = 20
    ld[2] = -5.6
    ld[3] = "Python ООП"
except CellIntegerException as e:
    print(e)
except CellFloatException as e:
    print(e)
except CellStringException as e:
    print(e)
except CellException:
    print("Ошибка при обращении к ячейке")
except Exception:
    print("Общая ошибка при работе с объектом TupleData")

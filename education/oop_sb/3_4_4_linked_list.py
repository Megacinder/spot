class StackObj:
    def __init__(self, data, next=None):
        self.__data = data
        self.__next = next

    @property
    def data(self):
        return self.__data

    @data.setter
    def data(self, value):
        self.__data = value

    @property
    def next(self):
        return self.__next

    @next.setter
    def next(self, value):
        self.__next = value


class Stack:
    def __init__(self, top: StackObj = None):
        self.top = top

    def search(self, obj: StackObj):
        if obj.next is None:
            return obj
        return self.search(obj.next)

    def push_back(self, obj: StackObj):
        if self.top is None:
            self.top = obj
            return
        if self.top.next is None:
            self.top.next = obj
            return
        last_obj = self.search(self.top.next)
        last_obj.next = obj

    def pop_back(self):
        if self.top is None:
            return
        if self.top.next is None:
            self.top = None
            return
        if self.top.next is not None:
            self.search(self.top.next).next = None

    def __add__(self, other):
        self.push_back(other)
        return self

    def __iadd__(self, other):
        return self + other

    def __mul__(self, other):
        for i in other:
            self.push_back(StackObj(i))
        return self

    def __imul__(self, other):
        return self * other


assert hasattr(Stack, 'pop_back'), "класс Stack должен иметь метод pop_back"
st = Stack()
top = StackObj("1")
st.push_back(top)
assert st.top == top, "неверное значение атрибута top"
st = st + StackObj("2")
st = st + StackObj("3")
obj = StackObj("4")
st += obj
st = st * ['data_1', 'data_2']
st *= ['data_3', 'data_4']
d = ["1", "2", "3", "4", 'data_1', 'data_2', 'data_3', 'data_4']
h = top
i = 0
while h:
    assert h._StackObj__data == d[
        i], "неверное значение атрибута __data, возможно, некорректно работают операторы + и *"
    h = h._StackObj__next
    i += 1

print(i)
print(len(d))
assert i == len(d), "неверное число объектов в стеке"
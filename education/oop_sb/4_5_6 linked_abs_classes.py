from abc import ABC, abstractmethod


class StackInterface(ABC):
    @abstractmethod
    def push_back(self, obj): ...

    @abstractmethod
    def pop_back(self): ...


class Stack(StackInterface):
    def __init__(self, top=None):
        self._top = top

    def push_back(self, obj):
        tail = self._top
        if tail is None:
            self._top = obj
            return
        elif tail.next is None:
            self._top.next = obj
            return
        else:
            while tail.next:
                tail = tail.next
            tail.next = obj
            return

    def pop_back(self):
        tail = self._top
        if tail is None:
            return None
        elif tail.next is None:
            self._top = None
            return tail
        else:
            while tail.next:
                prev_tail = tail
                tail = tail.next
                if tail.next is None:
                    prev_tail.next = None
                    return tail

    def __str__(self):
        tail = self._top
        s = str(tail)
        while tail:
            tail = tail.next
            s += ' -> ' + str(tail)
        return s


class StackObj:
    def __init__(self, data, next=None):
        self._data = data
        self._next = next

    @property
    def next(self):
        return self._next

    @next.setter
    def next(self, value):
        self._next = value


st = Stack()
st.push_back(StackObj("obj 1"))
print('1. st: ', st)
obj = StackObj("obj 2")
st.push_back(obj)
print('2. st: ', st)
del_obj = st.pop_back() # del_obj - ссылка на удаленный объект (если объектов не было, то del_obj = None)
print('3. st: ', st)
print('del_obj = ', del_obj)

class StackObj:
    def __init__(self, data, next=None):
        self.data = data
        self.next = next


class Stack:
    def __init__(self, top=None):
        self.top = top
        self.count = 0

    def push(self, obj):
        top = self.top
        if not top:
            self.top = obj
        while top:
            if top.next is None:
                top.next = obj
                break
            top = top.next
        self.count += 1

    def pop(self):
        node = self.top
        if not node:
            return None
        if not node.next:
            return node
        obj = self[self.count - 1]
        self[self.count - 2].next = None
        return obj

    def check_index(self, item):
        if item < 0 or item >= self.count or not isinstance(item, int):
            raise IndexError('неверный индекс')

    def __getitem__(self, item):
        self.check_index(item)
        i = 0
        top = self.top
        while top and i < item:
            top = top.next
            i += 1
        return top

    def __setitem__(self, key, value):
        self.check_index(key)
        obj = self[key]
        prev_obj = self[key - 1] if key > 0 else None
        value.next = obj.next
        if prev_obj:
            prev_obj.next = value

    def __repr__(self):
        node = self.top
        nodes = []
        while node is not None:
            nodes.append(node.data)
            node = node.next
        nodes.append("None")
        return " -> ".join(nodes)


st = Stack()
st.push(StackObj("obj11"))
st.push(StackObj("obj12"))
st.push(StackObj("obj13"))
st[1] = StackObj("obj2-new")
assert st[0].data == "obj11" and st[
    1].data == "obj2-new", "атрибут data объекта класса StackObj содержит неверные данные"
try:
    obj = st[3]
except IndexError:
    assert True
else:
    assert False, "не сгенерировалось исключение IndexError"
print(st)
obj = st.pop()
print(obj)
print(st)
assert obj.data == "obj13", "метод pop должен удалять последний объект стека и возвращать его"
n = 0
h = st.top
while h:
    assert isinstance(h, StackObj), "объект стека должен быть экземпляром класса StackObj"
    n += 1
    h = h.next

assert n == 2, "неверное число объектов в стеке (возможно, нарушена его структура)"
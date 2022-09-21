class StackObj:
    def __init__(self, data=None, next=None):
        self.data = data
        self.next = next


class Stack:
    NO_INDEX_MES = "неверный индекс"

    def __init__(self, top=None):
        self.top = top
        self.len = 0

    def is_index_correct(self, ix):
        if isinstance(ix, int) and ix in range(self.len):
            return True
        return False

    def __len__(self):
        return self.len

    def get_object(self, item):
        if not self.is_index_correct(item):
            raise IndexError(self.NO_INDEX_MES)
        obj = self.top
        i = 0
        while i < self.len:
            if i == item:
                return obj
            obj = obj.next
            i += 1

    def __getitem__(self, item):
        if not self.is_index_correct(item):
            raise IndexError(self.NO_INDEX_MES)
        return self.get_object(item).data

    def __setitem__(self, key, value):
        if not self.is_index_correct(key):
            raise IndexError(self.NO_INDEX_MES)
        self.get_object(key).data = value

    def push_back(self, obj):
        node = self.top
        if node is None:
            self.len += 1
            self.top = obj
        elif node.next is None:
            self.len += 1
            self.top.next = obj
        else:
            i = 0
            self.len += 1
            while i < self.len:
                if node.next is None:
                    node.next = obj
                    break
                node = node.next
                i += 1

    def push_front(self, obj):
        node = self.top
        self.top = obj
        self.top.next = node
        self.len += 1

    def __repr__(self):
        i = 0
        node = self.top
        if len(self) == 0:
            return 'empty'
        s = str(node.data)
        while i < len(self):
            if node.next is None:
                return s
            node = node.next
            s += ' -> ' + str(node.data)
        return s

    def __iter__(self):
        self.node = self.top
        return self

    def __next__(self):
        if self.node:
            node = self.node
            self.node = self.node.next
            return node
        else:
            raise StopIteration


st = Stack()
print(st, st.len)
st.push_back(StackObj('1'))
print(st, st.len)
st.push_back(StackObj('22'))
print(st, st.len)
st.push_back(StackObj('333'))
print(st, st.len)
st.push_back(StackObj('444'))
print(st, st.len)
st.push_back(StackObj('54'))
print(st, st.len)
st.push_back(StackObj('6'))
print(st, st.len)
st[2] = '123'
st.push_front(StackObj('-11'))
st.push_front(StackObj('-222'))
print(st, st.len)
for obj in st: # перебор объектов стека (с начала и до конца)
   print(obj.data)  # отображение данных в консоль

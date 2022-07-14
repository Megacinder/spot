class ObjList:
    def __init__(self, data):
        self.__next = None
        self.__prev = None
        self.__data = data

    def set_next(self, obj):
        self.__next = obj

    def set_prev(self, obj):
        self.__prev = obj

    def get_next(self):
        return self.__next

    def get_prev(self):
        return self.__prev

    def set_data(self, data):
        self.__data = data

    def get_data(self):
        return self.__data


class LinkedList:
    def __init__(self, head: ObjList = None, tail: ObjList = None):
        self.head = head
        self.tail = tail
        self.data_list = []

    def add_obj(self, obj: ObjList):
        if self.tail:
            prev_tail = self.tail
            prev_tail.set_next(obj)
            self.tail = obj
            self.tail.set_prev(prev_tail)
        else:
            self.head = obj
            self.tail = obj
        self.data_list.append(obj.get_data())

    def remove_obj(self):
        current_tail = self.tail
        if current_tail:
            prev_obj = current_tail.get_prev()
            if prev_obj:
                prev_obj.set_next(None)
                self.tail = prev_obj
            else:
                self.tail = None
            self.data_list.remove(current_tail.get_data())

    def get_data(self):
        return self.data_list


ob = ObjList("данные 1")
lst = LinkedList()
lst.add_obj(ObjList("данные 1"))
lst.add_obj(ObjList("данные 2"))
#lst.remove_obj()
#lst.add_obj(ObjList("данные 3"))
res = lst.get_data()    # ['данные 1', 'данные 2', 'данные 3']
print(res)

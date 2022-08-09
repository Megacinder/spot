class NewList:
    def __init__(self, lst=None):
        self.lst = lst or []

    @staticmethod
    def sub_two_lists(left_list, right_list):
        indexes_for_delete = []

        if isinstance(right_list, (list, NewList)):
            for ix1, i in enumerate(left_list):
                for ix2, j in enumerate(right_list):
                    if i == j and type(i) == type(j):
                        indexes_for_delete = [ix1] + indexes_for_delete
                        right_list.pop(ix2)
                        break
        for i in indexes_for_delete:
            left_list.pop(i)
        return left_list

    def __sub__(self, other):
        left_list = self.lst.copy()
        right_list = (other.lst if isinstance(other, NewList) else other).copy()
        return NewList(self.sub_two_lists(left_list, right_list))

    def __rsub__(self, other):
        left_list = (other.lst if isinstance(other, NewList) else other).copy()
        right_list = self.lst.copy()
        return NewList(self.sub_two_lists(left_list, right_list))

    def __isub__(self, other):
        return self - other

    def get_list(self):
        return self.lst


lst = NewList()
lst1 = NewList([0, 1, -3.4, "abc", True])
lst2 = NewList([1, 0, True])
assert lst1.get_list() == [0, 1, -3.4, "abc", True] and lst.get_list() == [], "метод get_list вернул неверный список"
res1 = lst1 - lst2

res2 = lst1 - [0, True]
res3 = [1, 2, 3, 4.5] - lst2
lst1 -= lst2
assert res1.get_list() == [-3.4, "abc"], "метод get_list вернул неверный список"


assert res2.get_list() == [1, -3.4, "abc"], "метод get_list вернул неверный список"
assert res3.get_list() == [2, 3, 4.5], "метод get_list вернул неверный список"
assert lst1.get_list() == [-3.4, "abc"], "метод get_list вернул неверный список"
lst_1 = NewList([1, 0, True, False, 5.0, True, 1, True, -7.87])
lst_2 = NewList([10, True, False, True, 1, 7.87])

print(lst_1.lst, '\n', lst_2.lst)
res = lst_1 - lst_2
print(res.get_list())
assert res.get_list() == [0, 5.0, 1, True, -7.87], "метод get_list вернул неверный список"
a = NewList([2, 3])
res_4 = [1, 2, 2, 3] - a # NewList: [1, 2]
assert res_4.get_list() == [1, 2], "метод get_list вернул неверный список"
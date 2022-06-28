class ListObject:
    def __init__(self, data):
        self.data = data
        self.next_obj = None

    def link(self, obj):
        self.next_obj = obj


# lst_in = [
#     "1. Первые шаги в ООП",
#     "1.1 Как правильно проходить этот курс",
#     "1.2 Концепция ООП простыми словами",
#     "1.3 Классы и объекты. Атрибуты классов и объектов",
#     "1.4 Методы классов. Параметр self",
#     "1.5 Инициализатор init и финализатор del",
#     "1.6 Магический метод new. Пример паттерна Singleton",
#     "1.7 Методы класса (classmethod) и статические методы (staticmethod)",
# ]
lst_in = "foo bar baz foo1 bar1 baz1".split()

ls = lst_in[:]
head_obj = ListObject(data=ls.pop(0))
node = head_obj
while ls:
    node.link(ListObject(ls.pop(0)))
    node = node.next_obj

import time


class Mechanical:
    def __init__(self, date):
        self.date = date

    def __setattr__(self, key, value):
        cond1 = key == "date" and type(value) == float and value > 0
        cond2 = "date" in self.__dict__
        if not cond1 or cond2:
            return
        object.__setattr__(self, key, value)


class Aragon:
    def __init__(self, date):
        self.date = date

    def __setattr__(self, key, value):
        cond1 = key == "date" and type(value) == float and value > 0
        cond2 = "date" in self.__dict__
        if not cond1 or cond2:
            return
        object.__setattr__(self, key, value)


class Calcium:
    def __init__(self, date):
        self.date = date

    def __setattr__(self, key, value):
        cond1 = key == "date" and type(value) == float and value > 0
        cond2 = "date" in self.__dict__
        if not cond1 or cond2:
            return
        object.__setattr__(self, key, value)


class GeyserClassic:
    MAX_DATE_FILTER = 100

    def __init__(self):
        self.slots = {}

    def add_filter(self, slot_num, filter):
        if slot_num in self.slots:
            return
        elif (
            type(filter) == Mechanical and slot_num == 1 or
            type(filter) == Aragon and slot_num == 2 or
            type(filter) == Calcium and slot_num == 3
        ):
            self.slots[slot_num] = filter

    def remove_filter(self, slot_num):
        self.slots.pop(slot_num, None)

    def get_filters(self):
        return self.slots.values()

    def water_on(self):
        if (
            len(self.slots) == 3 and
            sum(list(map(lambda x: 0 <= time.time() - x.date <= self.MAX_DATE_FILTER, self.slots.values()))) == 3
        ):
            return True
        return False



my_water = GeyserClassic()
my_water.add_filter(1, Mechanical(time()))
my_water.add_filter(2, Aragon(time()))
assert my_water.water_on() == False, "метод water_on вернул True, хотя не все фильтры были установлены"
my_water.add_filter(3, Calcium(time()))
assert my_water.water_on(), "метод water_on вернул False при всех трех установленных фильтрах"
f1, f2, f3 = my_water.get_filters()
assert isinstance(f1, Mechanical) and isinstance(f2, Aragon) and isinstance(f3, Calcium), "фильтры должны быть устанлены в порядке: Mechanical, Aragon, Calcium"
my_water.remove_filter(1)
assert my_water.water_on() == False, "метод water_on вернул True, хотя один из фильтров был удален"
my_water.add_filter(1, Mechanical(time()))
assert my_water.water_on(), "метод water_on вернул False, хотя все три фильтра установлены"
f1, f2, f3 = my_water.get_filters()
my_water.remove_filter(1)
my_water.add_filter(1, Mechanical(time() - GeyserClassic.MAX_DATE_FILTER - 1))
assert my_water.water_on() == False, "метод water_on вернул True, хотя у одного фильтра истек срок его работы"
f1 = Mechanical(1.0)
f2 = Aragon(2.0)
f3 = Calcium(3.0)
assert 0.9 < f1.date < 1.1 and 1.9 < f2.date < 2.1 and 2.9 < f3.date < 3.1, "неверное значение атрибута date в объектах фильтров"
f1.date = 5.0
f2.date = 5.0
f3.date = 5.0
assert 0.9 < f1.date < 1.1 and 1.9 < f2.date < 2.1 and 2.9 < f3.date < 3.1, "локальный атрибут date в объектах фильтров должен быть защищен от изменения"
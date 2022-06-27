class Translator:
    di1 = {}

    def add(self, eng, rus):
        if eng not in self.di1:
            self.di1[eng] = [rus]
        else:
            self.di1[eng].append(rus)

    def remove(self, eng):
        del self.di1[eng]

    def translate(self, eng):
        return self.di1[eng]


tr = Translator()
tr.add("tree", "дерево")
tr.add("car", "машина")
tr.add("car", "автомобиль")
tr.add("leaf", "лист")
tr.add("river", "река")
tr.add("go", "идти")
tr.add("go", "ехать")
tr.add("go", "ходить")
tr.add("milk", "молоко")

tr.remove("car")
print(*tr.translate("go"))

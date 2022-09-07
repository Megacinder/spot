class Cell:
    def __init__(self, is_free=True, value=0):
        self.is_free = is_free
        self.value = value

    def __bool__(self):
        return self.is_free


class TicTacToe:
    def __init__(self):
        self.pole = [[Cell() for _ in range(3)] for _ in range(3)]

    def clear(self):
        for row in self.pole:
            for i in row:
                i.is_free = True
                i.value = 0

    def __getitem__(self, item):
        if all(isinstance(i, int) for i in item) and all(i >= 3 for i in item):
            raise IndexError('неверный индекс клетки')

        y = item[0]
        x = item[1]

        if isinstance(y, slice):
            arr = []
            for row in self.pole:
                for i in range(len(row)):
                    if i == x:
                        arr.append(row[i])
            return tuple([j.value for j in arr[y]])

        if isinstance(x, slice):
            return tuple([j.value for j in list(self.pole[y][x])])

        return self.pole[y][x].value

    def __setitem__(self, key, value):
        y = key[0]
        x = key[1]

        if any(i >= 3 for i in key):
            raise IndexError('неверный индекс клетки')

        if not self.pole[y][x].is_free:
            raise ValueError('клетка уже занята')

        self.pole[y][x].value = value

    def __str__(self):
        s = ''
        for row in self.pole:
            for i in row:
                s += str(i.value)
            s += '\n'
        return s


g = TicTacToe()
g.clear()
print(g[0, 0])
assert g[0, 0] == 0 and g[2, 2] == 0, "начальные значения всех клеток должны быть равны 0"

from random import randint


class Cell:
    def __init__(self):
        self.__is_mine = False
        self.__number = 0
        self.__is_open = False
        self.ERROR = "недопустимое значение атрибута"

    @property
    def is_mine(self):
        return self.__is_mine

    @is_mine.setter
    def is_mine(self, value):
        if value not in (False, True):
            raise ValueError(self.ERROR)
        self.__is_mine = value

    @property
    def number(self):
        return self.__number

    @number.setter
    def number(self, value):
        if value not in range(9):
            raise ValueError(self.ERROR)
        self.__number = value

    @property
    def is_open(self):
        return self.__is_open

    @is_open.setter
    def is_open(self, value):
        if value not in (False, True):
            raise ValueError(self.ERROR)
        self.__is_open = value

    def __bool__(self):
        return True if not self.is_open else False


class GamePole:
    __instance = None

    def __new__(cls, *args, **kwargs):
        if not cls.__instance:
            cls.__instance = super().__new__(cls)
        return cls.__instance

    def __init__(self, n, m, total_mines):
        self.n = n
        self.m = m
        self.total_mines = total_mines
        self.__pole_cells = [[Cell() for _ in range(self.m)] for _ in range(self.n)]

    @property
    def pole(self):
        return self.__pole_cells

    def init_pole(self):
        for row in self.pole:
            for i in row:
                i.is_open = False
        for _ in range(self.total_mines):
            n = randint(0, self.n - 1)
            m = randint(0, self.m - 1)
            self.pole[n][m].is_mine = True
            self.pole[n][m].number = 0
            for a, b in (
                (-1, 0), (-1, 1), (0, 1), (1, 1), (1, 0), (1, -1), (0, -1), (-1, -1)
            ):
                if (
                    0 <= n + a < len(self.pole) and
                    0 <= m + b < len(self.pole[n + a])
                    # self.pole[n + a][m + b].is_mine is False
                ):
                    self.pole[n + a][m + b].number += 1

    def open_cell(self, i, j):
        if i > self.n - 1 or j > self.m - 1:
            raise IndexError('некорректные индексы i, j клетки игрового поля')
        self.pole[i][j].is_open = True

    def show_pole(self):
        for row in self.pole:
            for i in row:
                if i.is_mine:
                    print('*', end=' ')
                else:
                    print(i.number, end=' ')
            print()


pole = GamePole(5, 5, 5)  # создается поле размерами 10x20 с общим числом мин 10
pole.init_pole()
# if pole.pole[0][1]:
#     pole.open_cell(0, 1)
# if pole.pole[3][5]:
#     pole.open_cell(3, 5)
# pole.open_cell(30, 100)  # генерируется исключение IndexError
pole.show_pole()

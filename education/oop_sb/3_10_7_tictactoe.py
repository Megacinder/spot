from random import randint


class Cell:
    def __init__(self):
        self.value = 0

    def __bool__(self):
        return self.value == 0


class TicTacToe:
    FREE_CELL = 0  # свободная клетка
    HUMAN_X = 1  # крестик (игрок - человек)
    COMPUTER_O = 2  # нолик (игрок - компьютер)
    WIN_COMBS = (
        (0, 1, 2), (3, 4, 5), (6, 7, 8),
        (0, 3, 6), (1, 4, 7), (2, 5, 8),
        (0, 4, 8), (2, 4, 6),
    )

    def __init__(self, square_size: int = 3):
        self.square_size = square_size
        self.pole = tuple(tuple(Cell() for _ in range(self.square_size)) for _ in range(self.square_size))
        self.is_game_continue = True

    def init(self):
        self.pole = tuple(tuple(Cell() for _ in range(self.square_size)) for _ in range(self.square_size))

    def __check_one_ix(self, ix):
        is_ix_good = isinstance(ix, int) and ix in range(self.square_size)
        if not is_ix_good:
            raise IndexError('некорректно указанные индексы')
        else:
            return ix

    def __check_both_ixs(self, item: tuple):
        x, y = self.__check_one_ix(item[0]), self.__check_one_ix(item[1])
        return x, y

    def __get_free_cells(self):
        a = []
        for y in range(self.square_size):
            for x in range(self.square_size):
                if self.pole[y][x].value == 0:
                    a.append([y, x])
        return a

    def __getitem__(self, item):
        y, x = self.__check_both_ixs(item)
        return self.pole[y][x].value

    def __setitem__(self, key, value):
        y, x = self.__check_both_ixs(key)
        self.pole[y][x].value = value

    def show(self):
        for row in self.pole:
            for i in row:
                print(i.value, end=' ')
            print()

    def __go(self, who):
        free_cells = self.__get_free_cells()
        if len(free_cells) > 0:
            i = randint(0, len(free_cells) - 1)
            y = free_cells[i][0]
            x = free_cells[i][1]
            self.pole[y][x].value = who
        else:
            self.is_game_continue = False

    def human_go(self):
        self.__go(self.HUMAN_X)

    def computer_go(self):
        self.__go(self.COMPUTER_O)

    def get_pole_in_list(self):
        a = []
        for row in self.pole:
            for i in row:
                a.append(i.value)
        return a

    def who_wins(self):
        mx_list = self.get_pole_in_list()
        if sum(mx_list) < 3:
            return
        possible_comb = ''
        for comb in self.WIN_COMBS:
            for i in comb:
                possible_comb += str(mx_list[i])
            if possible_comb in ('111', '222'):
                return possible_comb
            possible_comb = ''
        if 0 not in mx_list:
            return '333'

    @property
    def is_human_win(self):
        b = self.who_wins()
        if b == '111':
            return True
        else:
            return False

    @property
    def is_computer_win(self):
        b = self.who_wins()
        if b == '222':
            return True
        else:
            return False

    @property
    def is_draw(self):
        b = self.who_wins()
        if b == '333':
            return True
        else:
            return False

    def __bool__(self):
        return self.is_game_continue


cell = Cell()
assert cell.value == 0, "начальное значение атрибута value объекта класса Cell должно быть равно 0"
assert bool(cell), "функция bool для объекта класса Cell вернула неверное значение"
cell.value = 1
assert bool(cell) == False, "функция bool для объекта класса Cell вернула неверное значение"
assert hasattr(TicTacToe, 'show') and hasattr(TicTacToe, 'human_go') and hasattr(TicTacToe, 'computer_go'), "класс TicTacToe должен иметь методы show, human_go, computer_go"
game = TicTacToe()
assert bool(game), "функция bool вернула неверное значения для объекта класса TicTacToe"
assert game[0, 0] == 0 and game[2, 2] == 0, "неверные значения ячеек, взятые по индексам"
game[1, 1] = TicTacToe.HUMAN_X
assert game[1, 1] == TicTacToe.HUMAN_X, "неверно работает оператор присваивания нового значения в ячейку игрового поля"
game[0, 0] = TicTacToe.COMPUTER_O
assert game[0, 0] == TicTacToe.COMPUTER_O, "неверно работает оператор присваивания нового значения в ячейку игрового поля"
game.init()
assert game[0, 0] == TicTacToe.FREE_CELL and game[1, 1] == TicTacToe.FREE_CELL, "при инициализации игрового поля все клетки должны принимать значение из атрибута FREE_CELL"
try:
    game[3, 0] = 4
except IndexError:
    assert True
else:
    assert False, "не сгенерировалось исключение IndexError"
game.init()
assert game.is_human_win == False and game.is_computer_win == False and game.is_draw == False, "при инициализации игры атрибуты is_human_win, is_computer_win, is_draw должны быть равны False, возможно не пересчитывается статус игры при вызове метода init()"
game[0, 0] = TicTacToe.HUMAN_X
game[1, 1] = TicTacToe.HUMAN_X
game[2, 2] = TicTacToe.HUMAN_X
game.show()
print(game.is_human_win)
assert game.is_human_win and game.is_computer_win == False and game.is_draw == False, "некорректно пересчитываются атрибуты is_human_win, is_computer_win, is_draw. Возможно не пересчитывается статус игры в момент присвоения новых значения по индексам: game[i, j] = value"
game.init()
game[0, 0] = TicTacToe.COMPUTER_O
game[1, 0] = TicTacToe.COMPUTER_O
game[2, 0] = TicTacToe.COMPUTER_O
assert game.is_human_win == False and game.is_computer_win and game.is_draw == False, "некорректно пересчитываются атрибуты is_human_win, is_computer_win, is_draw. Возможно не пересчитывается статус игры в момент присвоения новых значения по индексам: game[i, j] = value"
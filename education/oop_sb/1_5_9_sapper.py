import random


class Cell:
    def __init__(self, around_mines=0, mine=False):
        self.around_mines = around_mines
        self.mine = mine
        self.fl_open = False


class GamePole:
    PRIME_SYMBOL = '#'
    REPLACE_SYMBOL = '*'
    ADD_MINES = (
        (0, -1), (0, 1),
        (1, -1), (1, 0), (1, 1),
        (-1, -1), (-1, 0), (-1, 1)
    )

    def __init__(self, n, m):
        self.n = n
        self.m = m
        self.pole = [[Cell() for _ in range(n)] for _ in range(n)]
        self.init()

    def init(self):
        i = 0
        while i < self.m:
            a, b = random.randint(0, self.n - 1), random.randint(0, self.n - 1)
            if not self.pole[a][b].mine:
                self.pole[a][b].mine = True
                self.pole[a][b].around_mines = self.REPLACE_SYMBOL
                for j in self.ADD_MINES:
                    if 0 <= a + j[0] < self.n and 0 <= b + j[1] < self.n:
                        if self.pole[a + j[0]][b + j[1]].around_mines != self.REPLACE_SYMBOL:
                            self.pole[a + j[0]][b + j[1]].around_mines += 1
                i += 1

    def show(self):
        o_list = []
        x = []
        for i in self.pole:
            for j in i:
                x.append(j.around_mines)
            o_list.append(x)
            x = []
        return o_list


pole_game = GamePole(n=10, m=12)
pole_game.show()


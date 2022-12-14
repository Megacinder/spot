from random import randint, choice


class Ship:
    def __init__(self, length: int = 1, tp: int = 1, x: int = None, y: int = None):
        self._length = length
        self._tp = tp
        self._x = x
        self._y = y
        self._is_move = True
        self._cells = [1 for _ in range(self._length)]

    def set_start_coords(self, x: int, y: int):
        self._x = x
        self._y = y

    def get_start_coords(self):
        return self._x, self._y

    def get_length(self):
        return self._length

    def move(self, go: int):
        x_for_zero, y_for_zero = self._x, self._y if go in (-1, 1) else (-1, -1)
        if self._is_move and go in (1, -1):
            if self._tp == 1:
                if go == -1:
                    x_for_zero = self._x + self._length - 1
                self._x += go
            else:
                if go == -1:
                    y_for_zero = self._y + self._length - 1
                self._y += go
        return x_for_zero, y_for_zero

    def ship_plus_space_around(self, area='space'):
        space = []

        mul_x = self._length if self._tp == 1 else 1
        mul_y = 1 if self._tp == 1 else self._length

        ratio = 0 if area == 'ship' else 1

        ox_range = range(-1 * ratio, 1 * mul_x + 1 * ratio, 1)
        oy_range = range(-1 * ratio, 1 * mul_y + 1 * ratio, 1)

        for x in ox_range:
            for y in oy_range:
                space.append((self._x + x, self._y + y))

        return set(space)

    def is_collide(self, ship):
        if not isinstance(ship, Ship):
            return False

        space1 = self.ship_plus_space_around('space')
        ship1 = self.ship_plus_space_around('ship')

        space2 = ship.ship_plus_space_around('space')
        ship2 = ship.ship_plus_space_around('ship')

        if (ship1 & ship2) or (space1 & ship2) or (ship1 & space2):
            return True
        return False

    def is_out_pole(self, size):
        for x, y in self.ship_plus_space_around('ship'):
            if x is not None and y is not None and (x < 0 or x >= size - 1 or y < 0 or y >= size - 1):
                return True
        return False

    def __setitem__(self, key, value):
        self._cells[key] = value

    def __getitem__(self, item):
        return self._cells[item]

    def __repr__(self):
        return "({x}:{y}, len={length}, tp={tp})".format(
            x=str(self._x),
            y=str(self._y),
            length=str(self._length),
            tp=str(self._tp),
        )


class GamePole:
    def __init__(self, size):
        self._size = size
        self._ships = []
        self._pole = [[0 for _ in range(self._size)] for _ in range(self._size)]
        self._coords = []
        self._marked_ships = []

    def make_set_of_xy(self):
        if not self._coords:
            for i in range(self._size):
                for j in range(self._size):
                    self._coords.append((i, j))

    def set_ship_numbers(self, ship: Ship, number=1):
        coords = []
        for co in ship.ship_plus_space_around('ship'):
            self._pole[co[1]][co[0]] = number
            coords.append(co)
        return coords

    def get_random_xy(self, list_of_tuples):
        x, y = choice(list_of_tuples)
        return x, y

    def set_ships(self):
        for i in self._ships:
            is_out_of_pole = True
            xy_list = self._coords[:]
            while (
                is_out_of_pole
                or any([i.is_collide(j) for j in self._marked_ships])
                # or counter_of_random_combs < number_of_random_combs
            ):
                if not xy_list:
                    break
                x, y = self.get_random_xy(xy_list)
                i.set_start_coords(x, y)
                is_out_of_pole = i.is_out_pole(self._size)
                xy_list.remove((x, y,))
            if xy_list:
                self._marked_ships.append(i)
                coords = self.set_ship_numbers(i, 1)
                for co in coords:
                    self._coords.remove(co)

    def init(self):
        self.make_set_of_xy()
        max_length = 4
        for i in range(1, max_length + 1, 1):
            if i <= max_length - 3:
                self._ships.append(Ship(length=max_length, tp=randint(1, 2)))
            if i <= max_length - 2:
                self._ships.append(Ship(length=max_length - 1, tp=randint(1, 2)))
            if i <= max_length - 1:
                self._ships.append(Ship(length=max_length - 2, tp=randint(1, 2)))
            if i <= max_length:
                self._ships.append(Ship(length=max_length - 3, tp=randint(1, 2)))
        self.set_ships()

    def get_ships(self):
        return self._ships

    def move_ships(self):
        for i in self._marked_ships:

            possible_ways = [-1, 1]
            j = choice(possible_ways)

            while possible_ways:
                x_for_zero, y_for_zero = i.move(j)
                is_out_of_pole = i.is_out_pole(self._size)
                is_collide = any([i.is_collide(j) for j in self._marked_ships if j != i])
                # print('new i - x,y =', i.get_start_coords())
                if not is_out_of_pole and not is_collide:
                    self._pole[y_for_zero][x_for_zero] = 0
                    self.set_ship_numbers(i, 1)
                    break
                j = -1 * j
                i.move(j)
                possible_ways.remove(j)

    def get_pole(self):
        ret = tuple([tuple(i) for i in self._pole])
        return ret

    def show(self):
        for i in self.get_pole():
            print(*i)


# a = Ship(x=3, y=3, length=2, tp=2)
# b = Ship(x=5, y=5, length=3, tp=1)
# print(a, b)
#
# print(b._cells)
#
# # print(s.ship_coords_plus_space_around('ship'))
# # print(b.ship_coords_plus_space_around('ship'))
# # print(s.is_collide(b))
#
# g = GamePole(10)
# g.init()
# print(g.get_ships())
# g.show()
# g.move_ships()
# g.show()


# ship = Ship(2)
# ship = Ship(2, 1)
# ship = Ship(3, 2, 0, 0)
# assert ship._length == 3 and ship._tp == 2 and ship._x == 0 and ship._y == 0, "неверные значения атрибутов объекта класса Ship"
# assert ship._cells == [1, 1, 1], "неверный список _cells"
# assert ship._is_move, "неверное значение атрибута _is_move"
# ship.set_start_coords(1, 2)
# assert ship._x == 1 and ship._y == 2, "неверно отработал метод set_start_coords()"
# assert ship.get_start_coords() == (1, 2), "неверно отработал метод get_start_coords()"
# ship.move(1)
# s1 = Ship(4, 1, 0, 0)
# s2 = Ship(3, 2, 0, 0)
# s3 = Ship(3, 2, 0, 2)
# assert s1.is_collide(s2), "неверно работает метод is_collide() для кораблей Ship(4, 1, 0, 0) и Ship(3, 2, 0, 0)"
# assert s1.is_collide(
#     s3) == False, "неверно работает метод is_collide() для кораблей Ship(4, 1, 0, 0) и Ship(3, 2, 0, 2)"
# s2 = Ship(3, 2, 1, 1)
# assert s1.is_collide(s2), "неверно работает метод is_collide() для кораблей Ship(4, 1, 0, 0) и Ship(3, 2, 1, 1)"
# s2 = Ship(3, 1, 8, 1)
# assert s2.is_out_pole(10), "неверно работает метод is_out_pole() для корабля Ship(3, 1, 8, 1)"
# s2 = Ship(3, 2, 1, 5)
# assert s2.is_out_pole(10) == False, "неверно работает метод is_out_pole(10) для корабля Ship(3, 2, 1, 5)"
# s2[0] = 2
# assert s2[0] == 2, "неверно работает обращение ship[indx]"
# p = GamePole(10)
# p.init()
# for nn in range(5):
#     for s in p._ships:
#         assert s.is_out_pole(10) == False, "корабли выходят за пределы игрового поля"
#         for ship in p.get_ships():
#             if s != ship:
#                 assert s.is_collide(ship) == False, "корабли на игровом поле соприкасаются"
#     p.move_ships()
#
# gp = p.get_pole()
# assert type(gp) == tuple and type(gp[0]) == tuple, "метод get_pole должен возвращать двумерный кортеж"
# assert len(gp) == 10 and len(gp[0]) == 10, "неверные размеры игрового поля, которое вернул метод get_pole"

pole_size_8 = GamePole(8)
pole_size_8.init()
pole_size_8.show()
class Ship:
    def __init__(self, x: int = None, y: int = None, length: int = 1, tp: int = 1):
        self._x = x
        self._y = y
        self._length = length
        self._tp = tp
        self._is_move = True
        self._cells = [1 for _ in range(self._length)]

    def set_start_coords(self, x: int, y: int):
        self._x = x
        self._y = y

    def get_start_coords(self):
        return self._x, self._y

    def move(self, go: int):
        if self._is_move and go in (1, 0, -1):
            if self._tp == 1:
                self._x += go
            else:
                self._y += go

    def ship_coords_plus_space_around(self, area='space'):
        space = []

        mul_x = self._length if self._tp == 1 else 1
        mul_y = 1 if self._tp == 1 else self._length

        ratio = 0 if area == 'ship' else 1

        ox_range = range(-1 * ratio, 1 * mul_x + 1 * ratio, 1)
        oy_range = range(-1 * ratio, 1 * mul_y + 1 * ratio, 1)

        for x in ox_range:
            for y in oy_range:
                space.append((self._x + x, self._y + y))

        return space

    def is_collide(self, ship):
        if isinstance(ship, Ship):
            for i in self.ship_coords_plus_space_around('ship'):
                for j in ship.ship_coords_plus_space_around('ship'):
                    if i == j:
                        return True
            for i in self.ship_coords_plus_space_around('space'):
                for j in ship.ship_coords_plus_space_around('ship'):
                    if i == j:
                        return True
            for i in self.ship_coords_plus_space_around('ship'):
                for j in ship.ship_coords_plus_space_around('space'):
                    if i == j:
                        return True
            return False


class GamePole:
    def __init__(self): ...


s = Ship(x=3, y=3, length=2, tp=2)
b = Ship(x=4, y=5, length=3, tp=1)

print(s.ship_coords_plus_space_around('ship'))
print(b.ship_coords_plus_space_around('ship'))
print(s.is_collide(b))

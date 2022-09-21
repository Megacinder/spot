class Matrix:
    def __init__(self, *args):
        self.args = args
        self.mx = self.make_matrix(args)

    @staticmethod
    def is_matrix_not_square(mx):
        len_row = []
        for row in mx:
            len_row.append(len(row))
        if len(set(len_row)) != 1:
            return True
        else:
            return False

    @staticmethod
    def matrix_contains_type_only(mx):
        ret_val = True
        for row in mx:
            ret_val = ret_val and all(
                map(
                    lambda x: isinstance(x, (int, float)), row
                )
            )
        return ret_val

    def make_matrix(self, args):
        if len(args) == 1:
            xy = args[0]
            is_matrix_not_square = self.is_matrix_not_square(xy)
            does_matrix_contain_only_numbers = self.matrix_contains_type_only(xy)
            if is_matrix_not_square or not does_matrix_contain_only_numbers:
                raise TypeError('список должен быть прямоугольным, состоящим из чисел')

            mx = xy
        else:
            x = args[1]
            y = args[0]
            f = args[2]
            is_rows_cols_wrong_type = not isinstance(y, int) or not isinstance(y, int)
            is_fill_value_wrong_type = not isinstance(f, (int, float))
            if is_rows_cols_wrong_type or is_fill_value_wrong_type:
                raise TypeError('аргументы rows, cols - целые числа; fill_value - произвольное число')

            mx = [[f for _ in range(x)] for _ in range(y)]
        return mx

    def __getitem__(self, item):
        x, y = item
        return self.mx[x][y]

    def __setitem__(self, key, value):
        if not isinstance(value, (int, float)):
            raise TypeError('значения матрицы должны быть числами')
        if not isinstance(key, tuple) or len(key) != 2:
            raise IndexError('недопустимые значения индексов')
        x, y = key
        if x not in range(len(self.mx[0])) or y not in range(len(self.mx)):
            raise IndexError('недопустимые значения индексов')
        self.mx[x][y] = value

    def __len__(self):
        return 2 ** len(self.mx) + 2 ** len(self.mx[0])

    def __add__(self, other):
        if isinstance(other, Matrix):
            if len(self) != len(other):
                raise ValueError('операции возможны только с матрицами равных размеров')
            mx = [[self.mx[x][y] + other.mx[x][y] for y in range(len(self.mx[0]))] for x in range(len(self.mx))]
            return Matrix(mx)
        if isinstance(other, int):
            mx = [[self.mx[x][y] + other for y in range(len(self.mx[0]))] for x in range(len(self.mx))]
            return Matrix(mx)

    def __sub__(self, other):
        if isinstance(other, Matrix):
            if len(self) != len(other):
                raise ValueError('операции возможны только с матрицами равных размеров')
            mx = [[self.mx[x][y] - other.mx[x][y] for y in range(len(self.mx[0]))] for x in range(len(self.mx))]
            return Matrix(mx)
        if isinstance(other, int):
            mx = [[self.mx[x][y] - other for y in range(len(self.mx[0]))] for x in range(len(self.mx))]
            return Matrix(mx)



mx = [
    [1, 2, 3],
    [4, 5, 6],
]

matr = Matrix(mx)
matr2 = Matrix(2, 3, 4)
print(matr.__dict__)
print(matr.mx)
print(matr2.mx)
print(matr[1, 1])
matr[1, 1] = 15
print(matr.mx)
mx2 = matr + matr2
print(mx2.mx)
# print('matr.args ', matr.args)
# print(matr)

# print(Matrix.matrix_contains_type_only(matr))

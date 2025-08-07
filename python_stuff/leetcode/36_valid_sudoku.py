from typing import List


class Solution:
    def is_valid_sudoku(self, board: List[List[str]]) -> bool:
        sudoku_size = 9

        if len(board) > sudoku_size:
            return False

        for row in board:
            if len(row) > sudoku_size:
                return False

            numbers_only = [i for i in row if i != "."]
            if len(numbers_only) != len(set(numbers_only)):
                return False

        transposed_board = [[row[i] for row in board] for i in range(sudoku_size)]

        for row in transposed_board:
            numbers_only = [i for i in row if i != "."]
            if len(numbers_only) != len(set(numbers_only)):
                return False

        square = []

        for i in (0, 3, 6):

            for j in (0, 3, 6):

                for ii in range(i, i + 3):

                    for jj in range(j, j + 3):
                        if board[ii][jj] != ".":

                            square.append(board[ii][jj])

                if len(square) != len(set(square)):
                    return False

                square = []

        return True

    def is_valid_sudoku2(self, board: List[List[str]]) -> bool:
        res = []
        for i, row in enumerate(board):
            for j, x in enumerate(row):
                if x != '.':
                    print([(i, x), (x, j), (i // 3, j // 3, x)])
                    res += [(i, x), (x, j), (i // 3, j // 3, x)]
        return len(res) == len(set(res))


a = [
    ["5", "3", ".", ".", "7", ".", ".", ".", "."],
    ["6", ".", ".", "1", "9", "5", ".", ".", "."],
    [".", "9", "8", ".", ".", ".", ".", "6", "."],
    ["8", ".", ".", ".", "6", ".", ".", ".", "3"],
    ["4", ".", ".", "8", ".", "3", ".", ".", "1"],
    ["7", ".", ".", ".", "2", ".", ".", ".", "6"],
    [".", "6", ".", ".", ".", ".", "2", "8", "."],
    [".", ".", ".", "4", "1", "9", ".", ".", "5"],
    [".", ".", ".", ".", "8", ".", ".", "7", "9"]
]

b = [
    ["8", "3", ".", ".", "7", ".", ".", ".", "."],
    ["6", ".", ".", "1", "9", "5", ".", ".", "."],
    [".", "9", "8", ".", ".", ".", ".", "6", "."],
    ["8", ".", ".", ".", "6", ".", ".", ".", "3"],
    ["4", ".", ".", "8", ".", "3", ".", ".", "1"],
    ["7", ".", ".", ".", "2", ".", ".", ".", "6"],
    [".", "6", ".", ".", ".", ".", "2", "8", "."],
    [".", ".", ".", "4", "1", "9", ".", ".", "5"],
    [".", ".", ".", ".", "8", ".", ".", "7", "9"]
]


c = Solution()
print(c.is_valid_sudoku2(a))

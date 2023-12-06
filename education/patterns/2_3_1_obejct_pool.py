class DbPool:
    def __init__(self, size: int = 10):
        self.size = size
        self.pool = [DbConnection() for _ in range(size)]

    def acquire(self):
        if len(self.pool) != 0:
            self.pool.pop()

        if self.size - len(self.pool) < 0:
            s = "no resources"
        else:
            s = f"Доступы выданы, осталось подключений: {len(self.pool)}"
        return s

    def release(self, reusable):
        self.pool.append(reusable)


class DbConnection:
    pass
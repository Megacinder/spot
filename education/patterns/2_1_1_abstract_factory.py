import abc


class AbstractGUIFactory(metaclass=abc.ABCMeta):
    def __init__(self, colour: str):
        self.colour = colour

    def create_button(self):
        return PushButton(self.colour)

    def create_checkbox(self):
        return Checkbox(self.colour)


class AbstractButton(metaclass=abc.ABCMeta):
    def __init__(self, colour):
        self.colour = colour

    def push(self):
        """
        zalupa
        :return:
        """
        pass


class PushButton(AbstractButton):
    def __init__(self, colour):
        super().__init__(colour)

    def push(self):
        s = f"На кнопку нажали, цвет {self.colour}"
        print(s)


class Checkbox(AbstractButton):
    def __init__(self, colour):
        super().__init__(colour)

    def push(self):
        s = f"Галочка поставлена, цвет {self.colour}"
        print(s)


class WinFactory(AbstractGUIFactory):
    def __init__(self):
        super().__init__("синий")


class MacFactory(AbstractGUIFactory):
    def __init__(self):
        super().__init__("белый")


a = WinFactory()
b = a.create_checkbox()
print(b.colour)

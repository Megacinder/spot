import copy


class Button:
    # your code here
    def __init__(self, colour, button_type):
        self.colour = colour
        self.button_type = button_type

    def push(self):
        if self.button_type == 'pushbutton':
            print(f"На кнопку нажали, цвет {self.colour}")
        if self.button_type == 'checkbox':
            print(f"Галочка поставлена, цвет {self.colour}")


win_checkbox = copy.deepcopy(Button("синий", "checkbox"))
win_pushbutton = copy.deepcopy(Button("синий", "pushbutton"))
mac_checkbox = copy.deepcopy(Button("белый", "checkbox"))
mac_pushbutton = copy.deepcopy(Button("белый", "pushbutton"))
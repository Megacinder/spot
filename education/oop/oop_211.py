import string


class Registration:
    def __init__(self, login, password):
        self.login = login
        self.password = password

    @staticmethod
    def is_include_digit(val):
        for i in val:
            if i.isdigit():
                return True
        return False

    @staticmethod
    def is_include_all_register(val):
        __upper_cnt = 0
        for i in val:
            if i.isupper():
                __upper_cnt += 1
                if __upper_cnt == 2:
                    return True
        return False

    @staticmethod
    def is_include_only_latin(val):
        for i in val:
            if not i.isdigit() and i not in string.ascii_lowercase and i not in string.ascii_uppercase:
                return False
        return True

    @staticmethod
    def check_password_dictionary(val):
        with open('../../easy_passwords.txt', 'r') as f:
            for i in f.readlines():
                # print('i = ', i, 'pwd = ', a.password)
                if i.strip() == val:
                    return False
        return True

    @property
    def login(self):
        return self.__login

    @login.setter
    def login(self, val):
        if val.find('@') == -1:
            raise ValueError("Login must include at least one ' @ '")
        elif val.find('.') == -1:
            raise ValueError("Login must include at least one ' . '")
        else:
            self.__login = val

    @property
    def password(self):
        return self.__password

    @password.setter
    def password(self, val):
        if not isinstance(val, str):
            raise TypeError("Пароль должен быть строкой")
        if not 4 < len(val) < 12:
            raise ValueError('Пароль должен быть длиннее 4 и меньше 12 символов')
        if not Registration.is_include_digit(val):
            raise ValueError('Пароль должен содержать хотя бы одну цифру')
        if not Registration.is_include_all_register(val):
            raise ValueError('Пароль должен содержать хотя бы 2 заглавные буквы')
        if not Registration.is_include_only_latin(val):
            raise ValueError('Пароль должен содержать только латинский алфавит')
        if not Registration.check_password_dictionary(val):
            raise ValueError('Ваш пароль содержится в списке самых легких')
        self.__password = val


a = Registration('usr@.', 'asdfgHH1')
s2 = Registration("translate@gmail.com", "as1SNdf")
print(a.login, a.password)

# val = 'пиздаPIZDA.,mn'
#
# for i in val:
#     if not i.isdigit():
#         if i not in string.ascii_lowercase and i not in string.ascii_uppercase:
#             print('not latin = ', i)
#     elif i.isdigit():
#         print('digit = ', i)
#     else:
#         print('not digit and not latin = ', i)
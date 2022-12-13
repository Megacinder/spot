class ValidatorString:
    def __init__(self, min_length: int, max_length: int, chars: str = ''):
        self.min_length = min_length
        self.max_length = max_length
        self.chars = chars

    def is_valid(self, string):
        if not self.chars:
            is_char_in_str = True
        else:
            is_char_in_str = any([char for char in string if char in self.chars])
        if not (
            type(string) == str
            and self.min_length <= len(string) <= self.max_length
            and is_char_in_str
        ):
            raise ValueError('недопустимая строка')
        return string


class LoginForm:
    def __init__(self, login_validator: ValidatorString, password_validator: ValidatorString):
        self.login_validator = login_validator
        self.password_validator = password_validator

    def form(self, request: dict):
        if 'login' not in request or 'password' not in request:
            raise TypeError('в запросе отсутствует логин или пароль')
        self._login = self.login_validator.is_valid(request['login'])
        self._password = self.password_validator.is_valid(request['password'])


login_v = ValidatorString(4, 50, "")
password_v = ValidatorString(10, 50, "!$#@%&?")
lg = LoginForm(login_v, password_v)
# login, password = input().split()
login, password = 'sergey balakirev!'.split()
try:
    lg.form({'login': login, 'password': password})
except (TypeError, ValueError) as e:
    print(e)
else:
    print(lg._login)
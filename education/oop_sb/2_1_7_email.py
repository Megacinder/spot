from re import match
from random import choice, randint
from string import ascii_letters, digits


class EmailValidator:
    RE_CHECK_EMAIL = "^\w+([-.']\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*"
    PART1_LEN = 100
    PART2_LEN = 50

    def __new__(cls, *args, **kwargs):
        return None

    @staticmethod
    def __is_email_str(email):
        if isinstance(email, str):
            return True
        return False

    @classmethod
    def __is_email_has_right_len(cls, email):
        parts = email.split('@')
        if len(parts) == 1:
            return False
        if len(parts[0]) <= cls.PART1_LEN and len(parts[1]) <= cls.PART2_LEN:
            return True
        return False

    @classmethod
    def check_email(cls, email):
        if cls.__is_email_str(email) and cls.__is_email_has_right_len(email) and match(cls.RE_CHECK_EMAIL, email):
            return True
        return False

    @classmethod
    def get_random_email(cls):
        part1 = ''.join([choice(ascii_letters + digits + '_.') for _ in range(cls.PART1_LEN)])
        i = randint(1, cls.PART2_LEN - 2)
        part2_before_dot = ''.join([choice(ascii_letters + digits + '_') for _ in range(i)])
        part3_after_dot = ''.join([choice(ascii_letters + digits + '_') for _ in range(cls.PART2_LEN - i - 1)])
        return part1 + '@' + part2_before_dot + '.' + part3_after_dot


for i in range(10):
    a = EmailValidator.get_random_email()
    print(a, EmailValidator.check_email(a))

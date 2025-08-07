
s1 = "()"  # False
# s2 = "{[]}(((]}"  # True
# s1 = "([)]"  # False

def is_valid(s: str) -> bool:
    stack = []
    opened = '([{'
    closed = ')]}'

    for i in s:
        if not stack and i in closed:
            return False
        elif i in opened:
            stack.append(i)
        elif (
                (
                stack[-1] == '('
                and i == ')'
            )
            or (
                stack[-1] == '['
                and i == ']'
            )
            or (
                stack[-1] == '{'
                and i == '}'
            )
        ):
            stack.pop()
        else:
            return False

    return True if not stack else False


a = is_valid(s1)
print(a)

class Solution:
    def isValid(self, s: str) -> bool:
        if '(' not in s and '{' not in s and '[' not in s or s[0] in ')}]':
            return False
        i = 0
        q = []
        p1 = 'C'
        p2 = 'E'
        p3 = 'L'
        while i < len(s):
            # print('i =', i, ' q =', q)
            if s[i] == '(':
                q.append(p1)
            if s[i] == '{':
                q.append(p2)
            if s[i] == '[':
                q.append(p3)
            if s[i] in ')}]' and q == []:
                    return False
            if s[i] in ')':
                if p1 != q[-1]:
                    return False
                else:
                    q.pop()
            if s[i] == '}':
                if p2 != q[-1]:
                    return False
                else:
                    q.pop()
            if s[i] == ']':
                if p3 != q[-1]:
                    return False
                else:
                    q.pop()
            # print('inner q = ', q)
            i += 1
        # print('final q = ', q)
        return True if q == [] else False
        # return q


d = "({{{{}}}))"
a = Solution()
print(a.isValid(d))

print(d[0], d[-1])

from typing import List


class Solution:
    @staticmethod
    def get_longest_common_prefix(strs: List[str]) -> str:
        o_list = []
        for i in zip(*strs):
            if i == (i[0],) * len(i):
                o_list.append(i[0])
            if i != (i[0],) * len(i):
                return ''.join(o_list)
        return ''.join(o_list)
        # pref = [
        #     x[0] for x in zip(*strs)
        #     if x == (x[0],) * len(x)
        # ]
        # return ''.join(pref)


a = Solution()
li1 = ["cir", "car"]
li2 = ["dog", "racecar", "car"]
print(a.get_longest_common_prefix(li1))
print(a.get_longest_common_prefix(li2))

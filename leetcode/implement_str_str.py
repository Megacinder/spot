class Solution:
    def strStr(self, haystack: str, needle: str) -> int:
        if needle == "":
            return 0
        if needle[0] not in haystack:
            return -1
        i = 0
        j = 0
        while True:
            if haystack[i] != needle[j] and j == 0:
                i += 1
            else:
                break
            if haystack[i] == needle[j]:
                j += 1
                i += 1




haystack = "hello"
needle = "ll"
a = Solution()

print(a.strStr(haystack, needle))

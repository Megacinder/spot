def str_str(haystack: str, needle: str) -> int:
    if not needle:
        return 0

    len_ha, len_ne = len(haystack), len(needle)

    if len_ne > len_ha:
        return -1

    for i in range(len_ha - len_ne + 1):
        if haystack[i:i + len_ne] == needle:
            return i

    return -1

haystack = "heeeello"
needle = "eell"

a = str_str(haystack, needle)
print(a)

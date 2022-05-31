from typing import List, Tuple


class Solution:
    def removeElement(self, nums: List[int], val: int) -> int:
        k = 0
        for i in nums:
            if i != val:
                nums[k] = i
                k += 1
        return k


nums = [3,2,2,3]
val = 3
a = Solution()
print(a.removeElement(nums, val))

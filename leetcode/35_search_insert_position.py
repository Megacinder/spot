from typing import List


class Solution:
    def search_nsert(self, nums: List[int], target: int) -> int:
        low = 0
        high = len(nums) - 1

        while low <= high:
            mid = (high + low) // 2

            if target == nums[mid]:
                return mid

            if target < nums[mid]:
                high = mid - 1
            else:
                low = mid + 1

        return low



nums = [1,3,5,6]
target = 5
a = Solution()
print(a.search_nsert(nums, target))

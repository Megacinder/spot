from typing import List, Tuple


def remove_duplicates(nums: List[int]) -> Tuple[int, List]:
    k = 0
    max_item = nums[-1]
    init_len = len(nums)

    while True:
        if nums[k] == max_item:
            k += 1
            nums[k:] = [-1] * (init_len - k)
            break

        if k == len(nums):
            break

        if nums[k] == nums[k + 1]:
            nums.pop(k)
            continue

        k += 1

    return k  # , nums
    print(nums)


# nums1 = [0, 0, 1, 1, 1, 2, 2, 3, 3, 4]
# nums1 = [0, 1, 1, 1, 1]
# nums1 = [1, 1, 2]
# nums2 = [0, 1, 2, 3, 4, _, _, _, _, _]
a = remove_duplicates(nums1)
print(a)

# Output: 5, nums = [0, 1, 2, 3, 4, _, _, _, _, _]
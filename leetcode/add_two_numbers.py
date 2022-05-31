# Definition for singly-linked list.
from typing import Optional


class ListNode:
    def __init__(self, val=0, next=None):
        self.val = val
        self.next = next


s = ListNode(1, 2)
print(s.val)
'''
class Solution:
    def addTwoNumbers(self, l1: Optional[ListNode], l2: Optional[ListNode]) -> Optional[ListNode]:
        l1_int = ''.join(reversed(l1))
        l2_int = ''.join(reversed(l2))
        l3 = str(l1_int + l2_int)
'''
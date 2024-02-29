from typing import Optional


class ListNode:
    def __init__(self, val=0, next=None):
        self.val = val
        self.next = next


def merge_two_lists(list1: Optional[ListNode], list2: Optional[ListNode]) -> Optional[ListNode]:
    v1 = list1.val
    v2 = list2.val

    if not list1.next and not list2.next:
        c = ListNode(val=v2 if v1 >= v2 else v1, next=list2 if v1 >= v2 else list1)

    return c


l1 = ListNode(
    val=1,
    next=ListNode(
        val=2,
        next=ListNode(
            val=4,
            next=None,
        ),
    ),
)

l2 = ListNode(
    val=1,
    next=ListNode(
        val=3,
        next=ListNode(
            val=4,
            next=None,
        ),
    ),
)

o = merge_two_lists(l1, l2)
print(o)

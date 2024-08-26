from typing import Optional


class ListNode:
    def __init__(self, val=0, next=None):
        self.val = val
        self.next = next


def merge_two_lists(list1: Optional[ListNode], list2: Optional[ListNode]) -> Optional[ListNode]:
    node = ListNode()
    current = node

    while list1 and list2:
        if list1.val < list2.val:
            current.next = list1
            list1 = list1.next
        else:
            current.next = list2
            list2 = list2.next

        current = current.next

    current.next = list1 if list1 else list2

    return node.next


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
    val=3,
    next=ListNode(
        val=5,
        next=ListNode(
            val=6,
            next=None,
        ),
    ),
)



# l1 = ListNode(
#     val=2,
#     next=None,
# )
#
# l2 = ListNode(
#     val=2,
#     next=None,
# )

o = merge_two_lists(l1, l2)
print(o.val)
# while o:
#     print(o.val)
#     o = o.next

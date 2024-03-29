from typing import Optional


class ListNode:
    def __init__(self, val=0, next=None):
        self.val = val
        self.next = next


def merge_two_lists(list1: Optional[ListNode], list2: Optional[ListNode]) -> Optional[ListNode]:
    li1, li2 = list1, list2
    c = ListNode()

    while li1 and li2:
        cond = li1.val > li2.val

        c.next = ListNode(
            val=li2.val if cond else li1.val,
            next=li1 if cond else li2,
        )

        if cond:
            li2 = li2.next
        else:
            li1 = li1.next

        c = c.next

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

while o:
    print(o.val)
    o = o.next

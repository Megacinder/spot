from typing import Optional


class ListNode:
    def __init__(self, val=0, next=None):
        self.val = val
        self.next = next


class Solution:
    def mergeTwoLists(self, list1: list, list2: list) -> list:
        if not list1 and not list2:
            return []
        elif not list1:
            return list2
        elif not list2:
            return list1
        else:
            o_list = []
            i = 0
            j = 0
            while True:
                if i == len(list1) and j == len(list2):
                    break
                if list1[i] == list2[j]:
                    print(list1[i], ' == ', list1[i])
                    o_list.append(list1[i])
                    o_list.append(list2[j])
                    print('f = ', o_list)
                    i += 1
                    j += 1
                elif list1[i] > list2[j]:
                    print(list1[i], ' > ', list2[j])
                    o_list.append(list2[j])
                    print('f = ', o_list)
                    j += 1
                elif list1[i] < list2[j]:
                    print(list1[i], ' < ', list2[j])
                    o_list.append(list1[i])
                    print('f = ', o_list)
                    i += 1
                if i == len(list1):
                    print('list1 ended')
                    o_list.append(list2[j])
                    print('f = ', o_list)
                    j += 1
                elif j == len(list2):
                    print('list2 ended')
                    o_list.append(list1[i])
                    print('f = ', o_list)
                    i += 1
                print('i =', i, 'j =', j)
            return o_list


list1 = []
list2 = [0]

a = Solution()
li = a.mergeTwoLists(list1, list2)
print(li)

li1 = ListNode()
li.val = 1
li.next = 2
print(li.next)

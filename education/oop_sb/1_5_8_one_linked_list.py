class ListObject:
    def __init__(self, data):
        self.data = data
        self.next_obj = None

    def link(self, obj):
        self.next_obj = obj


lst_in = "foo bar baz foo1 bar1 baz1".split()

ls = lst_in[:]
head_obj = ListObject(data=ls.pop(0))
node = head_obj
while ls:
    node.link(ListObject(ls.pop(0)))
    node = node.next_obj

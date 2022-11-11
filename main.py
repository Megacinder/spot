class Vertex:
    IDD = 1

    def __init__(self):
        self._links = []
        self.id = Vertex.IDD
        Vertex.IDD += 1

    @property
    def links(self):
        return self._links

    def add_link(self, link):
        if link not in self._links:
            self._links.append(link)

    def __repr__(self):
        return f'v{str(self.id)}'


class Link:
    def __init__(self, v1: Vertex, v2: Vertex, dist=1):
        self._v1 = v1
        self._v2 = v2
        self._v1.add_link(self)
        self._v2.add_link(self)
        self._dist = dist

    @property
    def v1(self):
        return self._v1

    @property
    def v2(self):
        return self._v2

    @property
    def dist(self):
        return self._dist

    @dist.setter
    def dist(self, value):
        self._dist = value

    def __eq__(self, other):
        v1_v2 = self._v1 == other.v1 and self._v2 == other.v2
        v2_v1 = self._v2 == other.v1 and self._v1 == other.v2
        if isinstance(other, Link) and (v1_v2 or v2_v1):
            return True
        return False

    def __repr__(self):
        return f'{self._v1}-{self._v2}'


class LinkedGraph:
    def __init__(self):
        self._links = []
        self._vertex = []

    def add_vertex(self, v):
        if v not in self._vertex:
            self._vertex.append(v)

    def add_link(self, link):
        if link not in self._links:
            self._links.append(link)
            for i in (link.v1, link.v2):
                self.add_vertex(i)

    def find_path(self, start_v, stop_v):
        if start_v not in self._vertex or stop_v not in self._vertex:
            raise AttributeError('Not all vertexes in the list')

        dist = 0
        paths = []
        for link in self._links:
            if start_v == link.v1 and stop_v == link.v2 or start_v == link.v2 and stop_v == link.v1:
                return link.dist

        i = 0
        j = 1
        path = []
        while i < len(self._links):
            v1 = self._links[i].v1
            if v1 == start_v:
                path.append(v1)
                while j < len(self._links):
                    v2 = self._links[j].v1
                    if v2 == stop_v:
                        a = 1

            v2 = self._links[i].v2
            if v1 == start_v:
                x = self._links[i].v2

        return dist

    def show_tree(self, start_v, stop_v):
        if start_v == stop_v:
            return
        tree = {k: [] for k in self._vertex}
        for link in self._links:
            tree[link.v1].append(link.v2)
        for k, v in tree.items():
            print(k, ': ', v)

        queue = tree[start_v]
        while queue:
            print('queue = ', queue)
            curr_v = queue.pop()

            print('curr_v = ', curr_v)
            if stop_v == curr_v:
                print('stop_v = ', stop_v, ', curr_v = ', curr_v)
                return
            if not queue:
                queue += tree[curr_v]

        if not queue:
            return


map_graph = LinkedGraph()

v1 = Vertex()
v2 = Vertex()
v3 = Vertex()
v4 = Vertex()
v5 = Vertex()
v6 = Vertex()
v7 = Vertex()
v8 = Vertex()

map_graph.add_link(Link(v1, v2))
map_graph.add_link(Link(v2, v1))

map_graph.add_link(Link(v2, v3))
map_graph.add_link(Link(v1, v3))

map_graph.add_link(Link(v4, v5))
map_graph.add_link(Link(v6, v7))

map_graph.add_link(Link(v2, v7))
map_graph.add_link(Link(v3, v4))
map_graph.add_link(Link(v5, v6))

print(len(map_graph._links))   # 8 связей
print(len(map_graph._vertex))  # 7 вершин
path = map_graph.find_path(v4, v5)
map_graph.show_tree(v1, v8)

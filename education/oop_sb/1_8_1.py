class Data:
    def __init__(self, data, ip):
        self.data = data
        self.ip = ip


class Server:
    last_ip = 0

    def __init__(self):
        self.ip = self.add_ip()
        self.buffer = list()

    @classmethod
    def add_ip(cls):
        cls.last_ip += 1
        return cls.last_ip

    @staticmethod
    def send_data(data: Data):
        Router.buffer.append(data)

    def get_data(self):
        o_buffer = self.buffer
        self.buffer = []
        return o_buffer

    def get_ip(self):
        return self.ip


class Router:
    buffer = []
    linked_servers = []

    def link(self, server):
        self.linked_servers.append(server)

    def unlink(self, server):
        self.linked_servers.remove(server)

    @classmethod
    def send_to_server(cls, data: Data):
        for i in cls.linked_servers:
            if i.get_ip() == data.ip:
                i.buffer.append(data)

    @classmethod
    def send_data(cls):
        for i in cls.buffer:
            cls.send_to_server(i)
        cls.buffer = []


router = Router()
sv_from = Server()
sv_from2 = Server()
router.link(sv_from)
router.link(sv_from2)
router.link(Server())
router.link(Server())
sv_to = Server()
router.link(sv_to)
sv_from.send_data(Data("Hello", sv_to.get_ip()))
sv_from2.send_data(Data("Hello", sv_to.get_ip()))
sv_to.send_data(Data("Hi", sv_from.get_ip()))
router.send_data()
msg_lst_from = sv_from.get_data()
msg_lst_to = sv_to.get_data()


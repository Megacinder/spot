from sshtunnel import SSHTunnelForwarder, create_logger
from src.connection.envs import envs

envs = envs()

HOST = envs['VERTICA_HOST']
PORT = int(envs['VERTICA_PORT'])

tunnel_conn = {
    'ssh_address_or_host': (envs['SSH_HOST'], int(envs['SSH_PORT'])),
    'ssh_username': envs['SSH_USERNAME'],
    'ssh_pkey': envs['SSH_PKEY'],
    'remote_bind_address': (HOST, PORT),
    'local_bind_address': (HOST, PORT),
    'logger': create_logger(loglevel=1),
}


def ssh_tunnel():
    return SSHTunnelForwarder(**tunnel_conn)


if __name__ == "__main__":
    print(ssh_tunnel())

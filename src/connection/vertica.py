from logging import DEBUG
from vertica_python import connect
from sshtunnel import SSHTunnelForwarder, create_logger

from src.connection.envs import envs

envs = envs()

HOST = envs['VERTICA_HOST']
PORT = int(envs['VERTICA_PORT'])
TUNNEL_CONN = {
    'ssh_address_or_host': (envs['SSH_HOST'], int(envs['SSH_PORT'])),
    'ssh_username': envs['SSH_USERNAME'],
    'ssh_pkey': envs['SSH_PKEY'],
    'remote_bind_address': (HOST, PORT),
    'local_bind_address': (HOST, PORT),
    'logger': create_logger(loglevel=1),
}
DB_CONN = {
    'host': envs['VERTICA_HOST'],
    'port': envs['VERTICA_PORT'],
    'user': envs['VERTICA_DB_USER'],
    'password': envs['VERTICA_DB_PASSWORD'],
    'database': envs['VERTICA_DB_NAME'],
    'log_level': DEBUG,
    'log_path': envs['PROJECT_PATH'] + '/logs/vertica_conn.log',
}


class VerticaCursor:
    def __init__(self, db_conn_params: dict = None):
        self.db_conn_params = db_conn_params or DB_CONN
        self.ssh_tunnel = SSHTunnelForwarder(**TUNNEL_CONN)
        self.conn = None

    def __enter__(self):
        self.ssh_tunnel.start()
        self.conn = connect(**self.db_conn_params)
        return self.conn.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()
        self.ssh_tunnel.stop()


if __name__ == "__main__":
    with VerticaCursor() as cur:
        print(cur.execute("select 1").fetchall())

from logging import DEBUG
from vertica_python import connect

from .envs import envs
from .ssh_tunnel import ssh_tunnel

envs = envs()

DB_CONN = {
    'host': envs['HOST'],
    'port': envs['PORT'],
    'user': envs['DB_USER'],
    'password': envs['DB_PASSWORD'],
    'database': envs['DB_NAME'],
    'log_level': DEBUG,
    'log_path': envs['PROJECT_PATH'] + '/logs/vertica_conn.log',
}


class VerticaCursor:
    def __init__(self, ssh=ssh_tunnel(), db_conn_params=None):
        self.ssh = ssh
        self.db_conn_params = db_conn_params or DB_CONN
        self.conn = None

    def __enter__(self):
        self.ssh.start()
        self.conn = connect(**self.db_conn_params)
        return self.conn.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()
        self.ssh.stop()


if __name__ == "__main__":
    with VerticaCursor() as cur:
        print(cur.execute("select 1").fetchall())

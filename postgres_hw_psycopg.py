import psycopg2 as pg
from sshtunnel import SSHTunnelForwarder
import json


class DBConnector:
    def __init__(self, sens_file: str, host: str, database_port: int,
                 ssh_user: str, ssh_port: int, database_user: str, database: str):
        self.sens_file = sens_file
        self.host = host
        self.database_port = database_port
        self.ssh_user = ssh_user
        self.ssh_port = ssh_port
        self.database_user = database_user
        self.database = database
        self.conn = None
        self.cursor = None

    def connection(self):
        with open(self.sens_file) as f:
            file = json.loads(f.read())
            aws_dns = file['aws_dns']
            postgres_password = file['password']

        with (SSHTunnelForwarder(
            (aws_dns, self.ssh_port),
            ssh_host_key=None,
            ssh_username=self.ssh_user,
            ssh_password=None,
            ssh_pkey='/Users/eugene_ivanov/AWS/EC2/linux_server/eug_linux_server_key.pem',
            remote_bind_address=(self.host, self.database_port))
                as ec2_tunnel):
            conn = pg.connect(
                host=self.host,
                port=ec2_tunnel.local_bind_port,
                user=self.database_user,
                database=self.database,
                password=postgres_password)

            self.conn = conn
            self.cursor = conn.cursor()

    def get_cursor(self):
        if not self.cursor:
            self.connection()
        return self.cursor

class CustomerDB:
    def __init__(self, db_connector: DBConnector):
        self.conn = db_connector
    def create_table(self):
        with self.conn.get_cursor() as cur:
            print(cur)
            cur.execute("CREATE TABLE test_table(id SERIAL PRIMARY KEY, name VARCHAR(40));")
            cur.commit()


connection1 = DBConnector('sens.txt', 'localhost', 5432, 'ubuntu',
                          22, 'postgres', 'netology_psycopg2')
db_run = CustomerDB(connection1)
db_run.create_table()

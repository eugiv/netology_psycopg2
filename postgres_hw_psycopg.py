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

    def connection(self):
        with open(self.sens_file) as f:
            file = json.loads(f.read())
            aws_dns = file['aws_dns']
            postgres_password = file['password']

        ec2_tunnel = SSHTunnelForwarder(
            (aws_dns, self.ssh_port),
            ssh_host_key=None,
            ssh_username=self.ssh_user,
            ssh_password=None,
            ssh_pkey='/Users/eugene_ivanov/AWS/EC2/linux_server/eug_linux_server_key.pem',
            remote_bind_address=(self.host, self.database_port))

        ec2_tunnel.start()

        conn = pg.connect(
            host=self.host,
            port=ec2_tunnel.local_bind_port,
            user=self.database_user,
            database=self.database,
            password=postgres_password)

        return conn


class CustomerDB:
    def __init__(self, db_connector: DBConnector):
        self.connector = db_connector
        self.connection = None

    def get_connection(self):
        if self.connection is None or self.connection.closed !=0:
            self.connection = self.connector.connection()
        return self.connection

    def create_table(self):
        conn = self.get_connection()

        with conn.cursor() as cur:
            cur.execute('DROP TABLE test_table;')
            conn.commit()


connection1 = DBConnector('sens.txt', 'localhost', 5432, 'ubuntu',
                          22, 'postgres', 'netology_psycopg2')
db_run = CustomerDB(connection1)
db_run.create_table()

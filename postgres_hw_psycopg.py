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
    def __init__(self, db_connector: DBConnector, customer_data, phone_data, change_data):
        self.connector = db_connector
        self.customer_data = customer_data
        self.phone_data = phone_data
        self.change_data = change_data
        self.connection = None

    def get_connection(self):
        if self.connection is None or self.connection.closed != 0:
            self.connection = self.connector.connection()
        return self.connection

    def create_db(self):
        conn = self.get_connection()

        with conn.cursor() as cur:
            cur.execute('DROP TABLE phones;')
            cur.execute('DROP TABLE customers;')
            cur.execute('CREATE TABLE IF NOT EXISTS customers '
                        '(id SERIAL PRIMARY KEY,'
                        'first_name VARCHAR(60) NOT NULL,'
                        'last_name VARCHAR(100) NOT NULL,'
                        'email VARCHAR(100) NOT NULL UNIQUE);')

            cur.execute('CREATE TABLE IF NOT EXISTS phones '
                        '(phone_id SERIAL PRIMARY KEY,'
                        'cell_phone VARCHAR(30) UNIQUE,'
                        'customer_id INTEGER NOT NULL REFERENCES customers(id));')

            conn.commit()

    def db_interaction(self, query, data):
        conn = self.get_connection()
        with conn.cursor() as cur:
            cur.executemany(query, data)
            conn.commit()

    def add_customer(self):
        query = 'INSERT INTO customers(first_name, last_name, email) VALUES (%s, %s, %s);'
        self.db_interaction(query, self.customer_data)

    def add_phone(self):
        if self.phone_data:
            query = 'INSERT INTO phones(cell_phone, customer_id) VALUES (%s, %s);'
            self.db_interaction(query, self.phone_data)

    def change_customer(self):
        queries = ['UPDATE customers SET first_name=%s, last_name=%s, email=%s WHERE id= %s;',
                   'UPDATE phones SET cell_phone=%s WHERE phone_id= %s;']
        if self.change_data:
            counter = 0
            for query in queries:
                try:
                    self.db_interaction(query, [x[counter] for x in self.change_data])
                    counter += 1
                except Exception as e:
                    pass



connection1 = DBConnector('sens.txt', 'localhost', 5432, 'ubuntu',
                          22, 'postgres', 'netology_psycopg2')

cust_data = [('Ivan', 'Ivanov', 'ivan@ivanov.com'), ('Petr', 'Petrov', 'petr@petrov.com'),
             ('John', 'Wayne', 'john@wayne.com')]
phn_data = [('12-34', 1), ('23-45', 1), ('67-89', 3)]

data_to_change = [[('Semen', 'Kotov', 'semen@kotov.com', 1), ('88-88', 1)]]
db_run = CustomerDB(connection1, cust_data, phn_data, data_to_change)

db_run.create_db()
db_run.add_customer()
db_run.add_phone()
db_run.change_customer()

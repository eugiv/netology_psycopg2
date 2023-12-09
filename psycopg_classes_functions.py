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
    def __init__(self, db_connector: DBConnector, customer_data: list, phone_data: list, change_data: list,
                 phone_delete: list, customer_delete: list, customer_find: list):
        self.connector = db_connector
        self.customer_data = customer_data
        self.phone_data = phone_data
        self.change_data = change_data
        self.phone_delete = phone_delete
        self.customer_delete = customer_delete
        self.customer_find = customer_find
        self.connection = None

    def get_connection(self):
        if self.connection is None or self.connection.closed != 0:
            self.connection = self.connector.connection()
        return self.connection

    def create_db(self):
        conn = self.get_connection()

        with conn.cursor() as cur:
            # cur.execute('DROP TABLE phones;')
            # cur.execute('DROP TABLE customers;')
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

    def db_interaction(self, query, data=None):
        conn = self.get_connection()
        with conn.cursor() as cur:
            if 'select' not in query.lower():
                try:
                    cur.executemany(query, data)
                    conn.commit()
                except Exception as e:
                    print(e)
                    conn.commit()
            else:
                cur.execute(query, data)
                return cur.fetchall()

    def add_customer(self):
        if self.customer_data:
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
                except:
                    pass

    def delete_phone(self):
        if self.phone_delete:
            query = 'DELETE FROM phones WHERE phone_id=%s;'
            self.db_interaction(query, self.phone_delete)

    def delete_customer(self):
        if self.customer_delete:
            query = 'DELETE FROM customers WHERE id=%s;'
            self.db_interaction(query, self.customer_delete)

    def find_customer(self):
        if self.customer_find:
            fetched_data_lst = []
            for req in self.customer_find:
                query = ('SELECT first_name, last_name, email, cell_phone FROM customers c '
                         'LEFT JOIN phones p ON c.id=p.customer_id '
                         'WHERE first_name=%s OR last_name=%s OR email=%s OR cell_phone=%s;')
                fetched_data = self.db_interaction(query, (req, req, req, req))
                fetched_data_lst.extend(fetched_data)

            fetched_data_lst = [', '.join(str(item) for item in tpl) for tpl in fetched_data_lst]
            print('Your search request has been fulfilled: ', fetched_data_lst)
            return fetched_data_lst

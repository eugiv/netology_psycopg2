from psycopg_classes_functions import DBConnector, CustomerDB

# -- raw data --
cust_data = [('Ivan', 'Ivanov', 'ivan@ivanov.com'), ('Petr', 'Petrov', 'petr@petrov.com'),
             ('John', 'Wayne', 'john@wayne.com')]
phn_data = [('12-34', 1), ('23-45', 1), ('67-89', 3)]
data_to_change = [[('Semen', 'Kotov', None, 1), ('88-88', 1)]]
phn_del = ['3']
cust_del = []
cust_find = ['Semen', 'john@wayne.com']

# --class instances creation--
create_connection = DBConnector('sens.txt', 'localhost', 5432, 'ubuntu', 22,
                                'postgres', 'netology_psycopg2')
db_run = CustomerDB(create_connection, cust_data, phn_data, data_to_change, phn_del, cust_del, cust_find)

# -- class methods calling --
db_run.create_db()  # func 1
db_run.add_customer()  # func 2
db_run.add_phone()  # func 3
db_run.change_customer()  # func 4
db_run.delete_phone()  # func 5
db_run.delete_customer()  # func 6
db_run.find_customer()  # func 7

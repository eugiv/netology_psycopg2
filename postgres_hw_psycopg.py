import psycopg2 as pg
from sshtunnel import SSHTunnelForwarder
import json

with open('sens.txt') as f:
    file = json.loads(f.read())
    aws_dns = file['aws_dns']
    postgres_password = file['password']

with (SSHTunnelForwarder(
    (aws_dns, 22),
    ssh_host_key=None,
    ssh_username='ubuntu',
    ssh_password=None,
    ssh_pkey='/Users/eugene_ivanov/AWS/EC2/linux_server/eug_linux_server_key.pem',
    remote_bind_address=('localhost', 5432))
    as ec2_tunnel):

    conn = pg.connect(
        host='localhost',
        port=ec2_tunnel.local_bind_port,
        user='postgres',
        database='netology_psycopg2',
        password=postgres_password)

# Import required functions

import clickhouse_connect, os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv(override=True)

def connect_clickhouse():
    '''
    This function connects to Click_House and helps create a connection which will be used to extract data
    '''
    host = os.getenv('host')
    username = os.getenv('username')
    password = os.getenv('password')
    port = os.getenv('port')

    client = clickhouse_connect.get_client(host=host, port=port, username=username, password=password)
    return client


def connect_postgresql():
    '''
    This function connects to PostgreSQL DB and helps create a connection which will be used to load data
    '''
    pg_host = os.getenv('hostname')
    pg_username = os.getenv('u_name')
    pg_password = os.getenv('pwd')
    pg_port = os.getenv('dprt')
    pg_dbname = os.getenv('dnme')

    # Create the DB engine
    db_url = f'postgresql+psycopg2://{pg_username}:{pg_password}@{pg_host}:{pg_port}/{pg_dbname}'
    engine = create_engine(db_url)
    return engine



import psycopg2
from decouple import config


class RdsDbConnection:
    def __enter__(self):
        conn = psycopg2.connect(host=config('RDS_DB_HOST'),
                                port=config('RDS_DB_PORT'),
                                database='postgres',
                                user=config('RDS_DB_MASTER_USERNAME'),
                                password=config('RDS_DB_MASTER_PASSWORD'))
        self.conn = conn

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()


def create_table():
    with RdsDbConnection() as rdb:
        cursor = rdb.conn.cursor()
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS tv_shows (
            id SERIAL PRIMARY KEY,
            title TEXT,
            normalized_title TEXT,
            wikipedia_url TEXT,
            wikiquotes_url TEXT,
            metacritic_url TEXT,
            eztv_url TEXT
        )
        '''
        cursor.execute(create_table_query)
        rdb.conn.commit()


def insert_into_db(title, normalized_title, wikipedia_url, wikiquotes_url, metacritic_url, eztv_url):

    with RdsDbConnection() as rdb:
        cursor = rdb.conn.cursor()

        insert_query = '''
        INSERT INTO tv_shows (title, normalized_title, wikipedia_url, wikiquotes_url, metacritic_url, eztv_url)
        VALUES (%s, %s, %s, %s, %s, %s)
        '''
        data_to_insert = (title, normalized_title, wikipedia_url,
                          wikiquotes_url, metacritic_url, eztv_url)

        cursor.execute(insert_query, data_to_insert)
        rdb.conn.commit()


def retrieve_data(statement):
    data = []
    with RdsDbConnection() as rdb:
        cursor = rdb.conn.cursor()

        cursor.execute(statement)
        data = cursor.fetchall()
        rdb.conn.commit()
    return data


def execute_statement(statement):
    with RdsDbConnection() as rdb:
        cursor = rdb.conn.cursor()
        cursor.execute(statement)
        rdb.conn.commit()

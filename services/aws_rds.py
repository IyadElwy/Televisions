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


def copy_from_s3_to_db(bucket_name, file_name):
    with RdsDbConnection() as rdb:
        command = """
            with s3 AS (
                SELECT aws_commons.create_s3_uri(
                    '{bucket}',
                    '{filename}',
                    '{region}'
                ) as uri
            )
            SELECT aws_s3.table_import_from_s3(
                'tv_shows', 'title, normalized_title, wikipedia_url, wikiquotes_url, 
                metacritic_url, eztv_url', '(format csv, DELIMITER '','')',
                (select uri from s3), 
                aws_commons.create_aws_credentials('{access_key}', '{secret_key}', '')
            )
        """
        query = command.format(bucket=bucket_name, filename=file_name,
                               region=config('REGION'), access_key=config('AWS_ACCESS_KEY'),
                               secret_key=config('AWS_SECRET_ACCESS_KEY'))
        cursor = rdb.conn.cursor()
        cursor.execute(query)
        rdb.conn.commit()

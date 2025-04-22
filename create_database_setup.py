import psycopg2
from psycopg2 import sql
from scripts.constants import *

def create_database():
    try:
        # connect to the default 'postgres' database with autocommit enabled
        connection = psycopg2.connect(
            dbname="postgres",
            user=POSTGRES_USER,   
            password=POSTGRES_PASS,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )
        connection.autocommit = True
        cursor = connection.cursor()

        # check if the database exists
        cursor.execute(f"SELECT * FROM pg_database WHERE datname = '{DB_NAME}'")
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(sql.SQL(f"CREATE DATABASE {DB_NAME}"))
            print(f"Created database {DB_NAME}")
        else:
            print(f"{DB_NAME} already exists!")

        cursor.close()
        connection.close()

    except psycopg2.Error as e:
        print("An error occurred:", e)

if __name__ == '__main__':
    create_database()

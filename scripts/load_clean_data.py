import pandas as pd
import psycopg2
from scripts.constants import *
from scripts import common_functions

def load_clean_data(df: pd.DataFrame):

    try:
        # connect to the employee database and create a cursor
        connection = psycopg2.connect(dbname=DB_NAME, 
                                user=POSTGRES_USER, 
                                password=POSTGRES_PASS, 
                                host=POSTGRES_HOST, 
                                port=POSTGRES_PORT)
        connection.autocommit = True
        cursor = connection.cursor()

        # create the table for our clean data
        create_table_statement = f'''
        CREATE TABLE IF NOT EXISTS {CLEAN_DATA_TABLE_NAME} (
            pokedex_number INT PRIMARY KEY,
            name VARCHAR(500) NOT NULL,
            japanese_name VARCHAR(500) NOT NULL,
            classification VARCHAR(500) NOT NULL,
            type1 VARCHAR(500) NOT NULL,
            type2 VARCHAR(500) NOT NULL,
            height_m REAL NOT NULL,
            weight_kg REAL NOT NULL,
            hp INT NOT NULL,
            attack INT NOT NULL,
            defense INT NOT NULL,
            speed INT NOT NULL,
            sp_attack INT NOT NULL,
            sp_defense INT NOT NULL,
            base_egg_stesp INT NOT NULL,
            base_happiness INT NOT NULL,
            base_total INT NOT NULL,
            capture_rate INT NOT NULL,
            percentage_male REAL NOT NULL,
            generation INT NOT NULL,
            is_legendary BOOLEAN NOT NULL
        );
        '''
        cursor.execute(create_table_statement)
        print(f"Table {CLEAN_DATA_TABLE_NAME} is created.")

        # insert our clean data into the table
        engine = common_functions.create_sqlalchemy_engine()
        df.to_sql(CLEAN_DATA_TABLE_NAME, engine, if_exists="replace", index=False)

        print("Clean data has been inserted into the clean data table!")

        # create a .csv file with our clean data
        df.to_csv(CLEAN_DATA_FILEPATH, index=False)
        print(f"Clean data has been saved to {CLEAN_DATA_FILEPATH}")

    except psycopg2.Error as e:
        print("An error occurred:", e)
import psycopg2
from psycopg2 import sql
import pandas as pd
from scripts.constants import *
from scripts.common_functions import create_sqlalchemy_engine
import kagglehub
from kagglehub import KaggleDatasetAdapter

def load_raw_data():
    try:

        # create a SQLAlchemy engine to execute our queries
        engine = create_sqlalchemy_engine()
        connection = engine.raw_connection()
        connection.autocommit = True
        cursor = connection.cursor()

        # create the table for our raw data
        # using VARCHAR for all data to preserve raw data with its imperfections
        create_table_statement = f'''
        CREATE TABLE IF NOT EXISTS {RAW_DATA_TABLE_NAME} (
            abilities VARCHAR(5000),
            against_bug VARCHAR(500),
            against_dark VARCHAR(500),
            against_dragon VARCHAR(500),
            against_electric VARCHAR(500),
            against_fairy VARCHAR(500),
            against_fight VARCHAR(500),
            against_fire VARCHAR(500),
            against_flying VARCHAR(500),
            against_ghost VARCHAR(500),
            against_grass VARCHAR(500),
            against_ground VARCHAR(500),
            against_ice VARCHAR(500),
            against_normal VARCHAR(500),
            against_poison VARCHAR(500),
            against_psychic VARCHAR(500),
            against_rock VARCHAR(500),
            against_steel VARCHAR(500),
            against_water VARCHAR(500),
            attack VARCHAR(500),
            base_egg_steps VARCHAR(500),
            base_happiness VARCHAR(500),
            base_total VARCHAR(500),
            capture_rate VARCHAR(500),
            classfication VARCHAR(500),
            defense VARCHAR(500),
            experience_growth VARCHAR(500),
            height_m VARCHAR(500),
            hp VARCHAR(500),
            japanese_name VARCHAR(500),
            name VARCHAR(500),
            percentage_male VARCHAR(500),
            pokedex_number VARCHAR(500),
            sp_attack VARCHAR(500),
            sp_defense VARCHAR(500),
            speed VARCHAR(500),
            type1 VARCHAR(500),
            type2 VARCHAR(500),
            weight_kg VARCHAR(500),
            generation VARCHAR(500),
            is_legendary VARCHAR(500)
        );
        '''

        cursor.execute(create_table_statement)
        connection.commit()
        print(f"Table {RAW_DATA_TABLE_NAME} is created.")

        # load the raw data into the raw data table
        df: pd.DataFrame        
        df = kagglehub.load_dataset(KaggleDatasetAdapter.PANDAS, "rounakbanik/pokemon", RAW_DATA_FILEPATH)

        df.to_sql(RAW_DATA_TABLE_NAME, engine, if_exists='replace', index=False)
        print("Raw data has been inserted into the raw data table.")


    except psycopg2.Error as e:
        print("An error occurred:", e)

load_raw_data()





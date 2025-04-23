from scripts import common_functions
from scripts.constants import *
import pandas as pd
import subprocess

TYPE1_GROUP = ['flying', 'poison', 'ground', 'fairy', 'psychic', 'fighting', 'steel',
              'dark', 'grass', 'water', 'dragon', 'ice', 'rock', 'ghost', 'fire', 'electric',
              'bug', 'normal']

TYPE2_GROUP = ['flying', 'poison', 'ground', 'fairy', 'psychic', 'fighting', 'steel',
              'dark', 'grass', 'water', 'dragon', 'ice', 'rock', 'ghost', 'fire', 'electric',
              'bug', 'normal', None]

CAPTURE_RATE_CORRECTIONS = {
    '30 (Meteorite)255 (Core)': '30',
}

def fetch_pokemon_height(pokedex_number: str) -> str:
    result = subprocess.run(["python3.11", "pokeapi_utils.py", "height", pokedex_number], capture_output=True, text=True)
    return str(result.stdout.strip())

def fetch_pokemon_weight(pokedex_number: str) -> str:
    result = subprocess.run(["python3.11", "pokeapi_utils.py", "weight", pokedex_number], capture_output=True, text=True)
    return str(result.stdout.strip())


def manual_column_correct(df: pd.DataFrame, column_name: str, group: list[str]):
    print(f'\n ----- Manual column correct for column {column_name} -----')

    for index, row in df.iterrows():
        current_value = row[column_name]

        new_value = ""
        if current_value not in group:
            print(f"\nFound entry {current_value} that does not belong in group.")
            print(f"Group is: {group}")
            new_value = input("Enter new value: ").strip()

        if new_value:
            df.at[index, column_name] = new_value
    
    print(f"All entries in {column_name} have been checked for conformity to group.")

def clean_data() -> pd.DataFrame:

    # pull the raw data from postgres
    engine = common_functions.create_sqlalchemy_engine()
    df = pd.read_sql(f'SELECT * FROM {RAW_DATA_TABLE_NAME}', engine)

    # 0. fix a typo column name
    df.rename(columns={'classfication': 'classification'}, inplace=True)

    # 1. fix non-compliant entries in the capture_rate column
    df['capture_rate'] = df['capture_rate'].replace(CAPTURE_RATE_CORRECTIONS)

    # 2. supplement missing height values with data from PokeAPI
    df['height_m'] = df.apply(
        lambda row: fetch_pokemon_height(str(row['pokedex_number'])) if pd.isna(row['height_m']) else row['height_m'],
        axis=1
    )

    # 3. supplement missing percentage_male values with average for that column
    average_male = round(df['percentage_male'].astype(float).mean(), 1)
    df['percentage_male'] = df['percentage_male'].fillna(str(average_male))

    # 4. supplement missing weight values with data from PokeAPI
    df['weight_kg'] = df.apply(
        lambda row: fetch_pokemon_weight(str(row['pokedex_number'])) if pd.isna(row['weight_kg']) else row['weight_kg'],
        axis=1
    )

    # 5. manually correct any incorrect type1 values
    manual_column_correct(df, 'type1', TYPE1_GROUP)

    # 6. manaully correct any incorrect type2 values
    manual_column_correct(df, 'type2', TYPE2_GROUP)

    # 7. confirm all types are correct
    column_type_mapping = {
        'attack': int,
        'base_egg_steps': int,
        'base_happiness': int,
        'base_total': int,
        'capture_rate': int,
        'classification': str,
        'defense': int,
        'height_m': float,
        'hp': int,
        'japanese_name': str,
        'name': str,
        'percentage_male': float,
        'pokedex_number': int,
        'sp_attack': int,
        'sp_defense': int,
        'speed': int,
        'type1': str,
        'type2': str,
        'weight_kg': float,
        'generation': int,
        'is_legendary': int
    }

    for column in column_type_mapping:
        df[column] = df[column].astype(column_type_mapping[column])

    # 8. convert is_legendary to booleans
    df['is_legendary'] = df['is_legendary'].astype(bool)

    # 9. drop columns we don't care about
    cols_to_drop = ['abilities','against_bug','against_dark','against_dragon','against_electric',
                    'against_fairy','against_fight','against_fire','against_flying','against_ghost',
                    'against_grass','against_ground','against_ice','against_normal','against_poison',
                    'against_psychic','against_rock','against_steel','against_water'] 

    df.drop(cols_to_drop, axis=1, inplace=True)

    # 10. reorder columns to a more friendly order
    new_col_order = ['pokedex_number', 'name', 'japanese_name', 'classification', 'type1', 'type2', 'height_m', 
                     'weight_kg', 'hp', 'attack', 'defense', 'speed', 'sp_attack', 'sp_defense', 'base_egg_steps',
                     'base_happiness', 'base_total', 'capture_rate', 'percentage_male', 'generation', 'is_legendary']
    assert len(new_col_order) == len(column_type_mapping)

    df = df[new_col_order]

    # 11. reorder the rows by pokedex_number
    df.sort_values(by='pokedex_number', ascending=True, inplace=True)

    return df
import pandas as pd
import re
from scripts import common_functions
from scripts.constants import *

def profile_column(df: pd.DataFrame,
                   column_name: str, 
                   data_format_regex: str=None, 
                   check_for_groups: bool=False,
                   check_for_range: list[int]=None,
                   allow_duplicates=True,
                   allow_blanks=True
                   ):
    
    print(f"\n----- Profiling column {column_name} -----")

    column = df[column_name]

    if data_format_regex is not None:
        bad_format = [entry for entry in column.to_list() if not re.match(data_format_regex, str(entry))]

        if len(bad_format) != 0:
            print(f'* Found {len(bad_format)} entries that do not conform to regex {data_format_regex}:')
            print(bad_format)
        else: 
            print(f'* No format issues, all entries conform to regex {data_format_regex}')

        print()

    if check_for_groups:
        groups = column.value_counts()

        print(f'* Found {len(groups)} groups in column {column_name}:')
        print(groups)
        print()

    if check_for_range is not None:

        assert len(check_for_range) == 2
        min_val = check_for_range[0]
        max_val = check_for_range[1]

        out_of_range = []
        for entry in column.to_list():

            try:
                if not min_val <= float(entry) <= max_val:
                    out_of_range.append(entry)
            except ValueError:
                out_of_range.append(entry)

        if len(out_of_range) != 0:
            print(f'* Found {len(out_of_range)} entries that do not fall within range {check_for_range}:')
            print(out_of_range)
        else: 
            print(f'* No range issues, all elements fall within range {check_for_range}')

        print()

    if not allow_duplicates:
        num_duplicates = column.duplicated().sum()

        print(f'* Found {num_duplicates} duplicate entries for column {column_name}')
        print()

    if not allow_blanks:
        num_empty = column.astype(str).str.strip().eq('').sum()
        num_missing = column.isna().sum()

        print(f'* Found {num_empty + num_missing} empty or missing entries for column {column_name}')
        print()

def pokemon_data_profile(df=None, classification_col_name='classfication'):

    if df is None:
        # pull the raw data from postgres
        engine = common_functions.create_sqlalchemy_engine()
        df = pd.read_sql(f'SELECT * FROM {RAW_DATA_TABLE_NAME}', engine)

    # not interested in abilities or against_X columns, so won't profile them

    # checks for attack:
    # 1. No blanks
    # 2. All positive integers
    profile_column(df, 'attack', data_format_regex="^[0-9]+$", allow_blanks=False)
    print()

    # checks for base_egg_steps:
    # 1. No blanks
    # 2. All positive integers
    profile_column(df, 'base_egg_steps', data_format_regex="^[0-9]+$", allow_blanks=False)
    print()

    # checks for base_happiness:
    # 1. No blanks
    # 2. All positive integers
    profile_column(df, 'base_happiness', data_format_regex="^[0-9]+$", allow_blanks=False)
    print()

    # checks for base_total:
    # 1. No blanks
    # 2. All positive integers
    profile_column(df, 'base_total', data_format_regex="^[0-9]+$", allow_blanks=False)
    print()

    # checks for capture_rate:
    # 1. No blanks
    # 2. All positive integers
    # 3. All fall within [0, 255]
    profile_column(df, 'capture_rate', data_format_regex="^[0-9]+$", check_for_range=[0, 255], allow_blanks=False)
    print()

    # checks for classification:
    # 1. No blanks
    profile_column(df, classification_col_name, allow_blanks=False)
    print()

    # checks for defense:
    # 1. No blanks
    # 2. All positive integers
    profile_column(df, 'defense', data_format_regex="^[0-9]+$", allow_blanks=False)
    print()

    # checks for height_m:
    # 1. No blanks
    # 2. All positive floats
    profile_column(df, 'height_m', data_format_regex="^\d+(\.\d+)?$", allow_blanks=False)
    print()

    # checks for hp:
    # 1. No blanks
    # 2. All positive integers
    profile_column(df, 'hp', data_format_regex="^[0-9]+$", allow_blanks=False)
    print()

    # checks for japanese_name:
    # 1. No blanks
    # 2. All unique
    profile_column(df, 'japanese_name', allow_duplicates=False, allow_blanks=False)
    print()

    # checks for name:
    # 1. No blanks
    # 2. All unique
    profile_column(df, 'name', allow_duplicates=False, allow_blanks=False)
    print()

    # checks for percentage_male:
    # 1. No blanks
    # 2. All positive floats
    # 3. All fall within [0, 100]
    profile_column(df, 'percentage_male', data_format_regex="^\d+(\.\d+)?$", check_for_range=[0,100], allow_blanks=False)
    print()

    # checks for pokedex_number:
    # 1. No blanks
    # 2. All positive integers
    # 3. All unique
    # 4. All within [1, 1025]
    profile_column(df, 'pokedex_number', data_format_regex="^[0-9]+$", check_for_range=[1,1025], allow_duplicates=False, allow_blanks=False)
    print()

    # checks for sp_attack:
    # 1. No blanks
    # 2. All positive integers
    profile_column(df, 'sp_attack', data_format_regex="^[0-9]+$", allow_blanks=False)
    print()

    # checks for sp_defense:
    # 1. No blanks
    # 2. All positive integers
    profile_column(df, 'sp_defense', data_format_regex="^[0-9]+$", allow_blanks=False)
    print()

    # checks for speed:
    # 1. No blanks
    # 2. All positive integers
    profile_column(df, 'speed', data_format_regex="^[0-9]+$", allow_blanks=False)
    print()

    # checks for type1:
    # 1. No blanks
    # 2. Check for groups
    profile_column(df, 'type1', check_for_groups=True, allow_blanks=False)
    print()

    # checks for type2:
    # 1. Check for groups
    profile_column(df, 'type2', check_for_groups=True)
    print()

    # checks for weight_kg:
    # 1. No blanks
    # 2. All positive floats
    profile_column(df, 'weight_kg', data_format_regex="^\d+(\.\d+)?$", allow_blanks=False)
    print()

    # checks for generation:
    # 1. No blanks
    # 2. All positive integers
    # 3. All values between 1 and 9
    profile_column(df, 'generation', data_format_regex="^[0-9]+$", check_for_range=[1, 9], allow_blanks=False)
    print()

    # checks for is_legendary:
    # 1. No blanks
    # 2. All positive integers
    # 3. All values between 0 and 1
    profile_column(df, 'is_legendary', data_format_regex="[0-1]|True|False", allow_blanks=False)
    print()


if __name__ == '__main__':
    pokemon_data_profile()

from scripts.load_raw_data import load_raw_data
from scripts.data_profiling import pokemon_data_profile
from scripts.pokeapi_utils import get_pokemon_data
from scripts.data_cleaning import clean_data
from scripts.load_clean_data import load_clean_data

load_clean_data(clean_data())

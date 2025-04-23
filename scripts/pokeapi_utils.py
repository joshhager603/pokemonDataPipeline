import requests

BASE_POKEAPI_URL = 'https://pokeapi.co/api/v2/pokemon'

def get_pokemon_data(pokedex_number: int):

    url = f'{BASE_POKEAPI_URL}/{pokedex_number}'

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        return None
    
def get_pokemon_height(pokedex_number: int):
    pokemon_data = get_pokemon_data(pokedex_number)
    
    if pokemon_data is not None and 'height' in pokemon_data:
        return pokemon_data['height'] / 10
    else:
        return None
    
def get_pokemon_weight(pokedex_number: int):
    pokemon_data = get_pokemon_data(pokedex_number)

    if pokemon_data is not None and 'weight' in pokemon_data:
        return pokemon_data['weight'] / 10
    else:
        return None
    
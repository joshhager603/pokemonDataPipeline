import argparse
import requests

BASE_POKEAPI_URL = 'https://pokeapi.co/api/v2/pokemon'

def get_pokemon_data(pokedex_number: int):

    url = f'{BASE_POKEAPI_URL}/{pokedex_number}'

    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    return None

def get_pokemon_height(pokedex_number: int):

    pokemon_data = get_pokemon_data(pokedex_number)

    if pokemon_data and 'height' in pokemon_data:
        return_val = pokemon_data['height'] / 10
        print(return_val)
        return return_val
    
    print()
    return None

def get_pokemon_weight(pokedex_number: int):

    pokemon_data = get_pokemon_data(pokedex_number)

    if pokemon_data and 'weight' in pokemon_data:
        return_val = pokemon_data['weight'] / 10
        print(return_val)
        return return_val
    
    print()
    return None

def main():
    parser = argparse.ArgumentParser(description="Fetch Pokemon height or weight by Pokédex number.")
    parser.add_argument('stat', choices=['height', 'weight'], help="Which stat to fetch")
    parser.add_argument('pokedex_number', type=int, help="Pokédex number of the Pokémon")

    args = parser.parse_args()

    if args.stat == 'height':
        get_pokemon_height(args.pokedex_number)
    else:
        get_pokemon_weight(args.pokedex_number)

if __name__ == '__main__':
    main()

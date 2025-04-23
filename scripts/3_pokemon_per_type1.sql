DROP TABLE IF EXISTS pokemon_per_type1;

SELECT pokemon_data.type1, COUNT(pokemon_data.pokedex_number) AS num_pokemon
INTO pokemon_per_type1
FROM pokemon_data
GROUP BY pokemon_data.type1;
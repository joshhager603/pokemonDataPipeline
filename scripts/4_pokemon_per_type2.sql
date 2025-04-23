DROP TABLE IF EXISTS pokemon_per_type2;

SELECT pokemon_data.type2, COUNT(pokemon_data.pokedex_number) AS num_pokemon
INTO pokemon_per_type2
FROM pokemon_data
GROUP BY pokemon_data.type2;
DROP TABLE IF EXISTS bst_by_generation_analysis;

SELECT pokemon_data.generation, AVG(pokemon_data.base_total) AS average_bst
INTO bst_by_generation_analysis
FROM pokemon_data
GROUP BY pokemon_data.generation
ORDER BY pokemon_data.generation ASC;
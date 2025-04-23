DROP TABLE IF EXISTS bst_by_type1_analysis;

SELECT pokemon_data.type1, AVG(pokemon_data.base_total) AS average_bst
INTO bst_by_type1_analysis
FROM pokemon_data
GROUP BY pokemon_data.type1;
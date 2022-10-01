SELECT COUNT(*) AS 'the number of Pokémon caught by each type'
FROM Pokemon AS P
RIGHT JOIN CatchedPokemon AS C
ON P.id = C.pid
GROUP BY P.type
ORDER BY P.type;

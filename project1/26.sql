SELECT T.name, SUM(C.level) AS 'the total number of levels'
FROM Trainer AS T
JOIN CatchedPokemon AS C
ON T.id = C.owner_id
GROUP BY T.id
ORDER BY SUM(C.level) DESC
LIMIT 1;

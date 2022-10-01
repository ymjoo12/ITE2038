SELECT T.name, COUNT(*)
FROM Trainer AS T
JOIN CatchedPokemon AS C
ON T.id = C.owner_id
GROUP BY T.id
ORDER BY T.name;

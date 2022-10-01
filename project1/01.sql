SELECT name
FROM Trainer AS T
JOIN CatchedPokemon AS C
ON T.id = C.owner_id
GROUP BY T.id
HAVING COUNT(*) >= 3
ORDER BY COUNT(*);

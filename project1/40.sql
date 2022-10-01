SELECT DISTINCT T.name
FROM Trainer AS T
JOIN CatchedPokemon AS C
ON T.id = C.owner_id
JOIN Pokemon AS P
ON C.pid = P.id
WHERE P.name = 'Pikachu'
ORDER BY T.name;

SELECT SUM(C.level)
FROM CatchedPokemon AS C
JOIN Trainer AS T
ON C.owner_id = T.id
WHERE T.name = 'Matis';

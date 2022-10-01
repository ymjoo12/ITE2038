SELECT AVG(C.level)
FROM CatchedPokemon AS C
LEFT JOIN Pokemon AS P
ON C.pid = P.id
WHERE P.type = 'Water';

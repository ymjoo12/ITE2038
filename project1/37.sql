SELECT SUM(C.level)
FROM CatchedPokemon AS C
JOIN Pokemon AS P
ON C.pid = P.id
WHERE P.type = 'Fire';

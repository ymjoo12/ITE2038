SELECT DISTINCT C.nickname
FROM CatchedPokemon AS C
JOIN Gym AS G
ON C.owner_id = G.leader_id
JOIN Pokemon AS P
ON C.pid = P.id
WHERE G.city = 'Sangnok City' AND P.type = 'Water'
ORDER BY C.nickname;

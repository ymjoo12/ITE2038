SELECT P.name
FROM Pokemon AS P
JOIN CatchedPokemon AS C
ON P.id = C.pid
JOIN Gym AS G
ON C.owner_id = G.leader_id
WHERE G.city = 'Rainbow City'
ORDER BY P.name;

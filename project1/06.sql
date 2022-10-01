SELECT
  L.name AS 'name of the gym leader', 
  AVG(C.level) AS 'the average of the Pokémon level'
FROM (
  SELECT G.leader_id AS 'id', T.name AS 'name'
  FROM Gym AS G
  JOIN Trainer AS T
  ON G.leader_id = T.id
) AS L
JOIN CatchedPokemon AS C
ON L.id = C.owner_id
GROUP BY L.id
ORDER BY L.name;

CREATE VIEW X AS
SELECT T.id, T.name
FROM (
  SELECT DISTINCT C.owner_id, P.type
  FROM CatchedPokemon AS C
  JOIN Pokemon AS P
  ON C.pid = P.id
) AS Y
JOIN Trainer AS T
ON Y.owner_id = T.id
GROUP BY T.id
HAVING COUNT(*) = 1;

SELECT X.name, P.name, COUNT(*)
FROM X
JOIN CatchedPokemon AS C
ON X.id = C.owner_id
JOIN Pokemon AS P
ON C.pid = P.id
GROUP BY X.name, P.name
ORDER BY X.name;

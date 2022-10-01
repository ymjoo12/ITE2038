SELECT T.name
FROM Trainer AS T
WHERE T.id IN (
  SELECT owner_id
  FROM CatchedPokemon
  GROUP BY owner_id, pid
  HAVING COUNT(*) >= 2
)
ORDER BY T.name;

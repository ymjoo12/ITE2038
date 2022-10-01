SELECT DISTINCT P.name 
FROM CatchedPokemon AS C
LEFT JOIN Pokemon AS P
ON C.pid = P.id
WHERE C.owner_id IN (
  SELECT id
  FROM Trainer
  WHERE hometown IN ('Sangnok City','Blue City')
)
ORDER BY name;

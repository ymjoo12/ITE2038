SELECT DISTINCT T.name
FROM Trainer AS T
JOIN CatchedPokemon AS C
ON T.id = C.owner_id
WHERE 
  T.hometown = 'Sangnok City' AND
  C.pid IN (
    SELECT id
    FROM Pokemon
    WHERE name LIKE 'P%'
  )
ORDER BY T.name;

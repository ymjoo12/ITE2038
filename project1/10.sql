SELECT DISTINCT P.name
FROM Pokemon AS P
WHERE NOT EXISTS (
  SELECT *
  FROM CatchedPokemon AS C
  WHERE P.id = C.pid
)
ORDER BY P.name;

SELECT AVG(level) AS 'average level of Pokémon Red caught'
FROM CatchedPokemon
WHERE owner_id IN (
  SELECT id
  FROM Trainer
  WHERE name = 'Red'
);

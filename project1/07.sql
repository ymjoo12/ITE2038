CREATE VIEW X AS
SELECT nickname, level, hometown
FROM CatchedPokemon AS C
JOIN Trainer AS T
ON T.id = C.owner_id;

SELECT DISTINCT X.nickname
FROM X
JOIN (
  SELECT MAX(level) AS 'hlv', hometown
  FROM X
  GROUP BY hometown
) AS Y
ON X.level = Y.hlv AND X.hometown = Y.hometown
ORDER BY X.nickname;

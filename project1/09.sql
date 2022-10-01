CREATE VIEW X AS
SELECT  T.id AS 'tid', T.name, MAX(C.level) AS 'hlv'
FROM Trainer AS T
JOIN CatchedPokemon AS C
ON T.id = C.owner_id
GROUP BY T.id
HAVING COUNT(*) >= 4;

SELECT X.name, C.nickname
FROM X
JOIN CatchedPokemon AS C
ON X.tid = C.owner_id AND X.hlv = C.level
ORDER BY C.nickname;

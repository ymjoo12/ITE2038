CREATE VIEW X AS
SELECT C.name, COUNT(*) AS 'native'
FROM City AS C
JOIN Trainer AS T
ON C.name = T.hometown
GROUP BY C.name;

SELECT name
FROM X
WHERE native = (
  SELECT MAX(native)
  FROM X
);

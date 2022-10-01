CREATE VIEW X AS
SELECT after_id
FROM Evolution
WHERE before_id NOT IN (
  SELECT after_id
  FROM Evolution
);

SELECT P.name
FROM Pokemon AS P
RIGHT JOIN X
ON P.id = X.after_id
ORDER BY P.name;

SELECT P.name
FROM Pokemon AS P
JOIN (
  SELECT E.after_id
  FROM Pokemon AS P
  JOIN (
    SELECT E.after_id
    FROM Pokemon AS P
    JOIN Evolution AS E
    ON P.id = E.before_id
    WHERE P.name = 'Charmander'
  ) AS X
  ON P.id = X.after_id
  JOIN Evolution AS E
  ON P.id = E.before_id
) AS X
ON P.id = X.after_id;

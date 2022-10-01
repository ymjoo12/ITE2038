SELECT P.name
FROM Pokemon AS P
JOIN Evolution AS E
ON P.id = E.before_id
WHERE E.before_id > E.after_id
ORDER BY P.name;

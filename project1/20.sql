SELECT T.name
FROM Trainer AS T
RIGHT JOIN Gym AS G
ON T.id = G.leader_id
ORDER BY T.name;

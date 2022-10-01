SELECT T.name
FROM Trainer AS T
RIGHT JOIN Gym AS G
ON T.id = G.leader_id
RIGHT JOIN City AS C
ON G.city = C.name
WHERE C.description like '%Amazon%';

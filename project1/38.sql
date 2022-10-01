SELECT T.name 
FROM Trainer AS T
JOIN Gym AS G
ON T.id = G.leader_id
WHERE G.city = 'Brown City';

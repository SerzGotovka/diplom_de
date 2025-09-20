WITH datetime($cutoff) AS cutoff
MATCH (u:User)-[r:FRIENDS_WITH]-(v:User)
WHERE datetime(coalesce(r.created_at, '0001-01-01T00:00:00')) <= cutoff
RETURN u.user_id AS user_id, count(DISTINCT v) AS degree;

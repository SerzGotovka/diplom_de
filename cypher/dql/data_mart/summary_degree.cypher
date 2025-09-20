// Средняя степень и суммарные ребра (как UNDIRECTED)
// $cutoff — верхняя граница включительно, "конец дня"
WITH datetime($cutoff) AS cutoff
MATCH (u:User)-[r:FRIENDS_WITH]-(v:User)
WHERE datetime(coalesce(r.created_at, '0001-01-01T00:00:00')) <= cutoff
WITH u, count(DISTINCT v) AS deg
// ЕСЛИ в графе хранится по ДВА ребра на дружбу (u->v и v->u), делим пополам:
RETURN avg(deg) AS avg_friends, toInteger(sum(deg) / 2) AS total_friendships;

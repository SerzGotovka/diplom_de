// FRIENDS: (User)-[:FRIENDS_WITH]->(User)
MATCH (u1:User {user_id: $user_id}), (u2:User {user_id: $friend_id})
MERGE (u1)-[:FRIENDS_WITH {created_at: $created_at}]->(u2);

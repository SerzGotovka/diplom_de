MATCH (u:User {user_id: $user_id}), (c:Comment {comment_id: $target_id})
MERGE (u)-[:REACTED {reaction: $reaction, created_at: $created_at}]->(c);

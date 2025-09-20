// USER POSTED
MATCH (u:User {user_id: $user_id}), (p:Post {post_id: $post_id})
MERGE (u)-[:POSTED {created_at: $created_at}]->(p);

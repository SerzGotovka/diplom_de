// USER COMMENTED
MATCH (u:User {user_id: $user_id}), (c:Comment {comment_id: $comment_id}), (p:Post {post_id: $post_id})
MERGE (u)-[:COMMENTED {created_at: $created_at}]->(c)
MERGE (c)-[:ON]->(p);

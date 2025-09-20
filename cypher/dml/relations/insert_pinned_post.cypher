// COMMUNITY PINNED POST
MATCH (c:Community {community_id: $community_id}), (p:Post {post_id: $post_id})
MERGE (c)-[:PINNED]->(p);

// MEDIA ATTACHED TO POST
MATCH (m:Media {media_id: $media_id}), (p:Post {post_id: $attached_to_post})
MERGE (m)-[:ATTACHED_TO]->(p);

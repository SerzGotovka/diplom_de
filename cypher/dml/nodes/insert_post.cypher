MERGE (p:Post {post_id: $post_id})
SET p.user_id = $user_id,
    p.text = $text,
    p.created_at = $created_at

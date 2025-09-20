MERGE (c:Comment {comment_id: $comment_id})
SET c.post_id = $post_id,
    c.user_id = $user_id,
    c.text = $text,
    c.created_at = $created_at

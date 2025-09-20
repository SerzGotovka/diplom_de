INSERT INTO dds.comments (comment_id, post_id, user_id, text, created_at)
VALUES (%(comment_id)s, %(post_id)s, %(user_id)s, %(text)s, %(created_at)s)
ON CONFLICT (comment_id) DO NOTHING;
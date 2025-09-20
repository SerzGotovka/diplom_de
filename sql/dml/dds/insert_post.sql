INSERT INTO dds.posts (post_id, user_id, text, created_at)
VALUES (%(post_id)s, %(user_id)s, %(text)s, %(created_at)s)
ON CONFLICT (post_id) DO NOTHING;

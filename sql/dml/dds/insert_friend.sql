INSERT INTO dds.friends (user_id, friend_id, created_at)
VALUES (%(user_id)s, %(friend_id)s, %(created_at)s)
ON CONFLICT (user_id, friend_id) DO NOTHING;

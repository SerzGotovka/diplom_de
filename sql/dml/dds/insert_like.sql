INSERT INTO dds.likes (like_id, user_id, target_type, target_id, created_at)
VALUES (%(like_id)s, %(user_id)s, %(target_type)s, %(target_id)s, %(created_at)s)
ON CONFLICT (like_id) DO NOTHING;

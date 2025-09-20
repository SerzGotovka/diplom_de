INSERT INTO dds.users (user_id, name, created_at)
VALUES (%(user_id)s, %(name)s, %(created_at)s)
ON CONFLICT (user_id) DO NOTHING;

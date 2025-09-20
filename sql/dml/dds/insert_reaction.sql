INSERT INTO dds.reactions (reaction_id, user_id, target_type, target_id, reaction, created_at)
VALUES (%(reaction_id)s, %(user_id)s, %(target_type)s, %(target_id)s, %(reaction)s, %(created_at)s)
ON CONFLICT (reaction_id) DO NOTHING;

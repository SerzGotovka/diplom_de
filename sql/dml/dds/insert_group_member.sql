INSERT INTO dds.group_members (community_id, user_id, joined_at)
VALUES (%(community_id)s, %(user_id)s, %(joined_at)s)
ON CONFLICT (community_id, user_id) DO NOTHING;

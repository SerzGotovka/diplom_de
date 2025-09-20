INSERT INTO dds.communities (community_id, title, created_at)
VALUES (%(community_id)s, %(title)s, %(created_at)s)
ON CONFLICT (community_id) DO NOTHING;

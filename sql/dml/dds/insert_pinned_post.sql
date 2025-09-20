INSERT INTO dds.pinned_posts (community_id, post_id)
VALUES (%(community_id)s, %(post_id)s)
ON CONFLICT (community_id, post_id) DO NOTHING;

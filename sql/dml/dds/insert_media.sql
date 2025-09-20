INSERT INTO dds.media (media_id, media_type, url, attached_to_post, created_at)
VALUES (%(media_id)s, %(media_type)s, %(url)s, %(attached_to_post)s, %(created_at)s)
ON CONFLICT (media_id) DO NOTHING;

MERGE (m:Media {media_id: $media_id})
SET m.media_type = $media_type,
    m.url = $url,
    m.attached_to_post = $attached_to_post,
    m.created_at = $created_at

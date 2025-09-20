MERGE (comm:Community {community_id: $community_id})
SET comm.title = $title,
    comm.created_at = $created_at

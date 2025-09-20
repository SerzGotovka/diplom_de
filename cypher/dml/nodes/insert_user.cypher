MERGE (u:User {user_id: $user_id})
SET u.name = $name,
    u.created_at = $created_at

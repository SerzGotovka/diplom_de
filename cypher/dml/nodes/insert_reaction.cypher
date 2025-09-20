// REACTION node
MERGE (r:Reaction {
  reaction_id: $reaction_id
})
SET r.user_id = $user_id,
    r.target_type = $target_type,
    r.target_id = $target_id,
    r.reaction = $reaction,
    r.created_at = $created_at

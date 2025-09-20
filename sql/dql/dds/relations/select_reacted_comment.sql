SELECT user_id, target_id, reaction, created_at
FROM dds.reactions
WHERE target_type = 'comment';

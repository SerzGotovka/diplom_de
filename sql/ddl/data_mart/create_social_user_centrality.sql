CREATE TABLE IF NOT EXISTS data_mart.social_user_centrality
(
    dt Date,
    user_id String,
    degree UInt32,                -- число друзей
    degree_centrality Float64     -- degree / max_degree в срезе дня
)
ENGINE = ReplacingMergeTree
ORDER BY (dt, user_id);
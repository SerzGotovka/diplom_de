CREATE TABLE IF NOT EXISTS data_mart.community_daily_stats
(
    dt Date,
    community_id String,
    new_members UInt64,
    posts UInt64,
    comments UInt64,
    reactions UInt64
)
ENGINE = ReplacingMergeTree
ORDER BY (dt, community_id);

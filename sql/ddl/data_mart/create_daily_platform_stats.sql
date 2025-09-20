CREATE TABLE IF NOT EXISTS data_mart.daily_platform_stats
(
    dt Date,
    total_posts UInt64,
    total_comments UInt64,
    total_reactions UInt64,
    interactions_per_post Float64,
    dau UInt64,
    wau UInt64,
    mau UInt64,
    dau_wau_ratio Float64,
    wau_mau_ratio Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(dt)
ORDER BY (dt);

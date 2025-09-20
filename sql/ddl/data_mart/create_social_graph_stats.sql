CREATE TABLE IF NOT EXISTS data_mart.social_graph_stats
(
    dt Date,
    avg_friends Float64,          -- среднее число друзей на пользователя
    total_friendships UInt64      -- количество дружеских связей (уникальных рёбер)
)
ENGINE = ReplacingMergeTree
ORDER BY dt;
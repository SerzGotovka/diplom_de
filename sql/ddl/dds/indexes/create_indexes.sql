-- === Индексы на FK-колонках ===
CREATE INDEX IF NOT EXISTS idx_posts_user_id              ON dds.posts(user_id);
CREATE INDEX IF NOT EXISTS idx_comments_post_id           ON dds.comments(post_id);
CREATE INDEX IF NOT EXISTS idx_comments_user_id           ON dds.comments(user_id);
CREATE INDEX IF NOT EXISTS idx_media_attached_to_post     ON dds.media(attached_to_post);
CREATE INDEX IF NOT EXISTS idx_group_members_user_id      ON dds.group_members(user_id);
CREATE INDEX IF NOT EXISTS idx_group_members_community_id ON dds.group_members(community_id);
CREATE INDEX IF NOT EXISTS idx_pinned_posts_post_id       ON dds.pinned_posts(post_id);
CREATE INDEX IF NOT EXISTS idx_pinned_posts_community_id  ON dds.pinned_posts(community_id);
CREATE INDEX IF NOT EXISTS idx_friends_user_id            ON dds.friends(user_id);
CREATE INDEX IF NOT EXISTS idx_friends_friend_id          ON dds.friends(friend_id);

-- Временные фильтры
CREATE INDEX IF NOT EXISTS idx_posts_created_at     ON dds.posts(created_at);
CREATE INDEX IF NOT EXISTS idx_comments_created_at  ON dds.comments(created_at);
CREATE INDEX IF NOT EXISTS idx_reactions_created_at ON dds.reactions(created_at);
CREATE INDEX IF NOT EXISTS idx_likes_created_at     ON dds.likes(created_at);
CREATE INDEX IF NOT EXISTS idx_media_created_at     ON dds.media(created_at);
CREATE INDEX IF NOT EXISTS idx_users_created_at     ON dds.users(created_at);
CREATE INDEX IF NOT EXISTS idx_communities_created_at ON dds.communities(created_at);
CREATE INDEX IF NOT EXISTS idx_gm_joined_at         ON dds.group_members(joined_at);

-- DAU/WAU (distinct по user_id)
CREATE INDEX IF NOT EXISTS idx_posts_created_user     ON dds.posts(created_at, user_id);
CREATE INDEX IF NOT EXISTS idx_comments_created_user  ON dds.comments(created_at, user_id);
CREATE INDEX IF NOT EXISTS idx_reactions_created_user ON dds.reactions(created_at, user_id);
CREATE INDEX IF NOT EXISTS idx_likes_created_user     ON dds.likes(created_at, user_id);

-- Для community-метрик
CREATE INDEX IF NOT EXISTS idx_posts_by_user          ON dds.posts(user_id);
CREATE INDEX IF NOT EXISTS idx_comments_by_post       ON dds.comments(post_id);
CREATE INDEX IF NOT EXISTS idx_media_by_post          ON dds.media(attached_to_post);
CREATE INDEX IF NOT EXISTS idx_pinned_by_comm         ON dds.pinned_posts(community_id);
CREATE INDEX IF NOT EXISTS idx_gm_by_comm_user        ON dds.group_members(community_id, user_id);
CREATE INDEX IF NOT EXISTS idx_group_members_joined             ON dds.group_members (community_id, joined_at);

CREATE TABLE IF NOT EXISTS dds.group_members (
    community_id VARCHAR NOT NULL,
    user_id      VARCHAR NOT NULL,
    joined_at    TIMESTAMP NOT NULL,
    PRIMARY KEY (community_id, user_id),
    CONSTRAINT fk_gm_community
        FOREIGN KEY (community_id) REFERENCES dds.communities(community_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_gm_user
        FOREIGN KEY (user_id) REFERENCES dds.users(user_id)
        ON DELETE CASCADE
);
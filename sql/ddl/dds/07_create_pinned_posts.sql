CREATE TABLE IF NOT EXISTS dds.pinned_posts (
    community_id VARCHAR NOT NULL,
    post_id      VARCHAR NOT NULL,
    PRIMARY KEY (community_id, post_id),
    CONSTRAINT fk_pin_community
        FOREIGN KEY (community_id) REFERENCES dds.communities(community_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_pin_post
        FOREIGN KEY (post_id) REFERENCES dds.posts(post_id)
        ON DELETE CASCADE
);
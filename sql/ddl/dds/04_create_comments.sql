CREATE TABLE IF NOT EXISTS dds."comments" (
    comment_id VARCHAR PRIMARY KEY,
    post_id    VARCHAR NOT NULL,
    user_id    VARCHAR NOT NULL,
    text       TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    CONSTRAINT fk_comments_post
        FOREIGN KEY (post_id) REFERENCES dds.posts(post_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_comments_user
        FOREIGN KEY (user_id) REFERENCES dds.users(user_id)
        ON DELETE CASCADE
);
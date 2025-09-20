CREATE TABLE IF NOT EXISTS dds.likes (
    like_id     VARCHAR PRIMARY KEY,
    user_id     VARCHAR NOT NULL,
    target_type TEXT    NOT NULL, -- 'post' | 'comment'
    target_id   VARCHAR NOT NULL,
    created_at  TIMESTAMP NOT NULL,
    CONSTRAINT fk_like_user
        FOREIGN KEY (user_id) REFERENCES dds.users(user_id)
        ON DELETE CASCADE,
    CONSTRAINT chk_like_target_type
        CHECK (target_type IN ('post','comment'))
);
CREATE TABLE IF NOT EXISTS dds.reactions (
    reaction_id VARCHAR PRIMARY KEY,
    user_id     VARCHAR NOT NULL,
    target_type TEXT    NOT NULL, -- 'post' | 'comment'
    target_id   VARCHAR NOT NULL,
    reaction    TEXT    NOT NULL, -- 'like','love','wow','angry','sad'
    created_at  TIMESTAMP NOT NULL,
    CONSTRAINT fk_react_user
        FOREIGN KEY (user_id) REFERENCES dds.users(user_id)
        ON DELETE CASCADE,
    CONSTRAINT chk_react_target_type
        CHECK (target_type IN ('post','comment')),
    CONSTRAINT chk_react_enum
        CHECK (reaction IN ('like','love','wow','angry','sad'))
);
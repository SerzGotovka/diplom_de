CREATE TABLE IF NOT EXISTS dds.friends (
    user_id    VARCHAR NOT NULL,
    friend_id  VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (user_id, friend_id),
    CONSTRAINT fk_f_u  FOREIGN KEY (user_id)   REFERENCES dds.users(user_id)  ON DELETE CASCADE,
    CONSTRAINT fk_f_v  FOREIGN KEY (friend_id) REFERENCES dds.users(user_id)  ON DELETE CASCADE,
    CONSTRAINT chk_f_not_self CHECK (user_id <> friend_id)
);
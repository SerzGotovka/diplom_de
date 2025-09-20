CREATE TABLE IF NOT EXISTS dds.media (
    media_id        VARCHAR PRIMARY KEY,
    media_type      TEXT NOT NULL,
    url             TEXT NOT NULL,
    attached_to_post VARCHAR NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    CONSTRAINT fk_media_post
        FOREIGN KEY (attached_to_post) REFERENCES dds.posts(post_id)
        ON DELETE CASCADE,
    CONSTRAINT chk_media_type
        CHECK (media_type IN ('photo','video','album'))
);
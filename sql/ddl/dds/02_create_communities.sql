CREATE TABLE IF NOT EXISTS dds.communities (
    community_id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL
);

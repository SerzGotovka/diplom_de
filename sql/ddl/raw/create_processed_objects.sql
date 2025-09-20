-- Помечаем обработанные объекты из MinIO (или S3)
CREATE TABLE IF NOT EXISTS raw.processed_objects (
    id           bigserial PRIMARY KEY,
    storage      varchar(32) NOT NULL,   -- 'minio'
    object_key   text        NOT NULL,   -- путь/имя в бакете
    etag         text,
    processed_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (storage, object_key)
);

CREATE INDEX IF NOT EXISTS idx_processed_objects_storage_key
    ON raw.processed_objects (storage, object_key);

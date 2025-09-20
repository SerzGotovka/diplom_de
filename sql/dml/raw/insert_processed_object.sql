INSERT INTO raw.processed_objects (storage, object_key, etag)
VALUES (%(storage)s, %(object_key)s, %(etag)s)
ON CONFLICT (storage, object_key) DO NOTHING;
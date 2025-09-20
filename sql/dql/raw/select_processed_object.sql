SELECT 1
FROM raw.processed_objects
WHERE storage = %(storage)s AND object_key = %(object_key)s
LIMIT 1;
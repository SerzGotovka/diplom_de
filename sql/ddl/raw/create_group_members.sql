CREATE TABLE IF NOT EXISTS raw.group_members (
    id SERIAL PRIMARY KEY,
    event_json JSONB NOT NULL,
    loaded_at TIMESTAMP DEFAULT now()
);

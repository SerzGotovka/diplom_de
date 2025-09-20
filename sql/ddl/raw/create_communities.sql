CREATE TABLE IF NOT EXISTS raw.communities (
    id SERIAL PRIMARY KEY,
    event_json JSONB NOT NULL,
    loaded_at TIMESTAMP DEFAULT now()
);

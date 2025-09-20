CREATE TABLE IF NOT EXISTS raw.users (
    id SERIAL PRIMARY KEY,
    event_json JSONB NOT NULL,
    loaded_at TIMESTAMP DEFAULT now()
);

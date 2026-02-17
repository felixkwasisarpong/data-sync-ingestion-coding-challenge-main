CREATE TABLE IF NOT EXISTS ingested_events (
  event_id TEXT PRIMARY KEY,
  occurred_at TIMESTAMPTZ NULL,
  payload JSONB NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ingestion_state (
  id SMALLINT PRIMARY KEY CHECK (id = 1),
  cursor TEXT NULL,
  total_ingested BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO ingestion_state (id, cursor, total_ingested)
VALUES (1, NULL, 0)
ON CONFLICT (id) DO NOTHING;

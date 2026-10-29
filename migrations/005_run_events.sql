CREATE TABLE IF NOT EXISTS run_events (
  run_id uuid NOT NULL REFERENCES backend_runs(id) ON DELETE CASCADE,
  sequence integer NOT NULL CHECK (sequence BETWEEN 1 AND 10000),
  event jsonb NOT NULL CHECK (jsonb_typeof(event) = 'object'),
  PRIMARY KEY (run_id, sequence)
);

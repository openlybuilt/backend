CREATE TABLE IF NOT EXISTS run_recordings (
  run_id uuid PRIMARY KEY REFERENCES backend_runs(id) ON DELETE CASCADE,
  status text NOT NULL CHECK (status IN ('pending', 'saved', 'failed')),
  browser_id text,
  replay_id text,
  video bytea,
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((status = 'saved' AND video IS NOT NULL AND octet_length(video) BETWEEN 16 AND 104857600)
    OR (status <> 'saved' AND video IS NULL))
);
CREATE INDEX IF NOT EXISTS backend_runs_workflow_created_idx
  ON backend_runs (workflow_id, created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS workflows (
  id uuid PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS workflow_versions (
  workflow_id uuid NOT NULL REFERENCES workflows(id),
  version integer NOT NULL,
  definition jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (workflow_id, version)
);
ALTER TABLE backend_runs ADD COLUMN IF NOT EXISTS workflow jsonb;
ALTER TABLE backend_runs ADD COLUMN IF NOT EXISTS workflow_id uuid;
ALTER TABLE backend_runs ADD COLUMN IF NOT EXISTS workflow_version integer;
ALTER TABLE backend_runs ADD COLUMN IF NOT EXISTS workflow_inputs jsonb;
ALTER TABLE backend_runs ADD COLUMN IF NOT EXISTS steps jsonb;

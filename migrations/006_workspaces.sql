CREATE TABLE IF NOT EXISTS identities (
  id uuid PRIMARY KEY,
  issuer text NOT NULL,
  subject text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (issuer, subject)
);

CREATE TABLE IF NOT EXISTS workspaces (
  id uuid PRIMARY KEY,
  identity_id uuid NOT NULL UNIQUE REFERENCES identities(id),
  created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE workflows ADD COLUMN IF NOT EXISTS workspace_id uuid REFERENCES workspaces(id);
ALTER TABLE backend_runs ADD COLUMN IF NOT EXISTS workspace_id uuid REFERENCES workspaces(id);

CREATE INDEX IF NOT EXISTS workflows_workspace_idx ON workflows(workspace_id, id);
CREATE INDEX IF NOT EXISTS backend_runs_workspace_created_idx
  ON backend_runs(workspace_id, created_at DESC, id DESC);

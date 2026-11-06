CREATE TABLE IF NOT EXISTS guide_example_runs (
  id uuid PRIMARY KEY,
  workspace_id uuid NOT NULL REFERENCES workspaces(id),
  workflow_id uuid NOT NULL,
  workflow_version integer NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (workflow_id, workflow_version) REFERENCES workflow_versions(workflow_id, version)
);
CREATE INDEX IF NOT EXISTS guide_example_runs_workspace_created_idx
  ON guide_example_runs(workspace_id, created_at DESC, id DESC);

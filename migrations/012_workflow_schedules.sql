ALTER TABLE workspaces ADD COLUMN IF NOT EXISTS timezone text;

CREATE TABLE IF NOT EXISTS workflow_schedules (
  id uuid PRIMARY KEY,
  workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  workflow_id uuid NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
  kind text NOT NULL CHECK (kind IN ('once', 'interval')),
  run_at timestamptz,
  interval_minutes integer CHECK (interval_minutes BETWEEN 1 AND 525600),
  enabled boolean NOT NULL DEFAULT true,
  recording_enabled boolean NOT NULL DEFAULT true,
  approved_secret_names text[] NOT NULL DEFAULT '{}',
  required_secret_names text[] NOT NULL DEFAULT '{}',
  next_run_at timestamptz,
  issue text,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((kind = 'once' AND run_at IS NOT NULL AND interval_minutes IS NULL)
      OR (kind = 'interval' AND run_at IS NULL AND interval_minutes IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS workflow_schedules_workspace_workflow_idx
  ON workflow_schedules(workspace_id, workflow_id);
CREATE INDEX IF NOT EXISTS workflow_schedules_due_idx
  ON workflow_schedules(next_run_at) WHERE enabled AND next_run_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS schedule_creation_requests (
  workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  creation_key uuid NOT NULL,
  fingerprint text NOT NULL,
  schedule_id uuid REFERENCES workflow_schedules(id) ON DELETE SET NULL,
  PRIMARY KEY (workspace_id, creation_key)
);

CREATE TABLE IF NOT EXISTS schedule_occurrences (
  id uuid PRIMARY KEY,
  schedule_id uuid NOT NULL REFERENCES workflow_schedules(id) ON DELETE CASCADE,
  scheduled_at timestamptz NOT NULL,
  status text NOT NULL CHECK (status IN ('queued', 'dispatching', 'started', 'missed', 'failed', 'outcome_unknown')),
  run_id uuid UNIQUE,
  reason text,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(schedule_id, scheduled_at)
);

CREATE INDEX IF NOT EXISTS schedule_occurrences_queue_idx
  ON schedule_occurrences(schedule_id, scheduled_at) WHERE status = 'queued';

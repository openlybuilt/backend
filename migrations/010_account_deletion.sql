CREATE TABLE IF NOT EXISTS account_deletions (
  identity_hash text PRIMARY KEY,
  issuer text,
  subject text,
  workspace_id uuid,
  state text NOT NULL CHECK (state IN ('pending', 'data_deleted', 'provider_cleanup_pending', 'deleted')),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS account_deletion_replays (
  identity_hash text NOT NULL REFERENCES account_deletions(identity_hash) ON DELETE CASCADE,
  run_id uuid NOT NULL,
  browser_id text,
  replay_id text,
  PRIMARY KEY (identity_hash, run_id)
);

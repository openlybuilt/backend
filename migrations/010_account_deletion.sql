CREATE TABLE IF NOT EXISTS account_deletions (
  identity_hash text PRIMARY KEY,
  issuer text,
  subject text,
  workspace_id uuid,
  state text NOT NULL CHECK (state IN ('pending', 'data_deleted', 'deleted')),
  updated_at timestamptz NOT NULL DEFAULT now()
);

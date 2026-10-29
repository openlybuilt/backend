CREATE TABLE IF NOT EXISTS placeholders (
  workspace_id uuid NOT NULL REFERENCES workspaces(id),
  name text NOT NULL,
  secret boolean NOT NULL,
  ciphertext bytea NOT NULL,
  nonce bytea NOT NULL,
  tag bytea NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (workspace_id, name)
);

ALTER TABLE backend_runs ADD COLUMN IF NOT EXISTS request_fingerprint text;
ALTER TABLE backend_runs ADD COLUMN IF NOT EXISTS contains_secrets boolean NOT NULL DEFAULT false;

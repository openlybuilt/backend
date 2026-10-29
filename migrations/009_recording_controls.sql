ALTER TABLE backend_runs ADD COLUMN IF NOT EXISTS recording_enabled boolean NOT NULL DEFAULT true;
ALTER TABLE run_recordings DROP CONSTRAINT IF EXISTS run_recordings_status_check;
ALTER TABLE run_recordings ADD CONSTRAINT run_recordings_status_check CHECK (status IN ('pending', 'saved', 'failed', 'deleted'));
CREATE INDEX IF NOT EXISTS run_recordings_expiry_idx ON run_recordings(updated_at) WHERE status='saved';

ALTER TABLE workflows ADD COLUMN IF NOT EXISTS deleted_at timestamptz;

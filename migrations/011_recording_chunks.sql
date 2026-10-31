ALTER TABLE run_recordings ADD COLUMN IF NOT EXISTS byte_count bigint;
ALTER TABLE run_recordings ADD COLUMN IF NOT EXISTS upload_id uuid;
ALTER TABLE run_recordings DROP CONSTRAINT IF EXISTS run_recordings_check;
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='run_recordings'::regclass AND conname='run_recordings_storage_check') THEN
    ALTER TABLE run_recordings ADD CONSTRAINT run_recordings_storage_check CHECK (
      (status='saved' AND (
        (video IS NOT NULL AND octet_length(video)>=16 AND byte_count IS NULL AND upload_id IS NULL)
        OR (video IS NULL AND byte_count IS NOT NULL AND byte_count>=16 AND upload_id IS NOT NULL)
      )) OR (status<>'saved' AND video IS NULL AND byte_count IS NULL AND upload_id IS NULL)
    );
  END IF;
END $$;
CREATE TABLE IF NOT EXISTS recording_chunks (
  run_id uuid NOT NULL REFERENCES run_recordings(run_id) ON DELETE CASCADE,
  upload_id uuid NOT NULL,
  sequence integer NOT NULL CHECK (sequence>=0),
  data bytea NOT NULL CHECK (octet_length(data) BETWEEN 1 AND 1048576),
  PRIMARY KEY (run_id,upload_id,sequence)
);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = current_schema()
      AND table_name = 'workspaces' AND column_name = 'identity_id'
  ) THEN
    TRUNCATE TABLE workspaces CASCADE;
    ALTER TABLE workspaces DROP COLUMN identity_id;
  END IF;
END $$;
DROP TABLE IF EXISTS account_deletion_replays;
DROP TABLE IF EXISTS account_deletions;
DROP TABLE IF EXISTS identities;

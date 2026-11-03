ALTER TABLE workflow_schedules ADD COLUMN IF NOT EXISTS weekdays integer[];
ALTER TABLE workflow_schedules ADD COLUMN IF NOT EXISTS local_time text;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'workflow_schedules'::regclass AND conname = 'workflow_schedules_weekday_kind_check') THEN
    ALTER TABLE workflow_schedules DROP CONSTRAINT IF EXISTS workflow_schedules_kind_check;
    ALTER TABLE workflow_schedules DROP CONSTRAINT IF EXISTS workflow_schedules_check;
    ALTER TABLE workflow_schedules ADD CONSTRAINT workflow_schedules_weekday_kind_check CHECK (kind IN ('once', 'interval', 'weekly'));
    ALTER TABLE workflow_schedules ADD CONSTRAINT workflow_schedules_timing_check CHECK (
      (kind = 'once' AND run_at IS NOT NULL AND interval_minutes IS NULL AND weekdays IS NULL AND local_time IS NULL)
      OR (kind = 'interval' AND run_at IS NULL AND interval_minutes IS NOT NULL AND weekdays IS NULL AND local_time IS NULL)
      OR (kind = 'weekly' AND run_at IS NULL AND interval_minutes IS NULL AND weekdays IS NOT NULL AND cardinality(weekdays) BETWEEN 1 AND 7 AND weekdays <@ ARRAY[0,1,2,3,4,5,6] AND local_time IS NOT NULL AND local_time ~ '^([01][0-9]|2[0-3]):[0-5][0-9]$')
    );
  END IF;
END $$;

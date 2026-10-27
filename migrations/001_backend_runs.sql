CREATE TABLE IF NOT EXISTS backend_runs (
  id uuid PRIMARY KEY,
  instruction text NOT NULL,
  max_steps integer NOT NULL,
  timeout_ms integer NOT NULL,
  status text NOT NULL,
  cancel_requested boolean NOT NULL DEFAULT false,
  result jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

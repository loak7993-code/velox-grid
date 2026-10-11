-- velox-grid 001 — initial schema
-- Queue pattern: SELECT ... FOR UPDATE SKIP LOCKED — no external queue service.
CREATE TABLE api_keys (
  id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name              text NOT NULL,
  key_hash          text NOT NULL UNIQUE,          -- sha256 hex of the full key; the plaintext is never stored
  secret            text NOT NULL,                 -- per-key HMAC secret for webhook signatures
  rate_limit_per_min int NOT NULL DEFAULT 60,
  daily_quota       int NOT NULL DEFAULT 10000,
  revoked_at        timestamptz,
  created_at        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE jobs (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  key_id          uuid NOT NULL REFERENCES api_keys(id),
  idempotency_key text,
  url             text NOT NULL,
  engine          text NOT NULL DEFAULT 'auto',    -- auto | lite | cdp
  profile         jsonb NOT NULL DEFAULT '{}'::jsonb,
  extract         jsonb,                           -- { recipe } | { selector, fields }
  priority        smallint NOT NULL DEFAULT 5,
  status          text NOT NULL DEFAULT 'queued',  -- queued | running | succeeded | failed | canceled
  attempts        int NOT NULL DEFAULT 0,
  max_attempts    int NOT NULL DEFAULT 3,
  run_after       timestamptz NOT NULL DEFAULT now(),
  timeout_ms      int NOT NULL DEFAULT 120000,
  ttl_seconds     int NOT NULL DEFAULT 300,        -- result-cache TTL
  cache_key       text,
  cache_hit       boolean NOT NULL DEFAULT false,
  webhook_url     text,
  result          jsonb,
  error           text,
  locked_at       timestamptz,
  locked_by       text,
  heartbeat_at    timestamptz,
  created_at      timestamptz NOT NULL DEFAULT now(),
  started_at      timestamptz,
  finished_at     timestamptz,
  UNIQUE (key_id, idempotency_key)
);

CREATE INDEX jobs_queue_idx ON jobs (priority DESC, created_at ASC) WHERE status = 'queued';
CREATE INDEX jobs_key_recent_idx ON jobs (key_id, created_at DESC);
CREATE INDEX jobs_cache_idx ON jobs (cache_key) WHERE status = 'succeeded';

CREATE TABLE artifacts (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id       uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  name         text NOT NULL,
  path         text NOT NULL,                      -- relative to ARTIFACT_DIR
  bytes        bigint NOT NULL,
  content_type text NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  UNIQUE (job_id, name)
);

-- tenant-scoped result cache: a cache entry points at the job that produced it
CREATE TABLE cache_entries (
  cache_key  text PRIMARY KEY,
  key_id     uuid NOT NULL REFERENCES api_keys(id),
  job_id     uuid NOT NULL REFERENCES jobs(id),
  expires_at timestamptz NOT NULL,
  hits       int NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX cache_expiry_idx ON cache_entries (expires_at);

CREATE TABLE rate_windows (
  key_id       uuid NOT NULL REFERENCES api_keys(id),
  window_start timestamptz NOT NULL,               -- top of the minute
  count        int NOT NULL DEFAULT 0,
  PRIMARY KEY (key_id, window_start)
);

CREATE TABLE webhook_deliveries (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id       uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  url          text NOT NULL,
  attempt      int NOT NULL,
  status_code  int,
  error        text,
  delivered_at timestamptz NOT NULL DEFAULT now()
);


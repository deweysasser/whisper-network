-- whisper:foreign-keys-off
-- URL downloads: fetch jobs, downloader workers and pacing. jobs and tasks
-- are rebuilt (SQLite cannot alter a CHECK) following the procedure at
-- https://sqlite.org/lang_altertable.html; the directive above runs this
-- migration with foreign keys off. Children that reference jobs
-- (worker_failure_streak) keep referring to the name `jobs`.

CREATE TABLE jobs_new (
  id TEXT PRIMARY KEY,
  queue_id INTEGER NOT NULL REFERENCES queues,
  client_id TEXT NOT NULL,
  idempotency_key TEXT,
  state TEXT NOT NULL CHECK (state IN ('fetching','preparing','queued','running','done','failed')),
  weight TEXT NOT NULL DEFAULT 'medium' CHECK (weight IN ('low','medium','high')),
  model TEXT NOT NULL,
  language TEXT NOT NULL,
  prompt TEXT NOT NULL DEFAULT '',
  label TEXT NOT NULL DEFAULT '',
  source TEXT NOT NULL DEFAULT '',
  metadata TEXT,
  original_bytes INTEGER NOT NULL DEFAULT 0,
  original_sha256 TEXT NOT NULL DEFAULT '',
  original_type TEXT NOT NULL DEFAULT '',
  original_purged_at INTEGER,
  canonical_bytes INTEGER,
  canonical_sha256 TEXT,
  audio_samples INTEGER,
  canonical_purged_at INTEGER,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  started_at INTEGER,
  completed_at INTEGER,                   -- set on done and on failed
  compute_ms INTEGER,
  error TEXT,
  first_downloaded_at INTEGER,
  last_downloaded_at INTEGER,
  download_count INTEGER NOT NULL DEFAULT 0,
  retries INTEGER NOT NULL DEFAULT 0,
  fetch_url TEXT,                         -- NULL for uploads
  fetch_site TEXT,                        -- site key of fetch_url; NULL for uploads
  UNIQUE (client_id, idempotency_key));

INSERT INTO jobs_new (id, queue_id, client_id, idempotency_key, state, weight, model, language, prompt, label,
  source, metadata, original_bytes, original_sha256, original_type, original_purged_at, canonical_bytes,
  canonical_sha256, audio_samples, canonical_purged_at, created_at, updated_at, started_at, completed_at,
  compute_ms, error, first_downloaded_at, last_downloaded_at, download_count, retries)
SELECT id, queue_id, client_id, idempotency_key, state, weight, model, language, prompt, label,
  source, metadata, original_bytes, original_sha256, original_type, original_purged_at, canonical_bytes,
  canonical_sha256, audio_samples, canonical_purged_at, created_at, updated_at, started_at, completed_at,
  compute_ms, error, first_downloaded_at, last_downloaded_at, download_count, retries
FROM jobs;

CREATE TABLE tasks_new (
  job_id TEXT NOT NULL REFERENCES jobs ON DELETE CASCADE,
  seq INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('fetch','prepare','transcribe')),
  start_sample INTEGER,                   -- NULL for prepare
  end_sample INTEGER,
  cost REAL NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending','leased','done','failed','cancelled')),
  attempts INTEGER NOT NULL DEFAULT 0,
  result TEXT,
  engine_version TEXT,                    -- whisper.cpp for transcribe, ffmpeg for prepare
  worker_id TEXT,
  compute_ms INTEGER,
  error TEXT,
  pending_since INTEGER,
  PRIMARY KEY (job_id, seq));

INSERT INTO tasks_new (job_id, seq, kind, start_sample, end_sample, cost, state, attempts, result,
  engine_version, worker_id, compute_ms, error, pending_since)
SELECT job_id, seq, kind, start_sample, end_sample, cost, state, attempts, result,
  engine_version, worker_id, compute_ms, error, pending_since
FROM tasks;

DROP TABLE tasks;
DROP TABLE jobs;
ALTER TABLE jobs_new RENAME TO jobs;
ALTER TABLE tasks_new RENAME TO tasks;

CREATE INDEX jobs_queue_state ON jobs (queue_id, state, created_at);
CREATE INDEX jobs_queue_created ON jobs (queue_id, created_at, id);
CREATE INDEX jobs_completed ON jobs (completed_at) WHERE completed_at IS NOT NULL;
-- Must match jobStateRankSQL and jobStateKeySQL exactly (see 0009).
CREATE INDEX jobs_queue_state_order ON jobs (queue_id,
  (CASE state WHEN 'running' THEN 0 WHEN 'queued' THEN 1 WHEN 'preparing' THEN 1 WHEN 'fetching' THEN 1 WHEN 'failed' THEN 2 ELSE 3 END),
  (CASE WHEN state IN ('failed','done') THEN -COALESCE(completed_at, created_at) ELSE created_at END),
  id);
CREATE INDEX tasks_pending ON tasks (job_id, seq) WHERE state = 'pending';
CREATE INDEX tasks_state ON tasks (state);

ALTER TABLE workers ADD COLUMN kind TEXT NOT NULL DEFAULT 'processing' CHECK (kind IN ('processing','downloader'));
ALTER TABLE workers ADD COLUMN egress_key TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE workers ADD COLUMN ytdlp_version TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN egress_group TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN egress_ip TEXT NOT NULL DEFAULT '';

-- The egress a fetch lease ran from; NULL for non-fetch leases.
ALTER TABLE leases ADD COLUMN egress_key TEXT;

-- Per-site fetch policy, managed through the UI/admin API. egress_key NULL
-- applies to every egress. rules is a JSON array of "N/DUR".
CREATE TABLE fetch_policies (
  key TEXT PRIMARY KEY,
  site TEXT NOT NULL,
  egress_key TEXT,
  rules TEXT NOT NULL,
  concurrency INTEGER NOT NULL,
  backoff_initial_s INTEGER NOT NULL,
  backoff_cap_s INTEGER NOT NULL,
  updated_by TEXT NOT NULL,
  updated_at INTEGER NOT NULL);

INSERT INTO fetch_policies (key, site, egress_key, rules, concurrency, backoff_initial_s, backoff_cap_s, updated_by, updated_at) VALUES
  ('*', '*', NULL, '[]', 2, 300, 21600, 'system', CAST(strftime('%s','now') AS INTEGER) * 1000),
  ('youtube.com', 'youtube.com', NULL, '["1/3m"]', 1, 43200, 86400, 'system', CAST(strftime('%s','now') AS INTEGER) * 1000);

-- Fetch starts, for rate windows. Separate from leases so that deleting
-- jobs cannot reset a window.
CREATE TABLE fetch_starts (
  egress_key TEXT NOT NULL,
  site TEXT NOT NULL,
  started_at INTEGER NOT NULL);
CREATE INDEX fetch_starts_window ON fetch_starts (egress_key, site, started_at);

CREATE TABLE fetch_cooldowns (
  egress_key TEXT NOT NULL,
  site TEXT NOT NULL,
  until INTEGER NOT NULL,
  level INTEGER NOT NULL DEFAULT 0,
  manual INTEGER NOT NULL DEFAULT 0,
  set_by TEXT NOT NULL,
  reason TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (egress_key, site));

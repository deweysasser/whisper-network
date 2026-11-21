# whisper-network — Design Spec

Status: draft r3, 2026-11-15

Changes in r2:
- Weighted fair scheduling across queues, plus per-job `low`/`medium`/`high` weight (§8.2).
- Any input format is accepted; the system normalizes it, not the client (§8.1, §7.4).
- FLAC support checked against the source (§8.1.1).
- Enrollment tokens deferred; explicit approval only.
- New observability section: JSONL logs for VictoriaLogs, metrics for VictoriaMetrics/Prometheus (§10).

Changes in r3:
- Canonical audio is always FLAC; the server encodes WAV uploads in pure Go (corrects §8.1.1).
- Built on the `golang-program` template: kong CLI, zerolog logging, Makefile and GitHub workflows. Go 1.27.

---

## 1. Purpose

Distribute Whisper transcription across an ad-hoc fleet of machines. Any machine that can run a container (or a static binary) can join as a worker, with nothing configured in advance except the server URL.

The motivating workload is a large backlog of recordings. Hundreds of hours of audio take days of CPU on a single machine, and the fleet turns that into hours. Clients are typically automation, such as a batch script or a pipeline step, that only needs to speak HTTP. Clients don't change when the fleet grows.

### Goals

- One Go binary with two modes: `server` and `worker` (a worker is either a processing worker or, with `--downloader`, a downloader, §7.6).
- Zero-config worker start: `whisper-network worker --server https://…`.
- No shared filesystem. Media and results move over HTTP.
- **The system absorbs the media work.** Clients upload whatever they have, and conversion happens inside the service.
- At-least-once processing with fenced leases. A crashed or partitioned worker never loses a job, and it can never overwrite a result it no longer owns.
- Weighted fair sharing of the fleet across queues, and across job weights within a queue.
- Per-queue trust boundaries. A worker sees media only from queues it has been granted.
- Transcription never competes with a machine's real work (lowest CPU and I/O priority).
- First-class observability: JSONL logs for VictoriaLogs, Prometheus-format metrics both scrapeable and pushable.
- SQLite metadata plus a local blob directory. One volume to back up.

- **URL jobs.** A client holding `submit_url` may submit a media URL instead of an upload (§6.1). **The server still never fetches anything a client names**: only downloader workers do (§7.6), under a sandbox and an SSRF filter, and the bytes come back as the job's original.

### Non-goals (v1)

- High availability or multiple server replicas. One server, one SQLite file.
- Speaker diarization, translation, or any engine other than whisper.cpp.
- Downloading video (URL jobs fetch audio only), playlists, channels, live streams, or client-supplied credentials, cookies, headers or yt-dlp options. Operator-supplied cookies on a downloader are the only per-site authentication.
- Push notifications to clients. Clients poll.
- **Enrollment tokens.** Every worker is approved explicitly by a human. Deferred, not planned.
- Multi-tenant hostile isolation between queue admins. Queue admins are trusted operators.

---

## 2. Decisions that differ from the base idea

Each of these changes something in the original idea. Confidence is given for each.

| # | Original idea | Spec does | Why | Confidence |
|---|---|---|---|---|
| D1 | Worker `GET /job` to take a job | `POST /worker/{id}/leases` (long-poll) | GET must be safe and idempotent. Proxies, retries and prefetchers repeat GETs, and a repeated claim leaks a lease. Claiming creates a resource (a lease), so POST is the right method. | High |
| D2 | Heartbeat and results on `/job/{id}/…` | On `/lease/{id}/…` | The lease ID doubles as a fencing token. A worker whose lease expired gets `410 Gone` on heartbeat or result, so a slow worker can't clobber a re-run. Workers never need to know the job/task split. | High |
| D3 | `POST /job/{id}` or `PUT …/results`? | `PUT /lease/{id}/result` | A result has exactly one value per lease, and the worker may retry the upload after a network error. PUT gives idempotent replace semantics: the same lease plus the same body returns 200 again. | High |
| D4 | One job = one unit of work | A job is split into **tasks**: one optional `prepare` task, then N `transcribe` tasks of ≤ ~1 h audio each. Workers lease tasks. | Chunking is already required for memory (4–5 h meetings OOM'd at 3 GB unchunked). Splitting on the server also lets one long meeting run in parallel across the fleet, and it gives media conversion a natural home (D5). | Medium |
| D5 | (unspecified) | **Accept any media.** Normalization (ffmpeg → 16 kHz mono FLAC) is a `prepare` task run by a worker in a sandbox. Uploads that are already canonical skip it. | Clients don't have to convert. ffmpeg on untrusted media runs in a sandboxed worker child rather than on the server, which holds every secret and the DB. | Medium-high |
| D6 | "One job per CPU" | Slots = min(cores ÷ threads-per-slot, memory budget ÷ model peak RSS). Defaults to 4 threads per slot. | See §7.3. One thread per job makes per-job latency ~4× worse and multiplies memory by the core count. | Medium (needs measurement; a `bench` subcommand is part of v1) |
| D7 | Default `admin` login | `admin` is created on first start with a random password, printed once to the log (or taken from `WHISPER_ADMIN_PASSWORD`), and must be changed at first login | A known default password on a network service is the most common way these get owned. | High |
| D8 | Worker has a UUID | The **server** assigns the UUID on `POST /worker` and returns it with a secret | POST-to-collection semantics. The server owns identity, and a client-chosen ID can't collide with or squat on another worker's. | High |
| D9 | "First untaken across all queues" | Two-level weighted fair queueing: by queue weight (admin-set), then by job weight class within a queue, then FIFO (§8.2) | Strict FIFO lets one backlog starve every other queue for days. | High on the need, medium on parameter defaults |

---

## 3. Architecture

```
            ┌──────────────────────────── server ─────────────────────────────┐
 clients ──▶│ /queue  /job  /client    ─┐                                     │
 (API token)│                           ├─▶ authz ─▶ store (SQLite, WAL)       │
 browsers ─▶│ /ui  (session cookie)    ─┤                │                    │
            │                           │   blobs/ (original, canonical FLAC, │
 workers ──▶│ /worker /lease /models   ─┘           transcripts)              │
 (worker    │   scheduler: weighted fair pick in the claim txn                │
  token)    │   dispatcher: wakes long-polls when tasks appear                │
            │   reaper: expires leases, purges jobs/media                     │
            │   obs: zerolog JSONL → stdout; /metrics (+ optional push)        │
            └────────────────────────────────────────────────────────────────┘

 worker process (one per machine)
   ├─ N slot goroutines: claim ─▶ download ─▶ sandbox-exec {ffmpeg | whisper-cli} ─▶ PUT result
   ├─ heartbeat per active lease (carries worker gauges to the server)
   ├─ model cache (volume), fetched from server /models/{name}
   └─ JSONL logs → stdout; optional metrics push/scrape
```

### Components

| Component | Responsibility |
|---|---|
| `store` | All SQLite access. One writer connection plus a read pool, WAL mode, `busy_timeout`. Claims run in one `BEGIN IMMEDIATE` transaction. |
| `blobs` | Content stored on disk under `data/blobs/{job-id}/`. The DB holds paths, sizes and sha256 hashes, never bytes. |
| `media` | Pure Go: WAV/FLAC validation and decode (`mewkiz/flac`), energy analysis, split-point selection, WAV range serving. No cgo, no ffmpeg on the server. |
| `sched` | Virtual-time bookkeeping and candidate selection (§8.2). Pure functions over a snapshot, so it is unit-testable without SQLite. |
| `dispatcher` | Wakes waiting long-poll claims when a task becomes available. In-memory only; correctness never depends on it. |
| `reaper` | Periodic job: expires leases, requeues or fails tasks, purges media and jobs past retention, deletes stale pending workers. |
| `auth` | Password, session, client-token and worker-token authentication. One `Can(principal, action, resource)` function. |
| `ui` | Server-rendered `html/template` pages with htmx for small interactions. No JS build step. |
| `obs` | zerolog setup (VictoriaLogs field names), event names, metrics registry, push loop. |
| `worker` | Slot loop, lease heartbeats, `sandbox-exec` supervision, model cache. |

### Why exec `whisper-cli` and `ffmpeg` instead of cgo bindings

- If a child crashes or is OOM-killed, the worker survives and reports a failure.
- Priority, memory limits, thread count and sandbox are set per child process.
- Engine versions are pinned in the image and reported in provenance, separate from the Go release cadence.
- The Go binary stays pure Go. `modernc.org/sqlite` keeps it cgo-free, so it cross-compiles and the server image is static.

Trade-off: the worker image ships three binaries (`whisper-network`, `whisper-cli`, `ffmpeg`), which bends "one program" slightly. The Go binary is still the only thing anyone runs.

---

## 4. Domain model

### 4.1 Entities

| Entity | Identity | Notes |
|---|---|---|
| **Queue** | human-readable name, `[a-z0-9][a-z0-9-]{0,62}` | `public` is created at bootstrap. Has a `weight` (admin-set). Can be renamed but not deleted while it has jobs. |
| **Job** | UUIDv7 | Client-facing. Owns the media, parameters, `weight` class, metadata, the assembled transcript and the download log. |
| **Task** | `{job}/{seq}` | Internal. `kind` is `fetch` (seq −1, URL jobs only), `prepare` (seq 0, optional) or `transcribe` (seq 1…N, a sample range of the canonical audio). |
| **Lease** | random 128-bit ID | One attempt by one worker at one task. Expires unless heartbeated. Fencing token. |
| **Worker** | server-assigned UUID | Registered machine. States: pending, active, paused, rejected. Its **kind** (`processing` or `downloader`) is fixed at registration (§7.6). |
| **User** | username | Human UI login. |
| **Client** | ID plus bearer token | API principal. Created by a user in the UI or via `POST /client`. |
| **Grant** | (principal, role, scope) | All authorization data lives here. |

### 4.2 State machines

**Worker**

```
            approve                 pause
 pending ───────────▶ active ◀───────────▶ paused
    │                   │       resume        │
    └──── reject ───────┴──── reject ─────────┴──▶ rejected (token revoked)
                                                      │
                                                      └── delete (operator, or reaper after retention) ──▶ gone
```

- **pending**: may only `GET /worker/{id}` and wait. Claims return 403 with `state: pending`.
- **paused**: running leases continue to completion. New claims return 409 `paused`. The worker keeps polling slowly so it notices when it is resumed. An administrator pauses a worker, or the circuit breaker does (below).
- **rejected**: the token is revoked and active leases are cancelled; the next heartbeat returns 410, and the worker kills its child and exits non-zero. Terminal. A new registration is a new worker.
- **deleted**: a rejected worker is deleted by the reaper after `--rejected-worker-retention` (§8.5), or at once by a `worker_admin` over it (`DELETE /admin/workers/{id}`, or Delete in the UI, #94). Only a rejected worker can be deleted (`409` otherwise), so an active or paused worker is rejected first, which cancels its leases: deletion never strands a live lease. Deletion takes the worker's grants, the grants scoped to it and its work counters; the audit entry keeps the counters. Afterwards its token is simply unknown (`401`) instead of getting the "rejected, exit" answer, so a worker process somehow still running would re-register as a fresh `pending` worker, which grants nothing. A `pending` worker cannot be deleted by hand: one still running would at once re-register, and one that is gone is pruned by `--pending-worker-ttl`; reject it instead.

**Circuit breaker (auto-pause).** A bad worker (a broken build or model, a failing disk, too little memory) must not keep failing jobs, so the server takes it out of service by itself. Each worker has a **failure streak**: its leases that ended `failed` with `retryable` set, or `expired`, since its last completed task. A non-retryable failure is the input's fault (ffmpeg rejecting an upload as invalid data), not the worker's, so it neither counts nor resets the streak; releases and cancellations do neither either. A **pre-work** failure (below) is always the worker's own and counts, retryable or not. A completed task (a transcribe result or an accepted prepare) ends the streak.

When an **active** worker's streak reaches `--worker-failure-limit` (default 3) failures **spanning at least two distinct jobs**, the transaction that ended the lease also moves the worker to `paused`, with `decided_by = system` and a stored reason such as `3 consecutive failures across 2 jobs`, and appends the `worker.pause` audit entry (actor `system`, the reason in its detail, §5.2.1). The two-job rule means one poisoned input that crashes every worker it meets cannot take a healthy worker out of service. It is waived when the failure that reaches the limit is a pre-work one, which cannot be the input's fault: a misconfigured worker is paused even while only one job is queued (reason e.g. `3 consecutive failures in 1 job, the last before work started`). The server then logs `worker.auto_paused` and counts `whisper_worker_auto_paused_total` (§10). The paused worker finishes its current leases (they still count in the streak but cannot pause it again) and claims nothing new; the Workers page shows it as auto-paused with the reason. Resuming it (an administrator's decision, since the cause is usually on the machine) clears the reason and the streak. `--worker-failure-limit 0` turns the breaker off; the streak is still kept and shown.

The breaker complements **retry elsewhere** (below): retry elsewhere moves a failed task to another worker, and the breaker stops the failing worker from taking further tasks in the meantime. The streak lives in `worker_failure_streak`, one row per (worker, job) with the failures in that job, so deleting a job drops its share.

**Job** (derived from its tasks, but stored for cheap querying)

```
 fetching ──▶ preparing ──▶ queued ──▶ running ──▶ done      (fetching: URL jobs only; uploads start at preparing or queued)
     │            │            │          │
     └────────────┴────────────┴──────────┴──▶ failed     (a task failed for good, and no lease of the job is live any more)
 failed ──retry──▶ fetching (URL job whose media is gone) | preparing (prepare never succeeded) | queued     (its retained media still present)
 any state ──DELETE──▶ (row and blobs removed; active leases cancelled)
```

**Failing jobs: one bad task does not throw away the rest.** A task *fails for good* when it runs out of attempts, fails non-retryably, or waits out the retry wait limit. Its job can then no longer be done, but the work already running for it is kept:

- The job's `pending` tasks become `cancelled` at once, so no new lease is issued for the job.
- Its `leased` tasks run to completion on their workers: heartbeats still work, and a result is stored as usual (`done`). If such a lease ends without a result (failed, expired, released, or cancelled by worker rejection or grant revocation), its task becomes `cancelled` instead of requeuing.
- The job keeps its state (`running`, or `preparing`) and gets its `error` at once, named after the first task that failed for good; a client polling it sees the error early. It becomes `failed` in the transaction that ends its last live lease (a result, failure, release, cancellation, or the reaper's expiry); a reaper sweep is the backstop. A job with no other live lease fails at once, as before.
- Deleting the job, or revoking a worker's access, still cancels leases immediately.
- A failed job's `error` ends with its progress, `…; 2 of 12 tasks done, 4 cancelled` (transcribe tasks; omitted for a job that failed preparing), and `tasks` counts `done`, `failed` and `cancelled` accurately, so a client can tell what a retry (#56) would save. The `done` tasks' results stay in the database until job retention.

`FailJob` (used when assembly fails) still fails a job outright, cancelling its leases.

**Retrying a failed job** (`POST /job/{id}/retry`, the job page's **Retry** button, `whisper-network retry <job-id>`; #56, #62). A failure on the server's or the fleet's side must never require the submitter to upload again: a retry works from the media the server kept, the canonical audio, or the original if prepare never succeeded (failed-job retention, §8.5). Allowed for the job's owner, `queue_admin` on its queue, and `admin` (`auth.ActRetryJob`); one action serves both the owner's "resume" and the operator's "requeue". In one transaction:

- Only a `failed` job whose needed media is still on disk qualifies; anything else is `409` with problem type `not-retryable` and the reason (not failed; media purged), and nothing changes. The job document's `retryable` says which beforehand.
- `done` tasks and their results are kept. `failed` and `cancelled` tasks go back to `pending` with `attempts = 0` and no worker.
- The retry-elsewhere exclusions are cleared (the operator is asserting the cause is fixed) without deleting history: `jobs.retries` counts the job's retries and each lease records the count at claim time (`leases.retry`). Attempt counting, the claim's exclusion and the stranded-task rule consider only leases of the current retry, so the ended leases stay for debugging.
- `error`, `completed_at` and `compute_ms` are cleared; the job becomes `preparing` (no canonical audio yet) or `queued`, and `retries` goes up. The scheduler's reactivation rule is applied and waiting claims are woken.
- Audited as `job.retry` with the previous error; logged as `job.retried`.

The retry and the reaper's media purge both run under the job lock, and the reaper purges only a job it re-reads as `done` or `failed`, so a purge can never remove the media of a job that a retry has just made live.

`preparing` is skipped when the upload is already canonical. Transcribe tasks are created when the canonical audio is accepted.

**URL jobs: the `fetching` state and the `fetch` task.** A job created by `POST /queue/{q}/fetch` (§6.1) starts in `fetching` with one `fetch` task (seq **−1**, so seq 0 stays the prepare task and a fetch task sorts before it), holding the URL and its **site key** (§8.6). Only a downloader leases it (§7.6); its result is the fetched media (`PUT /lease/{id}/result`, streamed, §6.2). The server stores it exactly as an upload (`.part`, hash, size cap, fsync, rename), refuses it if it is a web page or API answer rather than media (HTML, XML, JSON or text, sniffed from the leading bytes), then makes the upload's ingest decision: the canonical fast path (`queued`, transcribe tasks) or `preparing` with a prepare task. A fetch task takes part in weighted fair queueing like any other (§8.2). **A fetch claim sets `jobs.started_at`** (not the first prepare or transcribe claim), so `wait_s` measures the time to fetch start for a URL job.

A fetch failure is handled like any other: the worker's reason becomes the task and job error, and its `stderr_tail` is logged with `task.failed`. For a yt-dlp failure the reason ends with yt-dlp's last `ERROR:` line and the tail is yt-dlp's stderr; for a plain GET it carries the Go error text. Both may quote the URL, so they reach the job's error and the server's logs (§10.1). A result the server itself rejects fails with `invalid result: ` and one of `not media`, `too large` or `unusable media`.

| Downloader outcome | Reported as | Server action |
|---|---|---|
| Rate limited or bot check (HTTP 429; yt-dlp's `HTTP Error 429: Too Many Requests`, `Sign in to confirm you're not a bot` or YouTube's session rate limit, matched only as yt-dlp phrases them at the start of an `ERROR:` message, so page data quoted later in it cannot put a site into cooldown) | `stage: "rate_limited"` | Requeue without using an attempt or excluding the worker; it does **not** count toward the worker's failure streak; the (egress group, site) pair goes into cooldown (§8.6). |
| Media gone, private, geo-blocked, 404/410, unsupported content | `retryable: false` | Fail the job at once (the input's fault). |
| Network error, 5xx, timeout | `retryable: true`, stage `input` | The normal attempt and retry-elsewhere rules. |
| yt-dlp missing, sandbox setup failure, work directory error, egress proxy down, a sandbox that cannot permit proxy connects (Linux below Landlock ABI 4, §7.6) | stage `pre-work` | The pre-work rules above (refunded, counts toward the breaker). |
| Result too large (`413`), not media (`422`), media ffmpeg-class validation refuses | decided by the server | The lease fails non-retryably with the reason above. |

A fetch task that is only ever rate limited would requeue forever, so `--retry-wait-limit` applies: **the wait is measured from the first `rate_limited` refusal of the current retry** (not from the last), and past it the job fails (`rate limited`). If the last eligible downloader for a queue disappears (grant revoked, worker rejected or deleted, or below the fetch sandbox floor) while URL jobs are `fetching`, those jobs fail with `no downloader holds fetch on queue X`, in the same transaction where practical, with a reaper sweep as the backstop.

**Retrying a URL job.** While its retained media (original or canonical audio) is on disk a retry follows the upload rules above. When the media is gone (the fetch never succeeded, or retention purged it) a retry **re-fetches** from the stored URL: the job returns to `fetching`, its canonical audio, transcribe tasks and their results are discarded and a new fetch task is created. A failed URL job is therefore `retryable` until job retention removes its row. The new fetch may yield different bytes; that is accepted.

**Task**

```
 pending ──claim──▶ leased ──result──▶ done
    ▲                 │
    └── expired/released/failed (attempts < max), or failed pre-work ──┘
                      ├── failed (attempts == max, or not retryable) ──▶ failed
                      ├── lease ends without a result while the job is failing ──▶ cancelled
 pending ──waited retry_wait_limit after failing (reaper, §8.2)──▶ failed
 pending ──a sibling failed for good (the job is failing)──▶ cancelled
```

**A bad worker must never cause a failed job.** A failure is often the worker's fault (a broken model file or build, too little memory, a flaky disk), so a task is never retried on a worker that already failed it, and its attempts count workers, not leases:

- **Attempts count distinct workers.** `attempts` is the number of distinct workers on which the task failed or whose lease on it expired, plus one while it is leased. The default `max_attempts` is 2: a task that failed on two different workers is abandoned rather than retried forever. A failure and an expiry on the same worker count once. A lease the worker releases voluntarily (shutdown, drain), one cancelled by the server (worker rejected, grant revoked), or a pre-work failure does **not** count.
- **Retry elsewhere.** A worker never leases a task again after failing it or letting its lease on it expire (§8.2). A released lease does not exclude its worker.
- **Input failures fail at once.** A failure the worker attributes to the input (`retryable: false`, e.g. ffmpeg's "Invalid data found when processing input") fails the job immediately: another worker would fail the same way.
- **Wait limit.** A task that failed and has since waited `--retry-wait-limit` (default 24 h) without any other eligible worker taking it fails its job (§8.2).
- **Pre-work failures are refunded.** A failure report carries a `stage`: `pre-work` (before the worker fetched the input or started a child: a missing tool, a model not in its table or failing to download, an `input_url` on another origin, its work directory), `input` (downloading or checking the input) or `child` (running ffmpeg or whisper-cli, reading their output, uploading the result). A `pre-work` failure says nothing about the task, so the task requeues without counting an attempt and without excluding the worker, retryable or not; it counts toward the worker's failure streak, so the circuit breaker pauses a misconfigured worker instead of letting it fail anyone's job. The lease records the stage (`leases.stage`). Reports without a stage (older workers), or with one the server does not know, behave as before; so does every stage when the breaker is off (`--worker-failure-limit 0`), since nothing would then stop a worker from taking and failing the same task forever.
- **Repetition loops are retried without context** (issue #99). Whisper sometimes locks onto one short line and repeats it to the end of its task; the conditioning on previous text keeps the loop going, which is why the next task starts clean. The server refuses a transcribe result in which one line (compared ignoring case, spacing and surrounding punctuation; blank segments skipped) appears **more than 50 times in a row**, or **more than 50 times and in over 30% of the segments**. The refusal is a `422 result-invalid` whose reason gives only counts, never the repeated text (it becomes the task's error, which is logged and visible without transcript permission, §10.1), and a failed lease with server-assigned stage `loop` (a worker cannot report it). It counts as an attempt, so the retry goes to another worker and a second loop fails the job, but it does **not** count toward the worker's failure streak: the model looped, not the machine. The task's next lease carries `no_context: true` and no `prompt`, and the worker runs whisper-cli with `-mc 0` (no previous-text conditioning). The server drops the prompt itself, so a worker that predates `no_context` still retries without it.

When a job fails, its error names the workers its task failed on and the job's progress, e.g. `transcribe task 6 failed after 2 attempt(s) (on workers A, B): out of memory; 2 of 12 tasks done, 4 cancelled`, or `transcribe task 1 failed after 1 attempt(s) (on worker A); no other eligible worker took it within 24h: out of memory; 0 of 1 tasks done, 0 cancelled`.

### 4.3 Schema (SQLite)

```sql
CREATE TABLE queues (
  id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL, description TEXT,
  weight INTEGER NOT NULL DEFAULT 100 CHECK (weight BETWEEN 1 AND 10000),
  max_upload_bytes INTEGER, created_at INTEGER NOT NULL);

CREATE TABLE users (
  id INTEGER PRIMARY KEY, username TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,            -- argon2id
  must_change_password INTEGER NOT NULL DEFAULT 0,
  disabled_at INTEGER, created_at INTEGER NOT NULL);

CREATE TABLE sessions (
  id TEXT PRIMARY KEY,                    -- sha256 of cookie value
  user_id INTEGER NOT NULL REFERENCES users ON DELETE CASCADE,
  csrf TEXT NOT NULL, expires_at INTEGER NOT NULL);

CREATE TABLE clients (
  id TEXT PRIMARY KEY, name TEXT NOT NULL,
  token_hash TEXT UNIQUE NOT NULL,        -- sha256; tokens are 256-bit random
  created_by TEXT NOT NULL,               -- 'user:<id>' | 'client:<id>'
  created_at INTEGER NOT NULL, last_used_at INTEGER, revoked_at INTEGER);

CREATE TABLE workers (
  id TEXT PRIMARY KEY, token_hash TEXT UNIQUE NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending','active','paused','rejected')),
  hostname TEXT, description TEXT,
  cpu_model TEXT, cores INTEGER, mem_bytes INTEGER, os TEXT, arch TEXT,
  engine_version TEXT, ffmpeg_version TEXT,
  sandbox_level TEXT,                     -- 'full' | 'fs-only' | 'none' (§7.4)
  kind TEXT NOT NULL DEFAULT 'processing' CHECK (kind IN ('processing','downloader')),  -- fixed at registration (§7.6)
  egress_key TEXT NOT NULL DEFAULT 'unknown',                                    -- downloaders (§8.6)
  egress_group TEXT NOT NULL DEFAULT '', egress_ip TEXT NOT NULL DEFAULT '',     -- '' when not reported
  ytdlp_version TEXT NOT NULL DEFAULT '',     -- facts' yt_dlp_version
  slots INTEGER, models TEXT,             -- JSON
  registered_at INTEGER NOT NULL, decided_by TEXT, decided_at INTEGER,
  last_seen_at INTEGER, gauges TEXT,      -- JSON, latest gauges from a heartbeat or claim
  pause_reason TEXT);                     -- set while paused by the circuit breaker (§4.2)

CREATE TABLE grants (
  principal TEXT NOT NULL,                -- 'user:<id>' | 'client:<id>' | 'worker:<id>'
  role TEXT NOT NULL,
  scope TEXT NOT NULL,                    -- '*' | 'queue:<id>' | 'worker:<id>'
  granted_by TEXT NOT NULL, granted_at INTEGER NOT NULL,
  PRIMARY KEY (principal, role, scope));

CREATE TABLE jobs (
  id TEXT PRIMARY KEY, queue_id INTEGER NOT NULL REFERENCES queues,
  client_id TEXT NOT NULL, idempotency_key TEXT,
  state TEXT NOT NULL CHECK (state IN ('fetching','preparing','queued','running','done','failed')),
  weight TEXT NOT NULL DEFAULT 'medium' CHECK (weight IN ('low','medium','high')),
  model TEXT NOT NULL, language TEXT NOT NULL, prompt TEXT, label TEXT, source TEXT,
  metadata TEXT,                          -- opaque client JSON, ≤ 8 KiB
  original_bytes INTEGER, original_sha256 TEXT, original_type TEXT,  -- sniffed
  original_purged_at INTEGER,
  canonical_bytes INTEGER, canonical_sha256 TEXT, audio_samples INTEGER,
  canonical_purged_at INTEGER,
  created_at INTEGER NOT NULL, started_at INTEGER, completed_at INTEGER,
  compute_ms INTEGER, error TEXT,
  first_downloaded_at INTEGER, last_downloaded_at INTEGER,
  download_count INTEGER NOT NULL DEFAULT 0,
  retries INTEGER NOT NULL DEFAULT 0,        -- times the failed job was retried (§4.2)
  fetch_url TEXT, fetch_site TEXT,           -- URL jobs only: the URL and its site key (§8.6); NULL for uploads
  UNIQUE (client_id, idempotency_key));
CREATE INDEX jobs_queue_state ON jobs (queue_id, state, created_at);
CREATE INDEX jobs_queue_state_order ON jobs (queue_id,       -- ?sort=state (§6.1)
  (CASE state WHEN 'running' THEN 0 WHEN 'queued' THEN 1 WHEN 'preparing' THEN 1 WHEN 'fetching' THEN 1 WHEN 'failed' THEN 2 ELSE 3 END),
  (CASE WHEN state IN ('failed','done') THEN -COALESCE(completed_at, created_at) ELSE created_at END), id);

CREATE TABLE tasks (
  job_id TEXT NOT NULL REFERENCES jobs ON DELETE CASCADE, seq INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('fetch','prepare','transcribe')),
  start_sample INTEGER, end_sample INTEGER,   -- NULL for prepare
  cost REAL NOT NULL,                         -- scheduling cost (§8.2)
  state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
  result TEXT,                                -- JSON segments (transcribe), times relative to task start
  engine_version TEXT, worker_id TEXT, compute_ms INTEGER, error TEXT,
  pending_since INTEGER,                      -- when it last became pending (UI only; NULL: the job's creation)
  PRIMARY KEY (job_id, seq));
CREATE INDEX tasks_pending ON tasks (job_id, seq) WHERE state = 'pending';

CREATE TABLE leases (
  id TEXT PRIMARY KEY, job_id TEXT NOT NULL, seq INTEGER NOT NULL,
  worker_id TEXT NOT NULL, created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL,
  ended_at INTEGER, outcome TEXT,             -- done | expired | released | failed | cancelled
  heartbeat_at INTEGER,                       -- latest heartbeat, NULL until the first (UI only)
  stage TEXT,                                 -- failed only: pre-work | input | child | rate_limited, as reported (§4.2)
  egress_key TEXT,                            -- fetch leases: the egress group it ran from (§8.6)
  retry INTEGER NOT NULL DEFAULT 0,           -- the job's retries when claimed; only the current retry's failures count (§4.2)
  FOREIGN KEY (job_id, seq) REFERENCES tasks ON DELETE CASCADE);
CREATE INDEX leases_live ON leases (expires_at) WHERE ended_at IS NULL;
CREATE INDEX leases_task_failures ON leases (job_id, seq, worker_id, ended_at)  -- retry elsewhere (§8.2)
  WHERE outcome IN ('failed','expired');

CREATE TABLE worker_stats (                   -- per-worker work counters
  worker_id TEXT PRIMARY KEY REFERENCES workers ON DELETE CASCADE,
  prepare_done INTEGER, transcribe_done INTEGER,  -- completed tasks by kind
  jobs INTEGER,                               -- distinct jobs with a task completed by the worker
  audio_ms INTEGER,                           -- audio of completed transcribe tasks
  prepare_compute_ms INTEGER, transcribe_compute_ms INTEGER,
  failures INTEGER, expired INTEGER,          -- leases ended 'failed' / 'expired'
  first_task_at INTEGER, last_task_at INTEGER);  -- first / last completion

CREATE TABLE throughput (                     -- fleet throughput (§6.3, #87)
  bucket INTEGER NOT NULL,                    -- Unix minute of the completions
  queue_id INTEGER NOT NULL,                  -- the job's queue
  audio_ms INTEGER NOT NULL,                  -- audio of transcribe tasks completed
  tasks INTEGER NOT NULL,                     -- transcribe tasks completed
  PRIMARY KEY (bucket, queue_id)) WITHOUT ROWID;

CREATE TABLE worker_failure_streak (          -- circuit breaker (§4.2)
  worker_id TEXT NOT NULL REFERENCES workers ON DELETE CASCADE,
  job_id TEXT NOT NULL REFERENCES jobs ON DELETE CASCADE,
  failures INTEGER NOT NULL,                  -- failed (retryable) and expired leases in this job
  PRIMARY KEY (worker_id, job_id)) WITHOUT ROWID;

CREATE TABLE sched_vtime (                    -- §8.2
  queue_id INTEGER NOT NULL REFERENCES queues ON DELETE CASCADE,
  class TEXT NOT NULL,                        -- '' = queue level; 'low'|'medium'|'high'
  vtime REAL NOT NULL DEFAULT 0,
  PRIMARY KEY (queue_id, class));

CREATE TABLE fetch_policies (                 -- per-site fetch policy (§8.6); seeded, then edited in the UI
  key TEXT PRIMARY KEY,                       -- site, or site@egress_key; '*' is the default
  site TEXT NOT NULL, egress_key TEXT,        -- NULL: every egress
  rules TEXT NOT NULL,                        -- JSON array of "N/DURATION"
  concurrency INTEGER NOT NULL, backoff_initial_s INTEGER NOT NULL, backoff_cap_s INTEGER NOT NULL,
  updated_by TEXT NOT NULL, updated_at INTEGER NOT NULL);
CREATE TABLE fetch_starts (                   -- fetch claims, for rate windows (not tied to jobs, so deleting jobs cannot reset a window)
  egress_key TEXT NOT NULL, site TEXT NOT NULL, started_at INTEGER NOT NULL);
CREATE TABLE fetch_cooldowns (                -- active pauses per (egress group, site)
  egress_key TEXT NOT NULL, site TEXT NOT NULL, until INTEGER NOT NULL,
  level INTEGER NOT NULL DEFAULT 0, manual INTEGER NOT NULL DEFAULT 0,
  set_by TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (egress_key, site));

CREATE TABLE audit (
  ts INTEGER NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL,
  target TEXT, detail TEXT);
```

Notes:
- Timestamps are Unix milliseconds in UTC.
- Migrations are embedded (`embed.FS`), applied on start, and versioned with `PRAGMA user_version`. **Migration 0012** (URL downloads) rebuilds `jobs` and `tasks` (SQLite cannot alter a CHECK) with the `-- whisper:foreign-keys-off` directive, which runs that migration with foreign keys off, following SQLite's documented rebuild procedure; it also adds the worker, lease and fetch-pacing columns and tables above and seeds the two default policies (§8.6). Servers opening the same database concurrently skip already-applied migrations lock-free.
- Invariant: at most one live lease per task (`ended_at IS NULL`). The claim transaction enforces it, and a test pins it (§11).
- **Worker counters** (`worker_stats`) are updated in the transaction that ends a lease, so they always agree with task outcomes. A completion (transcribe result or accepted prepare) counts its task by kind, its compute time, its audio (transcribe only, so prepare time does not skew the realtime factor) and its job, if no other task of that job is already done by the worker. That check needs no extra table: a done task never changes state or worker again and goes only with its job, so each (worker, job) pair counts once. `failed` and `expired` leases count as failures and expiries (pre-work failures included); a release or a cancellation (worker rejected, grant revoked, job deleted) counts as nothing. They are durable and built in; the Prometheus metrics (§10.2) stay for time series. `GET /admin/workers` returns them as `stats`; the workers table summarises them (tasks done, audio hours, average realtime factor, failure rate) and each worker's page shows them in full. They go with the worker when it is deleted (pruned, or deleted on demand once rejected, §4.2); the `worker.prune` or `worker.delete` audit entry keeps them.
- **Fleet throughput** (`throughput`) is counted the same way: a completed transcribe task adds its audio to the bucket of the minute it completed, for its job's queue, in the transaction that ends its lease. It does not refer to the job, so deleting a job (which clients do as soon as they have the transcript) does not change it, and a job split into many tasks contributes as each one completes. The table holds at most one row per minute and queue with work, and the reaper deletes rows past 24 h (§8.5), so it does not grow with traffic. The migration that added it backfilled from the done leases then kept. Prometheus already has the same total as a counter (`whisper_worker_audio_seconds_total`, §10.2); this table is the durable, server-side figure the dashboard reads.
- **Counter backfill.** The migration that added the counters filled them from what was then retained: completions from done tasks (kept as long as their job, `job_retention`), timed by their done lease if it was still kept, else by their job's completion; failures and expiries from ended leases (kept 30 days). Older history is not counted.

---

## 5. Authorization

### 5.1 Principals and authentication

| Principal | Credential | Transport |
|---|---|---|
| User | username + password, then a session cookie (`HttpOnly`, `Secure`, `SameSite=Lax`) plus a CSRF token on mutating forms | `/ui` |
| Client | `Authorization: Bearer wnc_<base62>` | API |
| Worker | `Authorization: Bearer wnw_<base62>` | API |

- The prefixes let secret scanners recognize leaked tokens. Tokens are 256-bit random values stored as sha256. That is enough for high-entropy secrets; argon2 is only needed for passwords.
- A token is shown exactly once, at creation.
- A user may also call the API with a session, so the UI and API share handlers. While the user's `must_change_password` is set (an administrator chose or generated the password), every API route refuses the session with `403` `password-change-required`; the UI handles the change itself. Creating a session re-checks, in the same transaction, that the password hash the login verified is still current and the user is not disabled, so a reset or disable that lands mid-login wins. A self-service password change does the same: the new password is set only if the hash the current password was checked against is still current and the user is not disabled, so an administrator's reset or disable that lands between the check and the change is not overwritten.

### 5.2 Roles

| Role | Scope | Allows |
|---|---|---|
| `admin` | `*` | Everything, including creating users and queues, **setting queue weights**, and granting roles. |
| `queue_admin` | `queue:X` | Approve or reject workers **for queue X** (i.e. grant `process` on X), grant and revoke `submit`, `submit_url`, `view_all`, `delete_all`, `process` and `fetch` on X, list all jobs on X and read their status documents (as `view_all`), delete any job on X. **Does not read transcripts**: the transcript and result need `read_all` as well. Cannot grant or revoke `queue_admin` (queue admins cannot make or remove other queue admins) or `read_all`. Cannot change X's weight, because weight is a fleet-wide resource decision. |
| `worker_admin` | `*` or `worker:W` | Approve, pause, resume and reject the worker(s) in scope, and delete them once rejected. With scope `*`, may also grant `worker_admin` on `worker:<id>`. Cannot grant queue access unless also `queue_admin` for that queue. |
| `submit` (client) | `queue:X` | Create jobs on X. See, read and delete its own jobs on X. |
| `view_all` | `queue:X` | List all jobs on X and read their full status documents (`GET /queue/X`, `GET /job/{id}`): state, timings, task counts, label, source, weight, model, errors, `prompt`, `metadata` and `submitted_by`. Not the transcript or result (`403`). For monitoring and management: operators, dashboards, agents that track or review what other agents submitted. |
| `read_all` | `queue:X` | Read the transcript and result of every job on X (`GET /job/{id}/transcript`, `GET /job/{id}/result`). Implies `view_all` on X (a reader can list and see what it may read). Granted and revoked only by `admin`. |
| `delete_all` (client) | `queue:X` | Delete any job on X. *Open item; see §13.* |
| `client_admin` | `*` | Create and revoke clients via `POST /client`. When creating a client it may hand on grants it holds itself (no escalation); this re-delegation applies only at creation. Revoking a client requires authority over every grant that client holds. |
| `process` (worker) | `queue:X` | Lease tasks (prepare and transcribe) from X. Only a processing worker may hold it. |
| `submit_url` (client) | `queue:X` | Create URL jobs on X (`POST /queue/X/fetch`). The same own-job rights as `submit` (see, read, delete, retry). It does not imply `submit`, nor `submit` it. Grantable by `admin` and `queue_admin` on X, and delegable at client creation like the other queue roles. |
| `fetch` (worker) | `queue:X` | Lease fetch tasks from X. Only a downloader may hold it. Granted by `queue_admin` on X: approving a downloader for a queue is the same kind of trust decision as `process`. Revoking it cancels the downloader's live fetch leases on that queue, as for `process`. |

**Approving a worker is two separate acts.** Lifecycle is `worker_admin`. Data access is `queue_admin`. Data access is the trust decision, because a worker receives that queue's media. The UI shows both on one approval screen, together with the worker's `sandbox_level`. Clicking "Approve for queues A, B" works only if the actor holds both rights for every queue chosen.

A worker in `active` state with no `process` (or, for a downloader, `fetch`) grants is legal and simply idle. **Worker kind is fixed at registration** (`kind: "downloader"` in the facts, stored immutably on the worker row): the server refuses `fetch` grants to processing workers and `process` grants to downloaders, and the claim enforces the same split independently of grants (`fetch` tasks go only to downloaders, `prepare` and `transcribe` only to processing workers).

**Seeing a job is not reading its transcript.** Who may do what with a job on queue X:

| | List and status (incl. prompt, metadata, submitter) | Transcript and result | Delete |
|---|---|---|---|
| The job's owner | yes | yes | yes |
| `view_all` on X | yes | `403` | no |
| `read_all` on X | yes | yes | no |
| `queue_admin` on X | yes | `403` | yes |
| `delete_all` on X | no (`404`) | no | yes |
| `admin` | yes | yes | yes |

The transcript and result are the sensitive part of a job: engine output from untrusted media, which can carry meeting content and prompt injection. Everything describing the job (prompt, metadata, who submitted it) is visible to anyone who can see the job, so a manager can review what was submitted; reading the output is a separate grant, and `queue_admin` (which manages a queue's workers and access) does not imply it. A database created before `read_all` existed is migrated (schema version 3) by giving every existing `view_all` and `queue_admin` grant a `read_all` grant on the same queue, so upgraded deployments keep the access they had; revoke those `read_all` grants to narrow it.

**Grant management.** Granting to, or revoking from, an existing principal requires `admin`, or `queue_admin` on queue X for `submit`, `submit_url`, `view_all`, `delete_all`, `process` and `fetch` on `queue:X` (not `read_all`: a queue admin does not itself read transcripts, so it cannot hand that out), or (for `worker_admin` on `worker:<id>` only) `worker_admin` on `*`. Revoking needs exactly the authority that granting would. Revoking a worker's `process` grant also cancels, in the same transaction, the worker's live leases on every queue it no longer holds `process` on (outcome `cancelled`; the task requeues without using an attempt, as on rejection), so withdrawn access to a queue's media takes effect at once. Holding a role never by itself lets you hand it on: re-delegating your own grants is allowed only when creating a client (`client_admin`, above). Revoking a client's token requires `client_admin` plus authority over every grant the client holds, so a client admin cannot revoke a client more privileged than itself. Grant listings do not reveal whether a principal exists.

#### 5.2.1 Audit

Every change to authorization or to stored state that a person, client or worker causes is recorded in the audit log (`GET /admin/audit`, filterable by action prefix, actor, target and time; the `/ui/audit` page). **A change and its audit entry commit in the same database transaction:** if the audit entry cannot be written, the change fails and nothing is applied. Changes the server makes itself (retention, pruning, session expiry, media purges, the circuit breaker's auto-pause) use the actor `system`; the offline admin command uses `admin-cli`.

| Action | Recorded when | Actor |
|---|---|---|
| `grant.add` / `grant.remove` | A grant is given or taken away | the granting principal |
| `client.create` / `client.revoke` | An API client is created or its token revoked | the acting principal |
| `user.create` / `user.disable` / `user.reset_password` | An administrator manages a UI user | the acting principal |
| `user.change_password` | A user changes their own password (all their sessions end) | the user |
| `auth.login` / `auth.logout` | A UI session starts (including the rotation after a password change) or ends | the user |
| `auth.login_failed` | A sign-in is refused. The target names the user only if the username exists, so a password typed into the username field is never stored | `anonymous` |
| `session.expire` | The reaper deletes expired sessions (one entry with the count) | `system` |
| `queue.create` / `queue.patch` / `queue.rename` | A queue is created or changed | the acting principal |
| `worker.register` | A worker registers (`POST /worker`) | the worker |
| `worker.update_facts` | A worker replaces its reported facts | the worker |
| `worker.approve` / `worker.pause` / `worker.resume` / `worker.reject` | Worker lifecycle and queue access decisions | the acting principal |
| `worker.pause` (auto) | The circuit breaker pauses a worker (§4.2); the detail carries the `reason`, `failures` and `jobs` | `system` |
| `worker.delete` | A rejected worker is deleted on demand (API or UI, single or "Delete all rejected", one entry per worker). Like `worker.prune`, the detail keeps the worker's final counters if it did any work | the acting principal |
| `worker.prune` | The reaper deletes a pending worker past `--pending-worker-ttl` or a rejected worker past `--rejected-worker-retention` (one entry per worker) | `system` |
| `job.create` | A job is created by upload or URL (a replayed idempotency key is not a new entry). A URL job's detail carries the site key, never the URL | the submitting principal |
| `fetch_policy.put` / `fetch_policy.delete` | A fetch policy is created, replaced or deleted (§8.6) | the acting administrator |
| `fetch_cooldown.set` / `fetch_cooldown.clear` | An administrator sets or clears a cooldown (§8.6) | the acting administrator |
| `job.delete` | A job is deleted by its owner, by a principal with `delete_all`/`admin`, or by retention | the deleting principal, or `system` |
| `job.purge_media` | An original upload or canonical audio is deleted (§8.5) | `system` |
| `job.retry` | A failed job is retried (§4.2); the detail carries the `previous_error` | the retrying principal |
| `job.bulk_retry` / `job.bulk_delete` | A bulk action ran (§6.1), one summary entry on the queue with the outcome counts and the filter; each changed job also has its own `job.retry` / `job.delete` | the acting principal |

Audit details never contain secrets or requester content: no passwords, tokens, prompts or metadata.

**Not audited: the task and lease lifecycle.** Claims, heartbeats, result uploads, task completion and failure, lease expiry and release, and transcript download counts are recorded in the structured logs (§10.1: `task.leased`, `task.done`, `task.failed`, `lease.expired`, …) and the metrics (§10.2), not in the audit table. This is deliberate. They are high-volume machine events, one per task per attempt and one per heartbeat, that would bury the audit log and grow it without bound. The audit table is for decisions: changes made by people, clients or workers to authorization and stored state, security-relevant events (sign-ins, refused sign-ins), and the system's own retention and pruning actions. What those machine events can affect is bounded by audited decisions (who may submit, which worker may process which queue), and lease cancellations caused by an audited change are counted in that change's audit entry (`grant.remove`, `worker.reject`).

### 5.3 Visibility rules

- `GET /queue/{q}` lists jobs where `client_id = me`, or every job if the caller holds `view_all`/`read_all`/`queue_admin`/`admin` on q.
- `GET /job/{id}` returns **404, not 403**, when the caller can't see the job, so job IDs can't be probed.
- A job the caller can see but not read (`view_all` or `queue_admin` without `read_all`, not the owner) is listed and its full status document returned (including `prompt`, `metadata` and `submitted_by`); its transcript and result are **403** (the job's existence is already known to the caller).
- Workers can't see jobs at all. They see only their own leases.
- **Which worker runs a job** (the UI's job table and job page) is shown only to `admin`, `queue_admin` on the job's queue, or `worker_admin` over that worker. Anyone else who can see the job (its owner, `view_all`) sees only that its tasks are running. A worker's description and hostname are operator-chosen and can name machines, people or places; a submitter has no need of them. A worker's page lists its current leases, but names the job only to principals who may view it.

---

## 6. HTTP API

All bodies are JSON unless noted. Errors use RFC 9457 `application/problem+json` with a stable `type` slug (`lease-expired`, `worker-paused`, `media-invalid`, …). Paths are unversioned. A breaking change would add `/v2` beside them; that is YAGNI until it happens.

### 6.1 Client API

| Method & path | Purpose | Auth |
|---|---|---|
| `POST /queue/{q}` | Create a job. Multipart body: part `params` (JSON) and part `media` (any audio or video). Optional `Idempotency-Key` header. Returns `201` with `Location: /job/{id}` and the job JSON. | `submit` on q |
| `POST /queue/{q}/fetch` | Create a URL job (below): JSON body of the job params plus `url`. Optional `Idempotency-Key`. Returns `201` with `Location: /job/{id}` and the job JSON in state `fetching`; `409 no-downloader` when no eligible downloader exists. | `submit_url` on q |
| `GET /queue/{q}` | List visible jobs. Query: `state`, `since` (created or updated after), `limit`, `cursor`, `sort`, `order` (below). | `submit`, `submit_url`, `view_all` or `read_all` on q |
| `POST /queue/{q}/bulk` | Retry or delete many jobs of q (below). | per job, as the single-job action |
| `PATCH /queue/{q}` | Change `weight`, `description` or `max_upload_bytes`. | `admin` (weight); `queue_admin` (the others) |
| `GET /job/{id}` | Job status (below), including `prompt`, `metadata` and `submitted_by`. | owner, or `view_all`, `read_all` or `queue_admin` on its queue |
| `GET /job/{id}/transcript` | `text/plain` transcript with the provenance header. Records a download. `409` if not done, unless `?partial=1` (§8.4). | owner, or `read_all` on its queue |
| `GET /job/{id}/result` | JSON segments with absolute times, for consumers that want structure. Also records a download. | same |
| `DELETE /job/{id}` | Delete the job, its blobs and live leases. `204`. | owner, `delete_all`, or `queue_admin` |
| `POST /job/{id}/retry` | Retry a failed job (§4.2). `200` with the job document; `409 not-retryable` if not failed or its media was purged (a URL job is fetched again instead). | owner, `queue_admin`, or `admin` |
| `POST /client` | Create a client with a set of grants (a subset of the caller's). Returns the token once. | `client_admin` |
| `GET /client/{id}`, `DELETE /client/{id}` | Inspect or revoke. | `client_admin` or self (GET) |

**URL jobs: `POST /queue/{q}/fetch`.** The body is the job params below (`model`, `language`, `weight`, `prompt`, `label`, `metadata`, `source`) plus `url`. `source` defaults to the URL. The server does **no network access** to validate: the scheme must be `http` or `https`, there must be no userinfo and a host, the length is at most `api.MaxSourceLen` (2048), and the host must have a registrable domain (it becomes the site key, §8.6) or be an IP literal (keyed by its canonical form, e.g. `127.0.0.1` or `2001:db8::1`). It does **not** judge address ranges, not even of IP literals: the downloader's egress filter is the single place that decision is made (§13), so a downloader run with `--allow-private-egress` can fetch `http://192.168.1.10/…`. Error details never quote the URL.

**No downloader: `409`, problem type `no-downloader`**, when no worker holds `fetch` on the queue (or `*`) that is `active` or `paused` and at or above `--fetch-min-sandbox` (§11.1). A downloader that is merely offline does not trigger it: the job waits like any queued task. The `fetch` lease carries `url`, `site` and `max_output_bytes` (below). CLI: `whisper-network submit --url URL` (other flags as for a file).

**Job create params**

```json
{
  "model": "small.en",               // required; must be on the server's model allowlist
  "language": "en",                  // default "en"; "auto" allowed
  "weight": "medium",                // optional: low | medium | high (default medium)
  "prompt": "This is a project planning meeting. Alice and Bob discuss the budget. …",   // ≤ 800 chars
  "label": "meeting-2026-01-15",
  "source": "https://youtube.com/watch?v=…",   // recorded only; a URL job is submitted as "url" (above)
  "metadata": { "requester": "…", "session": "…" }   // opaque, ≤ 8 KiB
}
```

Model choice stays with the client. The rule "`en` → `small.en`, anything else → `large-v3-turbo`" is client policy and lives in the client.

**Job status**

```json
{
  "id": "0193…", "queue": "public", "state": "running", "weight": "medium",
  "model": "small.en", "language": "en", "label": "…", "source": "…", "metadata": {…},
  "media": { "original_bytes": 1020000000, "original_type": "video/mp4",
             "duration_s": 12890.4, "original_purged": true, "audio_purged": false },
  "tasks": { "total": 5, "pending": 1, "leased": 2, "done": 2, "failed": 0 },
  "created_at": "…", "started_at": "…", "completed_at": null,
  "wait_s": 41.2, "elapsed_s": 1830.0, "compute_s": 2950.7,
  "downloads": { "count": 0, "first": null, "last": null },
  "error": null
}
```

The document is the same for everyone who may see the job (§5.2), including `prompt`, `metadata` and `submitted_by` (`{kind: "client"|"user", id, name}`); only the transcript and result need `read_all`.

The document also carries `retries` (how many times the job was retried), `retryable` (a failed job whose retained media is present) and, for a failed job with its media, `media.kept_until` (when failed-job retention lets the reaper purge it; a full disk can purge it sooner, §8.5). `media.audio_purged` is true once the audio a retry would need is gone: the canonical audio, or for a job that never got canonical audio, its original.

**List order** (#67). `GET /queue/{q}` lists oldest first by default, unchanged for existing clients and cursors. `sort=created&order=desc` lists newest first (`order=asc` is the default). `sort=state` lists running jobs, then `queued`, `preparing` and `fetching`, all oldest first (the scheduling order), then `failed`, then `done`, both most recently finished first; `order` does not apply to it. Paging is by keyset: a cursor encodes its order's key (`created_at, id`, or the state group, the time within the group and `id`), the page is chosen on the jobs table alone, and the state order is served by the expression index `jobs_queue_state_order` (no sort step, checked by a test of the query plan). A cursor of one order is rejected (`400`) by another. Under `sort=state` a job that changes state between two pages moves to its new group, so it can be listed twice (it moved to a later group) or not at all (it moved to an earlier one); jobs that keep their state are never skipped or repeated. The UI's queue page defaults to `sort=state`, has an order control, and remembers the choice per browser in a cookie.

**Bulk retry and delete** (#68). `POST /queue/{q}/bulk` with `{"action": "retry"|"delete", "job_ids": [...]}` or `{"action": …, "filter": {"state": "failed", "since": "<RFC 3339>", "cursor": "…"}}`, plus optional `"dry_run": true`. A filter selects as `GET /queue/{q}` would, oldest first, among the jobs the caller can see; `state` is required. At most 500 jobs per request: `job_ids` beyond that is `400`, and a larger filter selection returns `next_cursor` to continue. Each job is authorized as its single-job action (retry: owner, `queue_admin`, `admin`; delete: owner, `delete_all`, `queue_admin`, `admin`), so a bulk request never widens what the caller could do one job at a time; each runs in its own transaction, so one job's failure does not undo the others. The response lists every job's outcome, `retried`, `deleted`, `skipped` with a `reason` (`media purged`, `not failed`), `forbidden` (visible but not allowed), or `not_found` (unknown, on another queue, or not visible: as for single jobs, existence is not revealed), plus `counts` by outcome; a dry run reports `would_retry` / `would_delete` and changes nothing. Each changed job gets its own `job.retry` / `job.delete` audit entry and the request one `job.bulk_retry` / `job.bulk_delete` summary entry on the queue (none for a dry run). Retried jobs re-enter the fair scheduler like any other queued job. The UI's queue page has a checkbox per row, "select all on this page", and, with a state filter set, "all N matching"; it asks for confirmation with the count and then shows the per-job outcomes and a summary such as `3 retried, 1 skipped: media purged`. The CLI is `whisper-network retry --queue Q [--since T] [--dry-run]` and `whisper-network delete --queue Q --state S [--dry-run]`.

"How long it took" is reported three ways: `wait_s` (created → first lease), `elapsed_s` (first lease → completed), and `compute_s` (sum of task compute time, including prepare). With parallel tasks, compute can exceed elapsed.

**Upload handling (server, streaming)**
- Stream to `blobs/{job}/original.part` while hashing and enforcing `max_upload_bytes` (default 2 GiB, since video is now accepted; raise `--max-upload-bytes` for larger media; a worker preparing it needs about its size plus 1.6 GiB free above `--disk-reserve`, and prepare tasks go only to workers with that room, §7.2). Fsync, rename, then commit the DB row. A crash leaves at most an orphan `.part`, which the reaper deletes.
- **Admission.** A client (or user) may have at most `--max-uploads-per-principal` uploads in progress (default 2); more get `429` with `Retry-After`. Free disk is reserved as the media bytes arrive, at most 1 MiB ahead of the writer and never from the declared `Content-Length`, so a slow upload holds no more than what it has sent plus one step; near the floor the writer reserves just the bytes in hand rather than a whole step. A `Content-Length` larger than free space above `--min-free-bytes` is refused with `507` before the body is read; an upload that would take free space below `--min-free-bytes` stops with `507`. The body must average at least `--min-transfer-rate` over every sliding window of max(`--read-timeout`, 30 s), so a trickle cannot hold an upload slot and its reservation for hours; a slower body is cut off with `408`.
- **Sniff the type, don't trust it.** Read the leading bytes with Go's `http.DetectContentType` plus checks for RIFF/WAVE and `fLaC`. The client's Content-Type is ignored.
- **Fast path:** if the file is a valid canonical stream, it becomes the canonical audio directly and no prepare task is created. Canonical means PCM WAV or FLAC, 16 kHz, mono, 16-bit, fully decoded by the pure-Go validator.
- **Otherwise:** create a `prepare` task. The server does not judge whether ffmpeg can decode the file; the prepare task fails with ffmpeg's reason if it can't.
- `Idempotency-Key`: the same (client, key) returns the original job with `200` instead of creating a duplicate. This matters because large WAN uploads get retried.

### 6.2 Worker API

| Method & path | Purpose | Auth |
|---|---|---|
| `POST /worker` | Register. Body: facts (below). Returns `201` `{id, token, state:"pending"}`. Rate-limited per source IP; at most `--max-pending-per-source` workers from one address (IPv6: one /64) may await approval at once (`429`), and `--max-pending-workers` in total (`503`). | none |
| `GET /worker/{id}` | Own state and grants; config hints (heartbeat interval, poll interval). | that worker, or an admin |
| `POST /worker/{id}/gauges` | Report the gauges (§10.2) without claiming: sent by a worker whose host is too busy to take work (§7.3), so it stays seen and shows why it is idle. `204`. | active or paused worker |
| `PATCH /worker/{id}` | Update facts (engine versions, models, slots, sandbox level) after restart or upgrade. | that worker |
| `POST /worker/{id}/leases?wait=30s` | Claim one task. A downloader's body asks for `"kinds":["fetch"]`; it needs no models. Body: `{"kinds":["prepare","transcribe"],"models":["small.en"],"gauges":{…}}`, describing what this slot can run **right now** given free memory and disk. The optional `gauges` are the same readings a heartbeat carries (§10.2): heartbeats only flow while a lease is held, so an idle worker reports through its claims. Returns `201` with the lease, or `204` on timeout. | active worker |
| `GET /lease/{id}/input` | Prepare: the original bytes. Transcribe: a WAV stream (16 kHz mono s16) decoded from the canonical FLAC for this task's sample range. `Range` is supported for prepare; transcribe input is regenerated on retry. | lease holder, live lease |
| `POST /lease/{id}/heartbeat` | Extend the lease. Optional body: worker gauges (§10.2). Returns `200 {expires_at}`, or `410` if expired, cancelled or the job was deleted, or if the lease may not be extended any more: the worker is neither active nor paused, no longer holds `process` on the lease's queue, or the lease is past its maximum age (§8.3). The worker must then kill the child. | lease holder |
| `PUT /lease/{id}/result` | Prepare: body is the canonical FLAC (`audio/flac`). Transcribe: segments JSON. Fetch: the fetched media, streamed (chunked, length unknown up front), with `X-Fetch-Method: yt-dlp\|http`; `413` past the lease's byte cap, `422 not-media` for a web page or API answer. Idempotent for the same lease. `410` if the lease is no longer live; `422` if validation fails (counts as a failed attempt). | lease holder |
| `POST /lease/{id}/failure` | `{"reason":"…","stderr_tail":"…","retryable":true,"stage":"child"}`. Ends the lease; the task requeues for another worker if attempts remain and `retryable` (§8.2). `stage` (optional: `pre-work`, `input`, `child`) says how far the worker got; a `pre-work` failure requeues without using an attempt or excluding the worker (§4.2). | lease holder |
| `DELETE /lease/{id}` | Voluntary release on graceful shutdown. Does not consume an attempt. | lease holder |
| `GET /models/{name}` | Model file (ggml), sha256 in `ETag`. The server keeps a cache, filled from upstream on first request and pinned by hash. | any active worker |

**Fetch lease and result.** A fetch lease (`kind: "fetch"`, seq −1) carries `url`, `site` and `max_output_bytes` instead of an input: **`max_output_bytes` is the minimum of the queue's upload limit and the server's `--max-upload-bytes`**; `0` means no cap at either level, so the downloader is uncapped on its side. The server enforces its cap again while receiving (never above `--max-upload-bytes`), so the downloader's count is an early abort, not the guard. A claim of a fetch lease records the start for pacing (§8.6) in the claim transaction; the start stands even if the downloader then refuses the lease (a pre-work failure) or releases it, so such a lease still uses a rate-window slot. The result is streamed to disk exactly like an upload, so a failed upload means a re-fetch on retry; the downloader never writes the media to local disk.

**Registration facts**

```json
{ "hostname": "nuc-01", "description": "garage NUC, nights only",
  "cpu_model": "Example CPU", "cores": 8, "mem_bytes": 8589934592,
  "os": "linux", "arch": "amd64",
  "engine_version": "whisper.cpp 1.9.5", "ffmpeg_version": "7.1.1",
  "sandbox_level": "full", "kind": "processing", "kinds": ["prepare","transcribe"],
  "slots": 1, "models": ["small.en", "large-v3-turbo"] }
```

The description defaults to `$WHISPER_DESCRIPTION` or the hostname. A downloader registers `"kind": "downloader"`, `"kinds": ["fetch"]` and adds `yt_dlp_version`, `egress_group` and `egress_ip` (§8.6); it reports no models, whisper or ffmpeg versions.

**Lease** (claim response)

```json
{ "id": "lse_…", "kind": "transcribe", "job": "0193…", "seq": 2,
  "input_url": "/lease/lse_…/input", "audio_samples": 57600000,
  "model": "small.en", "model_sha256": "…", "language": "en", "prompt": "…",
  "heartbeat_interval_s": 30, "expires_at": "…" }
```

**Transcribe result**

```json
{ "engine_version": "whisper.cpp 1.9.5", "model": "small.en", "model_sha256": "…",
  "vad": "silero-v5.1.2", "language": "en", "compute_ms": 1180000,
  "segments": [ { "start_ms": 0, "end_ms": 4120, "text": "…" } ] }
```

The server validates the result: segment times must be monotonic, within the task's duration, with bounded text size. Times are converted to absolute using the task's start sample.

**Fetch administration (admin only, audited).** Like queue weights these are fleet-wide decisions, so they need `admin`; they are the only way to configure fetch policy (no config file or flag).

| Method & path | Purpose |
|---|---|
| `GET /admin/fetch-policies` | All admin. List every policy row, including invalid ones (`invalid: true` with `invalid_reason`). Durations come back in compact form (`10m`, `12h`). |
| `GET /admin/fetch-policies/{key}` | One policy row, valid or not (`404` if there is none). |
| `PUT /admin/fetch-policies/{key}` | Create or replace a policy: `{rules: ["1/3m", …], concurrency, backoff_initial, backoff_cap}`. `key` is a site (`youtube.com`), or `site@egress_key` (`youtube.com@label:home`), or `*`. Effective immediately. Also the repair for an invalid row. `200` with the stored row, as `GET` returns it (`201` when created); the path's key wins over one in the body. |
| `DELETE /admin/fetch-policies/{key}` | Delete a policy: `204` (`409` for `*`, which cannot be deleted). Also removes an invalid row. |
| `GET /admin/fetch-cooldowns` | Active cooldowns per (egress key, site). |
| `POST /admin/fetch-cooldowns` | Start a manual cooldown: `{egress_key, site, duration, reason}`, `duration` a Go duration, positive and at most `168h`. `201` with the stored row (an automatic cooldown it replaces keeps its level). A manual cooldown never shortens one already in force. |
| `DELETE /admin/fetch-cooldowns/{egress}/{site}` | Clear one (also resets its backoff level): `204`. `{egress}` is percent-encoded (egress keys contain `:`). |

### 6.3 UI (`/ui`)

`/` redirects to `/ui`. Pages:

- **Dashboard**: queue depths, running tasks, workers online, throughput (**audio-hours per hour**: the audio transcribed by tasks completed in the last hour, from `throughput`, §4.3; each task counts when it completes, and deleting its job afterwards does not change the figure), and each queue's **actual vs configured share** (§8.2). A **Capacity** tile shows the fleet's task slots in use / total on online active workers (from each worker's slots and its last heartbeat's busy slots), how many are available, how many are deferred (free, but on a host deferring for load), the split by kind (transcribe; prepare, which needs `--prepare-min-sandbox`), and the slots of paused, pending and offline workers apart; each queue row adds its **eligible slots** (in use / total on the workers that may process it), which tells a queue starved for capacity from one waiting its turn. The same numbers are `whisper_fleet_slots` (§10.2). The unit is the slot until resource-based admission replaces it; the code keeps the tally (`store.FleetCapacity`) separate so the tile can switch units. A **Fleet versions** tile lists the server's own version, each distinct whisper-network version run by active and paused workers with its worker count and skew flag (below), and each distinct whisper.cpp engine version with its count, flagged "mixed" when there is more than one: a job's transcript may then mix engines (§8.4).
- **Workers**: a table, pending first and filterable by state: name, state, queues, sandbox level, whisper-network **version** (engine and ffmpeg versions on hover), busy/total slots, last seen, the work counters' summary (§4.3: tasks done, audio hours, average realtime factor, failure rate; the breakdown on hover), and pause/resume in the row; "Delete all rejected (N)" (confirmed) deletes the rejected workers the viewer may manage, whatever the filter. Each worker has its own page with the facts list (including sandbox level, disk use — model cache, work directories, free space under `--state` — and cached models), approve (queue picker), pause, resume and reject, delete once rejected (confirmed), per-queue `process` grants, its full work counters, and current leases (job, task and lease age). A worker the viewer may not see is a 404.
- **Version skew.** Each worker's version is compared with the server's (`internal/version`, formats from `utils/version.sh`): identical, or the same build spelled with or without the `v`, shows plain; an older worker gets an "outdated" badge; a newer one a "newer than server" note; anything that cannot be ordered "differs". Where both parse, the base version (`X.Y.Z`) is compared first, then the commit count after the tag of a dev build (`v0.5.0-dev+7.g420d428` is 7 commits after `v0.5.0`, so newer than it). Builds tying on both (another hash, or a `.dirty` tree), builds with no reachable tag (`0.0.0-dev+g…`, whose place in history is unknown), other pre-releases and free-form strings "differ". A worker that reported no version shows "—". Tools get the same inputs from `GET /admin/version` (`{"version": "…"}`, for whoever may list workers) and the facts in `GET /admin/workers`. A worker's facts PATCH that changes `version`, `engine_version` or `ffmpeg_version` records the previous value as `old_version`, `old_engine_version` or `old_ffmpeg_version` in its `worker.update_facts` audit entry.
- **Queues**: create, edit (weight is admin-only) and list jobs; per-queue grants. The job list's Workers column names the workers holding a running job's live leases, or for a done or failed job the workers that completed its tasks, each linking to the worker's page (subject to §5.3). The job list is ordered by state by default, with an order control (state, newest, oldest) remembered per browser in a cookie (§6.1, list order), and has per-row checkboxes for bulk retry and delete (§6.1, bulk): "select all on this page" is a small script (`/ui/static/queue.js`, no inline code under the CSP), and with a state filter set "all N matching" acts on the whole filter. A bulk action always asks for confirmation with the count, then shows each job's outcome and a summary.
- **Pending work.** For those who see all of a queue's jobs, the queue page summarizes its pending tasks: the count by kind, the audio still to transcribe, the longest wait, and a table by kind, model and weight class with **why they are waiting**; it shows how many workers are eligible (active, seen in the last 3 minutes, holding `process` on the queue) and their slots in use, with a warning when none is. Each job row counts pending, leased and failed tasks and, when tasks are pending with none leased, how long they have waited. A task's wait is measured from `tasks.pending_since`, set when it is created and whenever it goes back to pending (a requeued lease, a job retry). The reasons are best effort, judged in memory against the queue's eligible workers the way a claim would offer the task: no eligible worker; none offering the job's model; prepare needing a worker at the minimum sandbox level; every able worker already failed it (retry elsewhere, §8.2); every able worker's slots busy; hosts deferring for load. None means it is waiting its turn. Reasons never name a worker.
- **Jobs**: filters, detail view (finished tasks; each running task's worker, lease age, last heartbeat and attempt; and every pending, failed or cancelled task with how long it has waited, its attempts, its last failure and the worker it failed on, and why it is waiting), delete, transcript download; for a failed job, **Retry** (§4.2) while its media is kept and "files kept until …"; for a job that is not done, the partial transcript (§8.4) to those who may read it.
- **Fetch** (`/ui/fetch`, admin): the fetch policies (list, add, edit, delete; invalid rows are flagged with their reason and repaired or deleted here), the active cooldowns per (egress group, site) with manual set and clear, and the downloaders. Handlers sit on the admin API above. The Workers page shows a worker's kind and its egress group; the Queues grant screens offer `submit_url` and `fetch`; job pages show the `fetching` state.
- **Users and clients**: create, disable, grant roles, mint tokens (shown once).
- **Audit log**: every approval, grant, weight change, deletion and token mint.
- **Names and links.** Workers, jobs and API clients are shown by name wherever they appear (tables, the audit log's actors and targets, grant lists, failure reasons), with a short id after the name and the full id in a tooltip, linked to their page: a worker by its description, else its hostname (`tc-3 (54990ea1…2f33)`); a job by its label, else its source, else only its short id (labels over 60 characters are cut, the full label in the tooltip); a client by its name, marked "(revoked)" if it is. One template (`ref`) renders them all, and each page resolves its ids in one query per kind. Visibility follows §5.3: a worker is named only to those who may see who processes the job (elsewhere, those who may list it), others read "a worker"; a job's label and link only to those who may see the job (others get its short id, unlinked); a client's name only to client admins (others read "a client"), except as a job's submitter: every viewer of the job sees the submitting client's name, as in the API's `submitted_by`, linked only for client admins. A pruned worker shows its id, unlinked, "(removed)"; a deleted job "(deleted)". A client's page shows its details (created by and when, last use, revocation, roles), its permissions while it is not revoked, and its recent audit activity.
- **Column, field and tile help.** Every table column, every field of a details list (a job's, worker's, queue's or client's facts, a worker's statistics) and every dashboard tile explains itself: its label carries the help as a tooltip and an ⓘ button (keyboard-focusable, `aria-describedby` the help text) that toggles the same text as a popover through `/ui/static/ui.js`, so it works on touch screens and with screen readers; some link to the section of this document that explains more. The text lives in one map (`internal/ui/colhelp.go`) used through the `th`, `dt` and `tile-label` templates, which share one ⓘ partial; a label without help fails to render, a test walks every page's headers, terms and tiles, and another checks the templates themselves so a field shown only in a rare state is covered too. State badges carry their meaning as a tooltip.

---

## 7. Worker design

### 7.1 Lifecycle

1. Load `state/credentials.json` from the state dir (default `/var/lib/whisper-network`). If it is missing, `POST /worker` and save it with mode 0600.
2. Probe facts: cores (cgroup quota aware), memory (cgroup limit aware), `whisper-cli --version`, `ffmpeg -version`, available models, and the sandbox level (§7.4). A downloader (§7.6) probes yt-dlp and its sandbox level instead.
3. While `pending`, poll `GET /worker/{id}` with backoff (30 s → 5 min). Log the ID and description once, so the operator knows what to approve.
4. Once `active`, start N slot goroutines (§7.3).
5. On SIGTERM: stop claiming, then by default `DELETE` every live lease and kill the children, so tasks requeue immediately instead of waiting out the lease timeout. With `--drain`, finish running tasks first.

**Without a state volume**, every container restart registers a new worker and the pending list fills with dead entries. Mitigations: the docs say to mount the volume, and the reaper deletes `pending` workers unseen for 7 days. Rejected workers stay for the audit trail until `--rejected-worker-retention`, unless an operator deletes them sooner (§4.2).

### 7.2 Slot loop

```
loop:
  while hostBusy(): report gauges; sleep ~30s   (§7.3)
  kinds, models := whatFitsInFreeMemory()
  lease := POST /worker/{id}/leases?wait=30s {kinds, models}
  if 204: continue
  start heartbeat goroutine (every heartbeat_interval; on 410 → cancel ctx)
  switch lease.kind:
    prepare:    download input (Range resume) → sandbox-exec ffmpeg → PUT result (FLAC)
    transcribe: ensure model (download, verify sha256) → download WAV → sandbox-exec whisper-cli → PUT result (JSON)
  on child error: POST failure with stderr tail
  cleanup tmp
```

- No local queueing. A slot claims only when it is free, and the download happens inside the lease, covered by heartbeats. Prefetching would be local queueing and is deliberately left out.
- Heartbeats run independently of the child process, so a long whisper run never starves them.
- **Prepare command:** `ffmpeg -nostdin -hide_banner -loglevel error -i in -map 0:a:0 -vn -sn -dn -ac 1 -ar 16000 -sample_fmt s16 -c:a flac -compression_level 5 out.flac`. The first audio stream is picked; selecting among multiple streams is a later concern.
- **Transcribe command:** `whisper-cli -m model -f in.wav -l lang -t threads --prompt … -oj -of out`. The JSON output is converted to the result schema.

**Disk.** Everything a worker writes lives under `--state`: the model cache (`models/`, 0.5–3 GB per model, kept across tasks) and one work directory per running lease (`work/<lease>-…/`, removed when the lease ends; the whole of `work/` is wiped at startup, so a crashed child's leftovers do not outlive the process).

- **Admission.** Before running a lease the worker estimates its footprint: prepare, `input_bytes` for the original plus the FLAC, counted as the smaller of `input_bytes` and the prepare child's `RLIMIT_FSIZE` (the lower of the worker's limit and the lease's `max_output_bytes`, the server's bound on a canonical result: verbatim FLAC of `--max-audio` plus overhead, about 1.6 GiB for 12 h). The FLAC cannot be larger, so a 12 GiB video needs about 13.6 GiB, not 24 GiB; transcribe, the WAV (`44 + 2 × audio_samples`), plus the model and VAD model if not yet cached; each plus 16 MiB of slack. The lease is admitted only if that fits in `free − --disk-reserve (1 GiB) − (footprints of running leases − bytes they have written so far)`, so concurrent slots cannot be promised the same space. Otherwise the worker releases the lease at once (`DELETE /lease/{id}`: no attempt consumed, the task requeues for a worker with room) and withholds that kind from its claims until the space it needed is free, or for at most 10 minutes so a smaller task of the kind gets a chance. If every kind is withheld the slot waits a poll interval between checks. Without a free-space reading (the probe failed) nothing is refused.
- **Size-aware routing of prepare tasks.** Hand-back alone would let a large original bounce between workers without room, each then withholding all prepare work for up to 10 minutes. So every claim's gauges (§10.2) also carry `prepare_max_input_bytes`, the largest original whose footprint (above) fits in the space available to a new lease now, and the claim offers no prepare task whose original is larger: a large file is only ever leased to a worker that has room for it at that moment. They also carry `prepare_capacity_bytes`, the same bound if the worker were idle (no work directories, nothing committed). A worker learns the server's bound on the result from `GET /worker/{id}` (`max_prepare_output_bytes`) at start, and from each prepare lease. A worker that sends neither (an older worker, or no free-space reading) is offered every prepare task, as before, and its hand-back still applies.
- **A file no worker can hold.** The reaper fails a prepare task that has been pending for `--retry-wait-limit` while its original is larger than the `prepare_capacity_bytes` of every online active worker that may take it (prepare kind and sandbox level, and `process` on its queue). The job fails with an error giving the original's size and the largest capacity, so the operator knows to add disk or a larger worker. It is never failed while no such worker is online, or while one of them reports no capacity (it might fit there).
- **Model cache cap.** `--model-cache-max` (default 0: unlimited) bounds `models/`. Whenever a download completes or a task releases a model, least-recently-used model files that no task holds are deleted until the directory fits. Last use is the file's modification time, touched on every use, so the order survives restarts. Partial downloads and models in use are never evicted; a cap smaller than the models in use at once is exceeded rather than failing a task.
- **Reporting.** The worker reports `models_bytes`, `work_bytes`, `state_free_bytes`, `prepare_max_input_bytes`, `prepare_capacity_bytes` (above) and the cached model list in its gauges (§10.2), measured by listing `models/`, walking `work/` (a few files per running lease) and `statfs` on `--state`, reused for 5 s and re-measured when the worker itself creates or removes a work directory or fetches a model.

### 7.3 Concurrency: is "one job per CPU" right?

**No.** whisper.cpp is multithreaded per job, and the encoder is GEMM-bound. Running one job per core with one thread each:
- multiplies memory by the core count (each process loads its own model and KV cache), and
- makes each job ~4–8× slower, so a 3-hour meeting takes most of a day.

Throughput per core does fall off above roughly 4–8 threads per process, because memory bandwidth saturates. So several mid-width jobs beat one job that spans every core.

**Rule:**

```
threads_per_slot = config (default 4)
slots_by_cpu     = max(1, floor(usable_cores / threads_per_slot))
slots_by_mem     = floor((mem_limit − reserve) / peak_rss(largest model offered))
slots            = min(slots_by_cpu, slots_by_mem)
```

- `usable_cores` and `mem_limit` come from the cgroup limits when present, so the rule respects what the container was actually given.
- Per-claim memory accounting means a slot only offers a model whose peak RSS fits in the memory not already reserved by other slots. A big-memory model never lands on a box that can't hold it.
- Prepare tasks are ffmpeg-bound and short (minutes of CPU per hour of video, reasoned). They use a slot like any task, with a small fixed memory reservation.
- `peak_rss` is a per-model table shipped with the binary and refined by `bench`.

**Host load admission.** Slots are sized for the whole machine, but a worker often shares its host. Its children run at idle priority, so they yield to other work, but a starved task still holds a lease that an idle worker could finish sooner. So before each claim a slot checks the host's CPU utilisation **excluding the worker's own process tree**, and does not claim while it is above `--max-host-load` (default 70%).

- **Measurement.** The busy share (everything but idle and iowait) of all CPU time in `/proc/stat`, minus the CPU time of the worker's own tree, averaged over the last minute from cumulative counters (samples taken when read, at most one a second). The tree's time is the sum over the worker and its live descendants (found through `/proc/<pid>/stat` parent links) of `utime + stime + cutime + cstime`: a reaped descendant's time moves into its parent's `cutime`, so each process is counted once. Subtracting the tree is the critical detail: without it, a worker running one 4-thread task on an 8-core box (50%) would refuse a second claim at 70%. The reading is host-wide, also in a container with a CPU quota: the question is whether the machine is busy, not whether the worker's own quota is used.
- **Hysteresis.** A slot that stopped claiming resumes when the load falls below `--max-host-load-resume` (default 60%), so slots do not flap around the threshold. A deferred slot re-checks every 30 s ± 20% (jittered, so slots do not stampede), and sends its gauges with `POST /worker/{id}/gauges` each time, since it sends no claims: the server keeps seeing the worker and shows it as "busy host, not taking work". As other load rises fewer slots claim, and none above the threshold.
- **Running tasks are not affected**: they keep going at idle priority. Only claiming is throttled.
- `--max-host-load 0` disables the check. Before the first reading (just after start) a slot waits a second for one; an unknown reading admits. On platforms without `/proc/stat` (macOS, which is CI-only) the load is not measured, and an enabled check is ignored with a warning at startup.
- The worker reports `host_load`, `deferred_for_load` and `claims_deferred` in its gauges (§10.2).

**Measurement (v1):** `whisper-network bench --model small.en --threads 2,4,8` runs a bundled 60 s clip and reports realtime factor and peak RSS. Its output tunes the defaults. The numbers above are reasoned, not measured. Confidence: medium.

### 7.4 Priority and sandbox

Every child runs under `whisper-network sandbox-exec -- <cmd>`. The worker re-execs itself into this wrapper, which applies the restrictions below to itself and then calls `execve` on the target. Landlock domains, the seccomp filter, rlimits, `no_new_privs` and the dropped capabilities survive `execve`, so they apply to the child without affecting the worker.

| Control | Mechanism | Applies to |
|---|---|---|
| Lowest CPU priority | `nice 19`, `SCHED_IDLE` (`sched_setattr`) | both |
| Lowest I/O priority | `ioprio_set` class idle | both |
| Filesystem | Landlock, newest rights the kernel has (go-landlock V11, best effort): read the input, the model, the system library paths, the tool binary and its directory if dedicated; write only the task's directory. Device ioctls (ABI ≥ 5) and connecting to pathname Unix sockets (ABI ≥ 9) are denied too | both |
| Network | seccomp filter: `socket(2)` and `socketpair(2)` fail with `EACCES` for every family (TCP, MPTCP, UDP, raw, netlink, Unix pathname and abstract); `io_uring` is disabled. `socketpair` is refused because an `AF_UNIX` datagram socket from it can `sendto` any pathname socket (`/dev/log`, the journald socket) that Landlock below ABI 9 does not check. The child inherits only `/dev/null` and pipes, never a socket, so `sendto`/`sendmsg` with an address has nothing to send from. Landlock adds TCP (ABI ≥ 4) and UDP (ABI ≥ 10) denial as a second layer | both |
| IPC and signals | Landlock scoping (ABI ≥ 6): no abstract Unix sockets or signals to processes outside the sandbox. Below ABI 6 the seccomp filter allows signals only to the child itself and its process group | both |
| Process tree | The child leads its own process group; seccomp denies `setsid`/`setpgid`, so descendants cannot leave it. Cancel, timeout and normal exit all `SIGKILL` the whole group; `PR_SET_PDEATHSIG` kills the child if the worker dies | both |
| Namespaces | seccomp denies `unshare` and `clone` with any `CLONE_NEW*` flag, and `setns`, with `EPERM`; `clone3`, whose flags a filter cannot read, fails with `ENOSYS`, so glibc (2.34+) falls back to `clone` for threads and `posix_spawn`. The Go runtime uses `clone` directly. No user namespace means no namespace-local capabilities to attack the kernel with | both |
| Privilege | `no_new_privs`; capability bounding, inheritable and ambient sets emptied; a root worker must name a separate `--child-uid`/`--child-gid` (it refuses to start otherwise), which cannot read `credentials.json`. The child uid must be dedicated to the worker's children: the worker refuses to start while `/proc` shows any task of that uid | both |
| Environment | Allow-list: `PATH`, `LANG`, `LC_*`, `TZ`, `GGML_*`, `OMP_*`, `CUDA_*`; `HOME` and `TMPDIR` are the task directory | both |
| Resources | `RLIMIT_AS`, `RLIMIT_FSIZE`, `RLIMIT_NOFILE`, `RLIMIT_NPROC` (below), wall-clock timeout scaled to the input: whisper-cli by audio duration, ffmpeg by size (`--prepare-timeout`, at least 2 h, or `input_bytes ÷ --prepare-min-rate` if longer: 12 GiB at the default 1 MiB/s gets 3.4 h). ffmpeg's `RLIMIT_FSIZE` is the lower of 4 GiB and the lease's `max_output_bytes`; an ffmpeg stopped by it (`SIGXFSZ`, or a write refused with `EFBIG`) fails the task as not retryable, since the media is longer than the server accepts and every worker would stop at the same point | both |
| Files | The worker creates the input and whisper-cli's response file in the child-owned task directory with `O_CREAT|O_EXCL|O_NOFOLLOW`, mode 0600, and `fchown`s them to the child uid. It opens `out.flac`/`out.json` with `O_NOFOLLOW|O_NONBLOCK` and requires a regular file with one link, within a size bound (JSON ≤ 64 MiB, FLAC ≤ `RLIMIT_FSIZE`). Inputs are downloaded only up to the size the lease states (`input_bytes` for prepare, `audio_samples` for transcribe); a lease without it fails, not retryable | — |

- **Checked 2026-11-15:** the default seccomp profiles of Docker (`moby/profiles`) and Podman (`containers/common`) both allow the `landlock_*` syscalls and stacking a seccomp filter after `no_new_privs`. That makes Landlock plus seccomp the unprivileged, in-container mechanism available. `unshare -n` is not an option, because it needs `CAP_SYS_ADMIN` under Docker's default profile.
- **`RLIMIT_NPROC`.** The kernel checks it at `fork` against every task (process or thread) of the child's real uid, in all PID namespaces, and all slots' children share that uid. So the worker gives them one shared allowance, `256 × slots`, rather than a per-child snapshot that another slot's child may already have used up. With `--child-uid` (dedicated, checked at startup) it is the absolute limit (`sandbox-exec --nproc-limit`): together, all children never exceed it. Without a child uid, children run as the worker's own uid, whose other tasks the worker does not own, so `sandbox-exec --nproc` sets the uid's task count at launch, as `/proc` shows it, plus the allowance.
- **The tool's directory** is granted whole only when it is outside the system paths (whisper.cpp's shared libraries and ggml's CPU backends sit next to `whisper-cli`, e.g. `/opt/whisper`). The worker refuses to start if such a directory is under `$HOME` or contains the state directory, or if the state directory is under any read-only path.
- **Residual gaps:**
  - Children of one worker share a uid (the worker's, or `--child-uid`). Each runs in its own Landlock domain, which denies `ptrace` across domains at every ABI and, from ABI 6, signals; but per-uid kernel limits are shared, so one child can exhaust the `RLIMIT_NPROC` allowance of the others (bounded by it, never beyond). The kernel counts across PID namespaces, while the worker sees only its own `/proc`, so another container running as the same uid would go unnoticed by the startup check and narrow the allowance: the child uid must be dedicated.
  - A non-root worker's children run as the worker's own uid. Landlock keeps them away from `credentials.json` by path; a separate uid adds a second layer and needs a root worker with `--child-uid`.
  - The process-group kill relies on seccomp. Where the filter is unavailable (an architecture other than amd64/arm64, or a kernel without seccomp), a descendant could `setsid` away, and the self-test reports the open socket checks.
  - Below Landlock ABI 6, a descendant of the child cannot signal its own pid (`raise` in a grandchild fails); ffmpeg and whisper-cli do not fork.
- **Self-test.** At startup the worker runs a child under a prepare task's spec that tries each escape: read a forbidden file, TCP connect, MPTCP connect, UDP `sendto`, connect to a pathname Unix socket outside the allowed paths, connect to an abstract Unix socket, `socketpair(AF_UNIX, SOCK_DGRAM)` then `sendto` a pathname datagram socket outside the allowed paths (`unix_dgram_sendto`), and `kill(parent, 0)`. The level is `full` only if every check is refused (a protocol the kernel lacks counts as refused), `fs-only` if the file read is refused but some network/IPC check is not, else `none`. The worker logs the Landlock ABI and which checks held.
- **`--prepare-min-sandbox`** (server, default `full`) controls which workers get prepare tasks. Prepare tasks go only to workers that meet this level. Transcribe tasks, whose input is server-generated WAV, go to any level.
- The operator should still give the container a low `--cpu-shares` and a memory limit. This is documented, since the worker cannot set it on itself.

Confidence that this holds across the fleet's kernels: medium-high for kernels with Landlock ABI ≥ 6 on amd64/arm64, where the self-test has been observed to refuse every check; lower elsewhere, which the self-test reports.

### 7.5 Platforms

- Container images: `linux/amd64` and `linux/arm64`, with whisper.cpp built for the CPU baseline. AVX2/NEON dispatch is done by ggml at runtime.
- **macOS:** Docker on Apple Silicon has no Metal, which costs a large factor. The worker therefore also ships as a native darwin/arm64 binary and finds `whisper-cli`/`ffmpeg` on `$PATH` (Homebrew). There is no Landlock on macOS, so `sandbox_level: none` and macOS workers get no prepare tasks under the default policy. That suits them, since they are best spent on transcription.
- **Windows:** the worker is not supported and refuses to start: there is no confinement for its child processes (issue #26). The server and client CLI build for Windows.
- **GPU** workers are out of scope for v1. The slot model generalizes to them (one slot per GPU, a different `peak_rss` budget), so nothing in the API blocks it.

### 7.6 Downloader workers (URL jobs)

`whisper-network worker --downloader` runs the same binary as a different kind of worker. It registers with `kind: downloader` (immutable), offers only `fetch`, needs no ffmpeg, whisper-cli or models (a processing worker needs no yt-dlp), and defaults to 2 slots. Its trust posture is separate from a processing worker's: it talks to arbitrary hosts the client named, but never decodes media.

- **yt-dlp is required.** A downloader without a working yt-dlp reports a `pre-work` failure on every fetch lease (so the circuit breaker pauses it) rather than acting as a GET-only fetcher that would turn a YouTube page into HTML "media". `--yt-dlp` names the binary.
- **Streaming, no local disk.** yt-dlp writes the media to stdout (`-o -`), and the downloader pipes it straight into the result `PUT`, counting bytes against the cap. A plain GET does the same.
- **yt-dlp invocation is fixed** (clients influence only the URL): `--ignore-config --no-playlist --match-filter "!is_live" -f bestaudio --use-extractors default,-generic --no-cache-dir --no-part -o - --proxy http://127.0.0.1:<port> -- <url>`. `-f bestaudio` takes one audio-only stream (no ffmpeg merge); the URL follows `--` so it can never be read as an option. **yt-dlp 2022.11 or later** is required: `--use-extractors default,-generic` is how yt-dlp disables its generic extractor (`--no-generic-extractor` does not exist), so that "no extractor matched" is a reliable signal. Cookies come only from the operator's `--yt-dlp-cookies FILE`, copied into the lease directory per run, never from clients.
- **Plain GET fallback only when yt-dlp reports the URL as unsupported** (an `Unsupported URL` or `No suitable extractor` error and nothing sent). Any other yt-dlp error is classified (§4.2) and never falls back, so a supported site's failure cannot feed its HTML to prepare as "media". The GET goes through the same egress filter and the same port allowlist as the proxy (80 and 443), follows at most 10 redirects (each hop filtered, ports included; only `http` and `https`), must get its response header within a minute, is closed when its connection moves no byte for 2 minutes (as the proxy's idle limit), and is bounded by `--fetch-timeout` (default 2 h, upload included).
- **Content check.** Before streaming, the downloader refuses a body that is not media: a `Content-Type` of `text/*`, JSON, XML, `xhtml` or JavaScript, or leading bytes that are not media by the one shared check (`media.NotMedia`): UTF-16 text (a byte order mark followed by UTF-16 ASCII, checked first because `FF FE` is also an MPEG frame sync), else media by container signature passes, else a leading `<` or `{` past a UTF-8 BOM and whitespace, else what the sniffer calls text, JSON or XML. It reports `retryable: false`. The server applies the same check on receipt (§4.2).
- **Failures are classified for control and reported in full.** Stage and retryability come from yt-dlp's own verdicts (its `ERROR:` lines, with the URL redacted so a URL cannot pick the verdict; progress and warning lines, which echo URL-controlled data, are ignored). The failure report carries the full details: yt-dlp's last `ERROR:` line in the reason, its stderr tail in `stderr_tail`, or a plain GET's error text. Routine worker logs name the site, not the URL; a failure's report and its `task.failed` event may quote it (§10.1).
- **Egress proxy (SSRF filter, §13).** The downloader runs a small loopback HTTP proxy, on a random port, that resolves the host itself and dials only through the address filter. It allows only `CONNECT` and plain `GET`/`HEAD`, and only to ports 80 and 443: the port allowlist belongs to the filtering dialer, which the plain GET (and each of its redirects) uses in-process too. The egress probe's client may also reach its own URL's port. The proxy's port is never 80, 443 or the server's port (the worker picks another), so an egress firewall that allows only those (§13) also blocks the fetch child's direct path around the proxy. A proxy that stops accepting (a permanent accept error) stops the worker, which then exits with an error; fetches meanwhile are refused as pre-work.
- **Sandbox profile.** yt-dlp runs under `sandbox-exec` like any child (§7.4), with these differences. The network is **not** denied wholesale: the spec sets `DenyNet` with `ConnectPorts = [proxy port]`, so the seccomp filter lets `socket(2)` create only `AF_INET`/`AF_INET6` stream sockets with protocol 0 or TCP (no MPTCP, UDP, raw, Unix or netlink), and `socketpair(2)` create only `AF_UNIX` stream pairs, which are connected to each other from birth (a `connect` on one fails with `EISCONN`, and a Unix stream socket refuses a destination address on `sendto`), so they reach nothing but their other end; runtimes need them for in-process wakeups (deno's signal driver aborts without one; Python's asyncio uses one). Datagram pairs stay refused. Landlock (**ABI ≥ 4, Linux ≥ 6.7**) refuses `bind(2)` and every TCP `connect(2)` except to that port. `listen`, `accept` and TCP Fast Open sends (`MSG_FASTOPEN` on `sendto`/`sendmsg`/`sendmmsg`, a connect Landlock's connect hook does not see) are refused by the **seccomp filter**, not by Landlock. DNS (UDP) is therefore blocked inside the child: all name resolution happens in the proxy. Filesystem: read-only system paths, the CA certificates and OpenSSL configuration (`/etc/ssl/certs`, `/etc/ssl/openssl.cnf`, `/etc/ca-certificates`, `/etc/pki/tls/certs`, `/etc/pki/tls/openssl.cnf`, `/etc/pki/ca-trust`; not `/etc/ssl` or `/etc/pki` whole, whose `private/` directories hold host keys), the timezone, the yt-dlp tree (its virtualenv and interpreter when outside the system paths), deno's when installed, and the per-run cookies copy; read-write only the lease's work directory. `RLIMIT_FSIZE` is 256 MiB (the media goes to stdout, so files are small), plus the usual file and process limits, lowest priority, and the fetch timeout. **No `RLIMIT_AS` by default**: deno's V8 reserves tens of GiB of address space at startup and aborts under a 4 GiB limit (measured with deno 2.9.8), which would break every YouTube fetch; `--fetch-address-space` sets one for operators who want it (memory accounting and cgroup limits are unaffected either way). **Below Landlock ABI 4 the child gets no sockets at all** (the spec fails closed, with a warning), and the startup probe (`sandbox.ProbeDownloader`) reports a level below `full`. A downloader there **refuses every fetch lease as pre-work without running yt-dlp** (one warning at startup), rather than burning attempts on fetches that can only fail; this holds whatever `--fetch-min-sandbox` says. A downloader's probe states `full` only when the filesystem is restricted **and** TCP connect is limited to the proxy port: the self-test connects to the proxy port and fails every other attempt, including `listen` and a TCP Fast Open send to another port (`tcp_fastopen`, which also catches a seccomp filter whose argument offsets are wrong on an architecture).
- **Sandbox floor.** The server's `--fetch-min-sandbox` (default `full`, as defined above) gates fetch leases as `--prepare-min-sandbox` gates prepare: fetch tasks go only to downloaders at or above it, and `409 no-downloader` counts only such downloaders. The operator can lower it, but that only helps a downloader whose fetch child is not network-confined at all (non-Linux, where no seccomp or Landlock is applied: level `none`); on Linux below ABI 4 the downloader refuses the leases itself (above), and with the floor lowered it would be offered them and refuse each as pre-work until the circuit breaker pauses it (§4.2). §13 states what lowering it costs.
- **Egress identity.** `--egress-group NAME` and `--egress-probe URL` (§8.6) say which outbound address the downloader shares; `--allow-private-egress` disables the address filter for operators with LAN sources (logged as a warning at startup).
- **Image.** The `downloader` Docker target (`<tag>-downloader`) is the binary on Debian with a pinned, checksum-verified yt-dlp standalone release and a pinned deno (the JavaScript runtime some yt-dlp extractors need); no ffmpeg, no whisper-cli. Its build ends with a smoke test, the hidden `whisper-network downloader-selftest`: it starts the egress proxy, resolves the tools as a downloader does, requires the probe to report `full`, and runs `deno eval` and `yt-dlp --version` under the exact fetch child spec and default limits; the image does not build if either cannot run there. A Linux Go test does the same with whatever yt-dlp and deno are installed. A weekly workflow bumps the yt-dlp pin by pull request, because sites break extractors often; it verifies the release's `SHA2-256SUMS` against its `SHA2-256SUMS.sig` with yt-dlp's release key, committed in the repository (`deploy/yt-dlp-release-key.asc`), before taking any checksum from it.
- **Cookies.** `--yt-dlp-cookies` is readable by the fetch child, so it is the most valuable thing a compromised yt-dlp can exfiltrate (§13), and it makes the downloader a confused deputy: anyone with `submit_url` on a queue it serves can fetch whatever that account can see. Use a throwaway account's cookies, if any.

---

## 8. Server behaviour

### 8.1 Media pipeline

```
upload ─▶ sniff ─┬─ canonical WAV/FLAC (Go-validated) ─────────────────┐
                 └─ anything else ─▶ prepare task (worker, sandboxed)  │
                                        │ PUT FLAC                     │
                                        ▼                              ▼
                               Go-validate FLAC ─▶ store canonical.flac ─▶ purge original
                                                         │
                                         energy scan ─▶ split points ─▶ transcribe tasks
                                                         │
                         GET /lease/{id}/input ◀── decode sample range → WAV stream
```

- **Canonical storage is always 16 kHz mono 16-bit FLAC.** A fast-path WAV upload is encoded to FLAC by the server in pure Go (§8.1.1); a fast-path FLAC upload is decoded fully to validate it and stored as-is. Everything downstream (splitting, serving) deals with one format.
- **Validation of prepare output is a full decode in pure Go** (`mewkiz/flac`), checking sample rate, channels, bit depth, total samples (≤ 12 h) and that the stream decodes without error. Go is memory-safe, so a malicious FLAC from a compromised worker can at worst fail validation or waste CPU, and the CPU is bounded by the size cap.
- **Analysis CPU is admission-controlled.** Validation, analysis and encoding hold one of GOMAXPROCS analysis slots, and one principal may hold at most `--max-analyses-per-principal` of them (default 1); further analyses wait up to 30 s, then get `429`. The decode loops check for cancellation before every frame, so a client that disconnects stops its analysis within one frame. CPU per input byte is bounded too: a fast-path FLAC averaging more than 32 decoded samples per frame byte (metadata blocks such as PADDING are not counted; measured: constant frames of digital silence reach ~340, real speech ~0.8, a noise floor 2–4, 11 s of speech followed by 10 min of digital silence ~38) is not decoded on the server but sent to a prepare task, so a sandboxed worker spends that CPU.
- **Split points:** `target = 60 min` (configurable), `window = ±60 s`. Inside each window, compute RMS energy over 100 ms frames and cut at the quietest frame. There is no overlap, so no de-duplication is needed when stitching. Jobs shorter than `target + window` get a single task. Words cut at a seam can be mangled; fixed-length chunking has the same problem, and cutting at silence makes it rarer.
- **Serving transcribe input as WAV** decoded on the fly (FLAC supports seek): whisper-cli then only ever parses WAV, and the server keeps the compressed copy. Bandwidth is not the binding constraint. WAV is ~115 MB per audio-hour, and an audio-hour takes ≥ 20 min of compute at `small.en`, so it's ~0.1 MB/s per slot.
- **Retention:** the original is purged as soon as canonical audio is accepted. The canonical audio follows `audio_retention` (§8.5).

#### 8.1.1 FLAC: what was checked (2026-11-15)

| Question | Finding | Source |
|---|---|---|
| Can `whisper-cli` 1.9.5 read FLAC? | **Yes.** `examples/common-whisper.cpp` loads all input through miniaudio 0.11 with WAV, MP3 and FLAC decoders compiled in (only device I/O, encoding, etc. are disabled). | whisper.cpp tag `v1.9.5` |
| Does it resample? | **Yes.** The decoder is configured for f32, mono, `WHISPER_SAMPLE_RATE` (16 kHz), so miniaudio resamples any rate. | same |
| Other formats? | Only when built with `WHISPER_FFMPEG`, which links libav **into whisper-cli**. Rejected: that would put the ffmpeg attack surface inside the transcribe child, with no separate prepare step. | `CMakeLists.txt` |
| Can Go **decode** FLAC? | Yes. `mewkiz/flac` v1.0.9 (pure Go) supports decode and sample seek (`NewSeek`, `Seek`). | `mewkiz/flac` |
| Can Go **encode** compressed FLAC? | **Yes, as of v1.0.14** (the pinned version). The encoder runs fixed-predictor analysis (orders 0–4, Rice partition order 0) by default. LPC selection is not automatic, so output is somewhat larger than ffmpeg's (not yet measured). *An earlier revision of this spec said "no"; that was based on v1.0.9.* | `analysis_fixed.go`, `encode_frame.go` |

What follows from this:
- Arbitrary media still needs ffmpeg, which is why prepare runs on workers. The server only decodes and validates FLAC, and encodes FLAC from already-validated PCM WAV. Both are pure Go, so the server never runs a native media parser.
- whisper-cli could take FLAC or MP3 directly, but the design does not rely on that. Feeding it server-generated WAV keeps the child's input surface to a trivial format and makes resampling deterministic, done once by ffmpeg.
- Storage saving: speech FLAC at 16 kHz mono is typically 40–60 % of WAV (reasoned, not measured on our corpus). For a hypothetical 500 h backlog that's ~25–35 GB instead of ~58 GB, transient.

### 8.2 Scheduling: weighted fair queueing

**Inputs**
- **Queue weight** `W_q`: an integer 1–10000, default 100, set only by `admin`.
- **Job weight class** `c ∈ {low, medium, high}`, default `medium`. Class weights `w_c` default to 1 / 2 / 4 (server config).
- **Task cost** = audio seconds × `model_factor` (`small.en` = 1, `large-v3-turbo` = 3; server config, and calibration comes from the realtime-factor metric). A prepare task costs a fixed 60. Cost is computed when the task is created and stored on it.

**Algorithm: two-level start-time fair queueing over virtual time**

Each queue has a virtual time `V_q`, and each (queue, class) pair has `V_qc`. On every claim:

1. **Candidates** are (q, c) pairs that have a pending task this worker may take: q is granted to the worker, the kind is offered, the model is offered, for prepare the sandbox level is met, and the task is not excluded for this worker by the retry-elsewhere rule below.
2. Pick q with the minimum `V_q`. Within q, pick c with the minimum `V_qc`. Within (q, c), pick the oldest job and its lowest pending seq. Ties break by queue ID.
3. Charge the cost: `V_q += cost / W_q` and `V_qc += cost / w_c`.
4. **Reactivation rule:** when a queue goes from having no pending work to having some (job created, or prepare finished), set `V_q = max(V_q, V_min)`, where `V_min` is the minimum `V` over queues that currently have pending work, or the last charged `V` if none do. Classes within a queue follow the same rule. This stops an idle queue from banking credit and then monopolising the fleet when it wakes up.

**Properties**
- While queues A and B are both backlogged and served by the same workers, the cost each receives converges to `W_A : W_B`. The deviation at any moment is bounded by one task's cost. For example, `public=100` and `meetings=300` gives meetings 75 % of the fleet while both have work, and lets either use 100 % alone.
- Within a queue, backlogged classes share 4 : 2 : 1. `low` is never starved, only slowed. This is weighting, not strict priority, as requested.
- Inside a class, ordering is FIFO by job. A huge job ahead of a small one in the same class and queue delays it. Raising the small job to `high` is the remedy.
- **Eligibility caveat:** when workers are granted different queue subsets, each claim is fair among *that worker's* candidates. The fleet-wide shares then approximate the weights rather than hit them exactly; this is the usual limitation of greedy fair queueing with heterogeneous servers. The dashboard and the metrics show actual against configured share, so this is visible rather than hidden.
- Cost is charged at lease time, and an expired or failed lease is not refunded. The re-run is charged again. Refunding would add bookkeeping to fix a rare, small error. See §13.
- Weight changes apply from the next charge; no retroactive correction.

**Retry elsewhere (anti-affinity)**

A task the claiming worker failed, or whose lease expired on it, is never eligible for that worker again. Released and cancelled leases, and pre-work failures (§4.2), do not count. The condition is part of the eligibility fragment, so it applies to the candidate query and to the pick inside the chosen (q, c) alike:

```sql
AND NOT (t.attempts > 0 AND EXISTS (SELECT 1 FROM leases x
  WHERE x.job_id = t.job_id AND x.seq = t.seq AND x.worker_id = :worker
    AND x.outcome IN ('failed','expired') AND x.stage IS NOT 'pre-work'))
```

`t.attempts > 0` skips the lookup for tasks that never failed (a pending task's attempts are the number of workers it failed on), and a partial index on failed and expired leases (`job_id, seq, worker_id, ended_at`) serves the rest.

- **Attempts.** When a lease ends, the task's `attempts` is recomputed as the number of distinct workers among its failed (not pre-work) and expired leases (§4.2). The task fails once that reaches `--max-attempts`, so every attempt it gets runs on a different worker. Because a worker never gets the task back, a repeat on one worker cannot normally happen; counting distinct workers also covers a failure and an expiry recorded for the same worker (e.g. leases from a server version without this rule).
- **Fairness.** An excluded task is invisible to this worker's claim: a (q, c) whose only pending tasks are excluded is not a candidate, so the worker is served from its other candidates instead of getting nothing, and nothing is charged for the skipped task. The retry is charged to its queue when another worker takes it.
- **Only one eligible worker: the wait limit.** Exclusion is permanent, so a task whose every eligible worker has failed it would wait forever. The reaper (§8.5) fails such a task for good, making its job failing (§4.2), once it has been pending for `--retry-wait-limit` (default 24 h) since it last went back to pending, with no worker having taken it since. The error names the workers that failed it and says no other eligible worker took it within the limit. Whether another worker is eligible is not knowable at claim time (it depends on which workers happen to be connected, busy or paused), so the rule is a wait, not a check: a worker that comes online or is approved within the limit still takes the retry. A worker that takes the task and releases it (shutdown, drain) restarts the wait; one that fails it adds itself to the excluded workers. A task that never failed is not affected: queued work waits as long as it needs.
- **Retention.** The reaper's ended-lease pruning (§8.5) keeps the leases of a task that is still pending or leased, whatever their age: they decide who may take it and how many attempts it has left.
- **Why not "retry on the same worker after a grace".** An earlier version let the failing worker retry after a grace period. On a small fleet that turned a broken worker into a failed job: it burned the task's last attempt on the same machine. A failed job is worse than a late one, so the rule became strict, with the wait limit as the bound.
- **Visibility.** A failed job's error names the workers its task failed or expired on (§4.2). `Store.RetryExclusion` reports why a pending task is excluded for a given worker (for queue views).

**Implementation**
- All of this runs inside the claim's `BEGIN IMMEDIATE` transaction:
  1. One grouped query gets the candidate (q, c) pairs and their oldest pending task: `SELECT j.queue_id, j.weight, MIN(j.created_at) … WHERE t.state='pending' AND <eligibility> GROUP BY 1, 2`.
  2. Load the `sched_vtime` rows for those pairs, pick in Go (`sched` package, pure), then update the task, the lease and the vtimes.
- With tens of queues this is microseconds. SQLite's single writer guarantees that no two claims see the same snapshot.
- Long-poll: if there are no candidates, the handler waits on the dispatcher's notification channel or the `wait` deadline, then retries once. Notifications are hints; the transaction is authoritative.

Confidence: high that this gives the fairness asked for. Medium on the default parameters (class weights, model factors), which should be tuned from the `whisper_sched_*` metrics once real traffic exists.

### 8.3 Lease timing

- `heartbeat_interval` = 30 s, and `lease_ttl` = 3 × interval = 90 s. A worker is presumed gone after 3 missed heartbeats. Both values are server config and are sent to the worker in each lease.
- The reaper runs every 10 s. It expires leases with `expires_at < now` (outcome `expired`) and requeues the task for another worker, or fails it when attempts are exhausted; in a failing job (§4.2) the task is cancelled instead, and the expiry of the job's last live lease fails the job. It then fails tasks that have waited `--retry-wait-limit` for a worker that has not already failed them (§8.2), and prepare tasks that have waited as long for a worker with room for their original (§7.2), and fails any failing job left with no live lease (a backstop: lease ends do this themselves).
- **Maximum lease age.** Heartbeats cannot keep a lease alive forever: once a lease is older than max(`--max-lease-age`, `--lease-age-factor` × the task's cost in seconds) (defaults 6 h and 4; cost is audio seconds × model factor, §8.2), heartbeats get `410` and the lease expires, counting as an attempt. This bounds a hung or misbehaving worker's hold on a task while leaving slow machines ample time.
- **Server restart:** on startup, every live lease is extended by one `lease_ttl` before the reaper starts. Workers reconnecting within that window carry on uninterrupted.

### 8.4 Assembly and transcript format

When the last transcribe task of a job is done:
1. Concatenate the segments in seq order, converting to absolute times.
2. Write `blobs/{job}/transcript.txt` (and `result.json`) atomically.
3. Mark the job `done` and set `completed_at` and `compute_ms`.

Transcript format: `[mm:ss.ss] text` lines, the same shape as YouTube caption transcripts, under a provenance header:

```
# transcript-source: whisper; whisper.cpp=1.9.5; model=small.en; vad=…; language=en; transcribed=2026-11-15T14:03:11Z; job=0193…; source=https://…; ffmpeg=7.1.1; tasks=4; workers=2
[00:00.00] …
[61:12.40] …
```

If the tasks ran on different whisper.cpp versions, the header lists all of them (`whisper.cpp=1.9.5,1.9.6`). See §13.

**Partial transcripts** (#66). A failed or unfinished job may have most of its tasks done. `GET /job/{id}/transcript?partial=1` and `/result?partial=1` assemble what exists, on request and without storing it (it changes as tasks finish, and a retried job may still complete): the done tasks' segments as above, and for every transcribe task that is not done a gap marked inline in the text, before the first segment at or after it, e.g. `[... missing 1:03:12-1:58:40 (task 6) ...]` (`H:MM:SS` of the task's audio). The provenance header ends with `; partial=true; missing=K/N` (K of the job's N transcribe tasks missing; `tasks=` counts the done ones); the JSON has `"partial": true` and `"missing": [{"task", "start_ms", "end_ms"}]`, and only real segments in `segments`. Without the flag nothing changes (`409 not-done` until done), so partial output is never mistaken for complete; a done job's `?partial=1` is its full transcript. A job with no transcribe task yet (preparing, or failed while preparing) has nothing to serve and stays `409`. Permissions are those of the full transcript (owner, `read_all`, `admin`), and a partial download is recorded like any other.

### 8.5 Retention (reaper)

| What | When | Config |
|---|---|---|
| Original media | as soon as canonical audio is accepted; for a job that failed preparing, as for a failed job's media (next rows) | — |
| Canonical audio | `completed_at + audio_retention` (default 24 h) | `WHISPER_AUDIO_RETENTION` |
| Failed job's media (the original if prepare never succeeded, else the canonical audio): kept so the job can be retried (§4.2) | at least `completed_at + failed_retention` (default 24 h)… | `WHISPER_FAILED_RETENTION` |
| …unless the disk fills | while the data filesystem's used space is above `failed_retention_max_disk` (default `80%`; a percentage of its size, or an absolute size such as `200GiB`), or its free space is below twice `--min-free-bytes` (the upload guard is near), the reaper purges failed jobs' media **oldest failure first** until the overage is covered. Live jobs' media and done jobs' media are never touched by this rule. Free space comes from the same injectable probe as the upload guard and `/readyz`, and the filesystem's size from its companion | `WHISPER_FAILED_RETENTION_MAX_DISK` |
| Job, transcript and tasks | `completed_at + job_retention` (default 30 d) unless deleted earlier | `WHISPER_JOB_RETENTION` |
| Queued jobs never processed | not purged; they stay visible on the dashboard and in `whisper_queue_oldest_pending_seconds` | — |
| Pending workers | 7 d after registration (polling does not extend it, so a pending worker cannot hold a slot under `--max-pending-workers` indefinitely), with their grants and the grants scoped to them | `WHISPER_PENDING_WORKER_TTL` |
| Rejected workers | 30 d after rejection, with their grants and work counters (§4.3); the `worker.prune` audit entry keeps the final counters of a worker that did any work. Until then the worker's token gets a "rejected" answer telling it to exit; afterwards the token is simply unknown. A `worker_admin` may delete a rejected worker sooner (`DELETE /admin/workers/{id}`, or the UI), with the same effect, audited as `worker.delete` | `WHISPER_REJECTED_WORKER_RETENTION` |
| Tasks that failed and wait for another worker | failed (with their job) after `retry_wait_limit` (default 24 h) pending with no worker taking them (§8.2) | `WHISPER_RETRY_WAIT_LIMIT` |
| Fleet throughput buckets (`throughput`, §4.3) | 24 h (the dashboard reads the last hour) | — |
| Ended leases | 30 d (debugging); never while their task is still pending or leased, since they record which workers failed it (§8.2) | — |
| Orphan `.part` files | on startup and daily | — |

Once a failed job's media is purged it can't be retried: `GET /job/{id}` reports `media.audio_purged: true` and `retryable: false`, a retry is `409 not-retryable`, and the UI no longer offers it. While the media is kept the document says until when (`media.kept_until`) and the job page shows "files kept until …". The reaper purges media only from a job it re-reads, under the job lock, as `done` or `failed`, so a job retried meanwhile keeps its media (§4.2). The gauges `whisper_storage_bytes{kind="failed"}` (media bytes held for failed jobs, a part of the `original` and `canonical` kinds) and `whisper_failed_jobs_retryable` (§10.2) show what the policy holds.

Not implemented: a separate retention for a failed prepare's original beyond the rule above, and a "failed, retryable" filter on the queue page (the job document's `retryable` serves API clients).

Every purge, deletion and prune the reaper makes is audited with actor `system` (§5.2.1).

### 8.6 Fetch scheduling: sites, egress groups and pacing

Fetch tasks are claimed through the same fair-queueing claim (§8.2) with extra eligibility rules, all evaluated inside the claim's transaction so concurrent claims cannot both take the last slot.

**Site key.** The registrable domain (eTLD+1, `publicsuffix`) of the submitted URL's host, with a small alias table (`youtu.be`, `youtube-nocookie.com`, `m.youtube.com` and similar map to `youtube.com`). Redirects do not change it. The site key, not the URL, is what logs, metric labels and audit details carry.

**Egress groups.** Downloaders with the same **egress key** are one group, treated by the scheduler as one IP. The effective key is, in order: `label:NAME` from `--egress-group`; else `ip:A.B.C.D` from `--egress-probe URL` (an endpoint that returns the downloader's public address, re-probed every 10 minutes and on errors); else `ip:` + the source address the server sees, if it is public (what a downloader may dial); else `unknown`, one shared conservative group. The worker page shows it.

**Policies.** A policy is keyed by site, optionally narrowed to one egress group (`youtube.com@label:home`); the most specific wins, and `*` is the default for unlisted sites. A policy holds zero or more **rate rules** `N/WINDOW` (a Go duration, minimum `1s`, maximum `168h`; rules stack, e.g. `1/30s`, `40/1h`, `300/24h`), each a sliding window over fetch starts per egress group; a **concurrency** limit (in-flight fetches per group); and a **backoff** (initial cooldown and cap after a rate limit). Manual limits matter because automatic backoff alone is too late: YouTube blocks an address for about 12 h once tripped, so the server stays under the limit proactively. Policies live in SQLite and are managed **only in the UI or the admin API** (§6.2): there is no file or flag, changes take effect at once, and each is audited. The migration **seeds** `*` (no rate rules, concurrency 2, backoff 5 min doubling to a 6 h cap) and `youtube.com` (`1/3m`, concurrency 1, backoff from 12 h to a 24 h cap).

**Invalid rows fail closed.** The admin API never writes a row that does not validate, but a hand edit might. Such a row blocks what it governs: its site (or its site on that egress, for `site@egress`) is never claimable, it never falls back to a less specific policy, and an invalid `*` blocks every site. It is listed with `invalid: true` and the reason (`GET /admin/fetch-policies`, the UI), counted by `whisper_fetch_invalid_policies`, logged once per change, and repaired with a `PUT` or removed with a `DELETE`.

**Claim eligibility.** A downloader may claim a fetch task for (its egress key, the task's site) only if a valid policy governs the pair (none: blocked, fail closed), no cooldown is active, the in-flight count is under the policy's concurrency, every rate rule has room, the task is on a queue the downloader holds `fetch` on, it is at or above the fetch sandbox floor, and it has not failed that task before (retry elsewhere, §8.2). The claim records the start (`fetch_starts`) and the lease's egress key in the same transaction. The start counts against the rate rules even if the downloader then refuses the lease as pre-work or releases it: pacing errs toward fetching less. A pending fetch task blocked only by time (a rate window, a cooldown) wakes waiting claims through one resettable timer at the earliest moment pacing frees it; concurrency needs no timer because the lease end that frees a slot notifies.

**Rotation.** When several groups' downloaders are long-polling for a site, the one whose group fetched that site **least recently** goes first (a group that never fetched it counts as earliest; equal last starts do not block each other). **Rotation counts only downloaders that are waiting now and could take the task**: the server tracks each long-polling downloader (worker, egress key, the sandbox level its claim reports), and a group is preferred only if pacing allows it and one of *its waiting* downloaders is active, holds the grant, meets the sandbox floor and has not failed that task. A group whose granted downloaders are busy or offline, and whose waiting one cannot serve the site, never holds the others back. After a fetch claim made while more than one group was waiting, the server wakes the waiting claims: the new start changes which group is least recent, so a claim rotation held back looks again instead of sleeping out its long poll.

**Backoff.** A `rate_limited` outcome starts a cooldown for the (group, site) pair of `backoff_initial × 2^level`, capped at the policy's cap (the `*` policy's values, or a built-in fallback, if the governing policy is unusable, so a rate limit always cools the pair down). **Automatic escalation ignores bursts**: further `rate_limited` reports within one minute of the cooldown just set (concurrent fetches that all hit the same block) change nothing, so one incident does not climb the ladder. A successful fetch resets the level (an expired automatic cooldown is removed; an active one keeps its end but drops to level 0). **A manual cooldown is never shortened** by an automatic one; an administrator can set one (an address known to be blocked) or clear one in the UI or API.

**Observability.** Events: `fetch.submitted`, `fetch.done` (site, egress key, method `yt-dlp` or `http`, bytes, duration), `fetch.rate_limited`, `fetch.cooldown`. Metrics (site labels are bounded to the sites named by a policy, else `other`): `whisper_fetch_total{site,outcome}`, `whisper_fetch_bytes_total{site}`, `whisper_fetch_cooldowns`, `whisper_fetch_blocked_tasks` (pending fetch tasks held back by pacing from every active downloader) and `whisper_fetch_invalid_policies`.

---

## 9. Example client integration: a batch automation client

A typical client is a batch script or pipeline step that needs transcripts and does not want to run media tools or models itself. It talks plain HTTP and is unaffected by how many workers the fleet has.

| Client step | With whisper-network |
|---|---|
| Admission (size cap, pending-job cap) | Enforced client-side before anything is sent. |
| Submit | `POST /queue/{q}` with the media file as-is, `Idempotency-Key = the client's own job id`, and metadata `{requester, session, label, source}`. Delete the local file once the upload is accepted. |
| Media conversion and sandboxing | None in the client. ffmpeg runs in a worker's prepare sandbox (§7.4). |
| Waiting | A poller runs `GET /queue/{q}?since=…` every 60 s. Nothing blocks. |
| Completion | On `done`: `GET /job/{id}/transcript` (provenance header included), hand it to the consumer, then `DELETE /job/{id}`. |
| Status | Reads the client's own job table, refreshed from the service. |
| Retry policy | Server `max_attempts`. Failures arrive as `state: failed` with `error`. |

The client gets one token with `submit` on a dedicated queue, so which workers may hear its audio is an explicit `queue_admin` decision. A bulk backlog can be submitted at `weight: low`, so interactive requests at `medium` overtake it within the same queue.

The client's host can also run a worker, so adopting the service never reduces capacity.

---|---|
| Admission (size cap, per-session pending cap) | Enforced harness-side before anything is sent. |
| Submit | `POST /queue/{q}` with the media file as-is, `Idempotency-Key = harness job id`, and metadata `{requester, session, label, source}`. Delete the local file once the upload is accepted. |
| Media conversion and sandboxing | None in the harness. ffmpeg runs in a worker's prepare sandbox (§7.4). |
| Waiting | A poller runs `GET /queue/{q}?since=…` every 60 s. Nothing blocks. |
| Completion | On `done`: `GET /job/{id}/transcript` (provenance header included), deliver it to the requesting agent, then `DELETE /job/{id}`. |
| Status tool | Reads the harness's own job table, refreshed from the service. |
| Retry policy | Server `max_attempts`. Failures arrive as `state: failed` with `error`. |

The harness gets one client token with `submit` on a dedicated queue, so which workers may hear its audio is an explicit `queue_admin` decision. A bulk backlog can be submitted at `weight: low`, so interactive requests at `medium` overtake it within the same queue.

The harness host can also run a worker, so moving to the service never reduces capacity.

---

## 10. Observability

### 10.1 Logs: JSONL for VictoriaLogs

**Format.** One JSON object per line on stdout, from zerolog (the template's logger). At startup, zerolog's global field names are set to VictoriaLogs' defaults (`zerolog.TimestampFieldName = "_time"`, `MessageFieldName = "_msg"`), so `/insert/jsonline` needs no field-mapping arguments:

| zerolog default | Emitted as | Notes |
|---|---|---|
| `time` | `_time` | RFC 3339 nanoseconds, UTC |
| `message` | `_msg` | Short human sentence |
| `level` | `level` | `debug` / `info` / `warn` / `error` (lowercase) |

Every line also carries `service="whisper-network"`, `mode` (`server`/`worker`), `version`, `host`, and on workers `worker_id` once it is known.

**Stream fields.** Set `_stream_fields=service,mode,host` at ingestion (query arg or `VL-Stream-Fields` header on the collector). These are low-cardinality fields. High-cardinality IDs (`job_id`, `lease_id`, `worker_id`) are ordinary fields, never stream fields; making them stream fields would explode VictoriaLogs' stream count.

**Event fields** are flat (no nesting), named consistently across server and worker so a single query can follow a job end to end (`job_id:0193… | sort by (_time)`):

`event`, `job_id`, `task_seq`, `task_kind`, `lease_id`, `queue`, `weight`, `worker_id`, `client_id`, `user`, `model`, `duration_ms`, `audio_s`, `bytes`, `outcome`, `error`.

**Event catalog** (`event` is a stable, dotted name; dashboards and alerts key on it, not `_msg`):

| event | level | Emitted by | Key fields |
|---|---|---|---|
| `http.request` | info (debug for heartbeats) | server | `method`, `route` (pattern, e.g. `/job/{id}`, never the raw path), `status`, `duration_ms`, `bytes_in`, `bytes_out`, `principal`, `remote` |
| `job.created` | info | server | `job_id`, `queue`, `weight`, `model`, `bytes`, `original_type`, `fast_path`, `source`; a URL job has `site` instead of `source`, `bytes` or `original_type` |
| `job.prepared` | info | server | `job_id`, `audio_s`, `tasks`, `canonical_bytes` |
| `job.done` / `job.failed` | info / warn | server | `job_id`, `queue`, `wait_s`, `elapsed_s`, `compute_s`, `error` |
| `job.deleted` / `job.purged` | info | server | `job_id`, `actor` / `what` |
| `job.retried` | info | server | `job_id`, `actor`, `state`, `tasks`, `retries`, `previous_error`, `refetch` (a URL job fetched again, §4.2) |
| `task.leased` | info | server | `job_id`, `task_seq`, `task_kind`, `lease_id`, `worker_id`, `queue`, `cost`, `vtime` |
| `lease.expired` / `lease.released` | warn / info | server | `lease_id`, `worker_id`, `attempt` |
| `task.done` / `task.failed` | info / warn | server + worker | `lease_id`, `compute_ms`, `audio_s`, `rtf`, `error`, `stderr_tail` (≤ 4 KiB); a task the reaper fails at the retry wait limit (§8.2) has no lease and carries `failed_on` (the workers) |
| `worker.registered` / `.approved` / `.paused` / `.resumed` / `.rejected` | info | server | `worker_id`, `hostname`, `actor`, `queues` |
| `worker.auto_paused` | warn | server | `worker_id`, `actor` (`system`), `reason`, `failures`, `jobs`, and the `lease_id` / `job_id` of the failure that tripped it (§4.2) |
| `worker.sandbox` | info / warn | worker | `sandbox_level`, `landlock_abi` |
| `auth.denied` | warn | server | `principal`, `route`, `reason` |
| `admin.change` | info | server | `actor`, `action`, `target` (mirrors the audit table) |
| `reaper.run` | debug | server | counts per action |
| `fetch.submitted` | info | server | `job_id`, `queue`, `site` |
| `fetch.done` | info | server | `lease_id`, `job_id`, `worker_id`, `site`, `egress_key`, `fetch_method` (`yt-dlp` / `http`), `bytes`, `original_type`, `fast_path`, `duration_ms` |
| `fetch.rate_limited` | warn | server | `lease_id`, `job_id`, `worker_id`, `site`, `egress_key` |
| `fetch.cooldown` | warn | server | `site`, `egress_key`, `until`, `cooldown_level`, `manual` |

Unnamed downloader and fetch messages carry: `policy_keys` (server: invalid fetch policy rows, logged when the set changes); `yt_dlp_version`, `egress_group`, `egress_ip_known` (downloader registration); `proxy_port` (egress proxy started); `error_class` (`refused`, `dns`, `timeout`, `status`, `body`, `other`: a failed egress probe); `yt_dlp_outcome` (`ok`, `unsupported`, `rate_limited`, `permanent`, `transient`: a finished yt-dlp run).

**Never logged:** tokens, passwords, cookies, `Authorization` headers, prompt text, transcript text, or the contents of `metadata` (which carries requester names). Their lengths may be logged. `source` URLs of uploads are logged, because they are the redo key and are not secret. **A URL job's URL is never logged** (it defaults into `source` and is job content that can carry credentials in its query string): the worker, the server, the audit log and metric labels carry the **site key** instead, and fetch and URL-job events carry no `source` or URL field at all (the `job.created` event for a URL job omits `source`, which defaults to the URL). The `obs.IsForbiddenField` rules apply to every new field name. The exception is a failed fetch: its `task.failed` event carries the worker's reason and `stderr_tail` in full (§4.2), which may quote the URL.

**Shipping.** stdout only, and a collector ships it. That might be vlagent, Vector, Fluent Bit or the Docker log driver; it sets `_stream_fields`. Workers on ad-hoc machines may have no collector at all. That is acceptable, because everything about a task that matters fleet-wide is already logged by the server: lease events, results, and failures with the stderr tail. Direct push from the binary (`--log-push-url`) is deferred until a fleet machine actually needs it.

Flags come from the template: `--debug` / `--quiet` set the level (default `info`), and `--output-format auto|jsonl|terminal` chooses the format. `auto` gives human-readable output on a TTY and JSONL otherwise, so containers emit JSONL with no configuration.

### 10.2 Metrics: VictoriaMetrics and Prometheus

**Library:** `github.com/VictoriaMetrics/metrics`.
- It writes Prometheus text exposition, so both Prometheus and VictoriaMetrics can scrape it.
- `PrometheusHistogram` produces standard `le` buckets, so `histogram_quantile` works in either system.
- Push is built in (`InitPushWithOptions`, with extra labels and headers) to VictoriaMetrics' `/api/v1/import/prometheus`.

It was chosen over `prometheus/client_golang` because push is native, and it is smaller. The trade-off is that it lacks client_golang's collector ecosystem, which isn't needed here. Confidence: medium-high.

**Server**
- **Scrape:** `GET /metrics` on a separate listener, `--metrics-listen` (default `:9090`), so metrics are never exposed on the public port and need no auth on a private network.
- **Push (optional):** `--metrics-push-url` (e.g. `http://vm:8428/api/v1/import/prometheus`), `--metrics-push-interval` (default 15 s), and `--metrics-labels` for extra `k="v"` labels.

**Workers: zero-config by default**
- Fleet metrics are derived on the server from the protocol, so workers need no metrics config to be visible. The server already knows every lease, result, compute time and realtime factor.
- Heartbeat bodies carry a few worker gauges, which the server re-exports with a `worker` label: slots busy, child RSS, load average, cgroup CPU throttling, disk use under `--state` (model cache, work directories, free space; §7.2), and the host CPU load excluding the worker's own processes with whether the worker is holding back claims for it (§7.3). Claims carry the same gauges, so idle workers, which send no heartbeats, stay current; a worker not claiming because its host is busy sends them on their own (`POST /worker/{id}/gauges`). Disk and host-load series are written only for workers that reported them.
- **Optional, for host detail:** `--metrics-push-url` (same semantics as the server) or `--metrics-listen` for scraping.

**Cardinality rules**
- Never label by job, lease or client.
- `worker` (ID) is allowed; the fleet is expected to stay under ~100 workers. `whisper_worker_info` carries the descriptive labels, which can be joined in queries.
- `route` uses the pattern, not the path.

**Metric catalog**

| Metric | Type | Labels | Why |
|---|---|---|---|
| `whisper_jobs` | gauge | `queue`, `state` | Backlog shape |
| `whisper_tasks` | gauge | `queue`, `kind`, `state` | |
| `whisper_queue_backlog_cost` | gauge | `queue` | Pending cost: the work remaining, in model-weighted audio seconds |
| `whisper_queue_oldest_pending_seconds` | gauge | `queue` | Primary alert signal |
| `whisper_queue_weight` | gauge | `queue` | For share calculations |
| `whisper_sched_served_cost_total` | counter | `queue`, `class` | Fairness check: `rate(served)` against weight |
| `whisper_sched_vtime` | gauge | `queue` | Debugging the scheduler |
| `whisper_jobs_created_total` | counter | `queue`, `weight`, `fast_path` | |
| `whisper_jobs_finished_total` | counter | `queue`, `outcome` | |
| `whisper_job_wait_seconds` / `whisper_job_elapsed_seconds` | histogram | `queue` | Latency as seen by clients |
| `whisper_task_compute_seconds` | histogram | `kind`, `model` | |
| `whisper_task_realtime_factor` | histogram | `model`, `worker` | Calibrates `model_factor` and slot sizing |
| `whisper_leases_ended_total` | counter | `outcome`, `kind` | Expiry rate means sick workers or a TTL that is too short |
| `whisper_claim_wait_seconds` | histogram | — | Idle fleet vs starved fleet |
| `whisper_workers` | gauge | `state` | |
| `whisper_server_info` | gauge (=1) | `version` | The server's own version, to compare with workers' |
| `whisper_worker_info` | gauge (=1) | `worker`, `hostname`, `description`, `version`, `engine_version`, `ffmpeg_version`, `sandbox_level`, `arch` | Join target; `version` is the worker's whisper-network version, for skew alerts |
| `whisper_worker_last_seen_timestamp_seconds` | gauge | `worker` | Dead-worker alert |
| `whisper_worker_slots` / `whisper_worker_slots_busy` | gauge | `worker` | Utilization |
| `whisper_fleet_slots` | gauge | `state` (`in_use`, `available`, `deferred`, `paused`), `kind` (`all`, `transcribe`, `prepare`) | Fleet capacity, as the dashboard's Capacity tile: slots of active workers seen in 10 min, from their last heartbeat; `deferred` are free slots on hosts deferring for load, `available` the other free ones; `paused` (kind `all`) counts paused workers' slots. A worker offering both kinds counts in both; `prepare` needs `--prepare-min-sandbox`; downloaders count only in kind `all` |
| `whisper_worker_audio_seconds_total` | counter | `worker`, `model` | Throughput per machine (resets with the process; the durable per-worker totals are the `worker_stats` counters, §4.3) |
| `whisper_worker_child_rss_bytes` | gauge | `worker` | Memory sizing (§7.3) |
| `whisper_worker_models_bytes` / `whisper_worker_work_bytes` | gauge | `worker` | Disk use under `--state`: model cache, lease work directories |
| `whisper_worker_state_free_bytes` | gauge | `worker` | Worker disk alert |
| `whisper_worker_host_load` | gauge | `worker` | Host CPU utilisation in percent over the last minute, excluding the worker's own processes (§7.3) |
| `whisper_worker_claims_deferred_total` | counter | `worker` | Slot claim checks that held back because the host was busy (resets with the worker process) |
| `whisper_worker_failure_streak` | gauge | `worker` | The circuit breaker's input: failed (retryable) and expired leases since the last completed task (§4.2) |
| `whisper_worker_auto_paused` | gauge (0/1) | `worker` | 1 while the circuit breaker holds the worker paused; survives server restarts (from the database) |
| `whisper_worker_auto_paused_total` | counter | `worker` | Circuit breaker trips since the server started |
| `whisper_http_requests_total` | counter | `route`, `method`, `code` | |
| `whisper_http_request_duration_seconds` | histogram | `route` | |
| `whisper_upload_bytes_total` / `whisper_input_served_bytes_total` | counter | `queue` / `kind` | Bandwidth |
| `whisper_storage_bytes` | gauge | `kind` (`original`, `canonical`, `transcript`, `failed`: the part of the first two held for failed jobs, §8.5) | |
| `whisper_failed_jobs_retryable` | gauge | — | Failed jobs whose media is kept, so they can be retried |
| `whisper_data_free_bytes` | gauge | — | Disk alert |
| `whisper_db_tx_seconds` | histogram | `op` | SQLite contention, if it ever appears |
| `whisper_fetch_total` | counter | `site`, `outcome` (`done`, `failed`, `rate_limited`, `not_media`, `too_large`, `invalid`, `released`, `expired`) | Fetch lease outcomes (§8.6). `site` is a site named by a fetch policy, else `other`, never a URL |
| `whisper_fetch_bytes_total` | counter | `site` | Bytes of media fetched |
| `whisper_fetch_cooldowns` | gauge | — | (egress, site) cooldowns in force |
| `whisper_fetch_blocked_tasks` | gauge | — | Pending fetch tasks pacing holds back from every active downloader |
| `whisper_fetch_invalid_policies` | gauge | — | Fetch policy rows that do not validate (they block their sites, §8.6) |
| `go_*`, `process_*` | — | — | From the library |

**Suggested alerts** (vmalert / Prometheus rules, shipped as `deploy/alerts.yml`)

| Alert | Condition |
|---|---|
| Queue stalled | `whisper_queue_oldest_pending_seconds > 6h` and `whisper_queue_backlog_cost > 0` |
| Lease churn | `rate(whisper_leases_ended_total{outcome="expired"}[15m]) > 0.1` |
| Worker vanished | active worker with `time() - whisper_worker_last_seen_timestamp_seconds > 600` |
| Disk | `whisper_data_free_bytes / (whisper_data_free_bytes + sum(whisper_storage_bytes)) < 0.1` |
| Worker disk | `whisper_worker_state_free_bytes < 2 GiB` for 15 min (the worker starts handing back large tasks near its 1 GiB `--disk-reserve`) |
| No eligible worker | `whisper_tasks{state="pending"} > 0` and no worker granted that queue has been seen in 10 min (computed server-side as `whisper_queue_eligible_workers == 0`) |
| Worker auto-paused | `whisper_worker_auto_paused == 1`: the circuit breaker took a worker out of service; it stays out until an administrator resumes it |

### 10.3 Health

`GET /healthz` (process up) and `GET /readyz` (DB writable, data dir free space above threshold), both on the main listener and unauthenticated.

---

## 11. Operations

### 11.1 Configuration

Every flag has an env var equivalent (`WHISPER_*`). There is no config file in v1.

**Server**

| Flag | Default | Notes |
|---|---|---|
| `--listen` | `:8080` | |
| `--metrics-listen` | `127.0.0.1:9090` | Loopback by default; empty disables it |
| `--metrics-push-url` / `--metrics-push-interval` / `--metrics-labels` | — / 15s / — | |
| `--debug` / `--quiet` / `--output-format` | off / off / `auto` | Template flags (§10.1) |
| `--data` | `/data` | SQLite file plus `blobs/`, `models/` |
| `--public-url` | unset (relative paths) | External base URL for `Location` headers and `/llms.txt`. Never applied to the worker protocol: lease `input_url`s stay relative so workers may reach the server at a different address |
| `--tls-cert/--tls-key` | — | If unset, plain HTTP; the server logs a warning when not bound to localhost |
| `--require-tls` | off | Refuse to start without TLS |
| `--trust-proxy` | off | Trust `X-Forwarded-For` (rightmost entry) for rate limiting and audit; only behind a proxy that sets it |
| `--heartbeat-interval` | 30s | Leases live 3× this |
| `--max-attempts` | 2 | Distinct workers a task may fail on (or let its lease expire) before its job fails; a worker never gets a task it failed back (§4.2, §8.2) |
| `--retry-wait-limit` | 24h | A task that failed and has waited this long with no other eligible worker taking it fails its job, naming the workers that failed it (§8.2); so does a prepare task whose original is larger than any online worker can hold (§7.2) |
| `--worker-failure-limit` | 3 | Circuit breaker: pause a worker after this many failed (retryable) or expired leases in a row, spanning at least 2 jobs; `0` disables (§4.2) |
| `--task-target` | 60m | |
| `--split-window` | 60s | Search window around each split point |
| `--class-weights` | `low=1,medium=2,high=4` | |
| `--model-factors` | `small.en=1,large-v3-turbo=3` | |
| `--prepare-min-sandbox` | `full` | `full` / `fs-only` / `none` |
| `--fetch-min-sandbox` | `full` | `full` / `fs-only` / `none`: the least sandbox level a downloader needs to get fetch tasks. For a downloader `full` means the filesystem is restricted **and** TCP connect is limited to its egress proxy port by Landlock network rules (ABI ≥ 4, Linux ≥ 6.7), so the proxy cannot be bypassed by name resolution or another port (§7.6, §13). Lowering it accepts the residual in §13; it only helps a downloader with no network confinement at all (non-Linux), since a Linux one below ABI 4 refuses fetch leases itself |
| `--max-upload-bytes` | 2 GiB | A queue may set a lower cap; a queue cap above this is refused (400), and uploads are clamped to it regardless |
| `--min-free-bytes` | 1 GiB | Uploads that would take free disk below this are refused (507) |
| `--audio-retention` / `--job-retention` | 24h / 720h | §8.5 |
| `--failed-retention` / `--failed-retention-max-disk` | 24h / 80% | §8.5 |
| `--pending-worker-ttl` | 168h | Pending workers are deleted this long after registering (§8.5) |
| `--rejected-worker-retention` | 720h | Rejected workers are deleted this long after rejection (§8.5) |
| `--max-pending-workers` | 100 | Cap on workers awaiting approval; registration beyond it is 503 |
| `--max-pending-per-source` | 3 | Cap on workers awaiting approval registered from one address (IPv4 address or IPv6 /64); beyond it registration is 429 |
| `--max-uploads-per-principal` | 2 | Job uploads one client or user may have in progress at once; more get 429 with `Retry-After` |
| `--max-analyses-per-principal` | 1 | Media analyses (validation, encoding) one principal may run at once; more wait up to 30 s, then get 429 |
| `--max-lease-age` / `--lease-age-factor` | 6h / 4 | Heartbeats stop extending a lease older than max(age, factor × task cost in seconds) (§8.3) |
| `--models` | `small.en,large-v3-turbo` | Allowlist plus pinned sha256 (built-in table) |
| `--vad-model` | `silero-v5.1.2` | VAD model sent with transcribe leases; empty disables |
| `--model-upstream` | built-in | Base URL to fetch model files from instead |
| `--max-claim-wait` | 60s | Cap on a worker claim's long-poll |
| `--session-ttl` | 12h | UI session lifetime |
| `--shutdown-timeout` | 15s | Graceful shutdown limit |
| `--read-timeout` | 60s | Limit on reading a request (headers and small bodies); a streaming upload must instead keep making progress at least this often |
| `--min-transfer-rate` | 64 KiB | Slowest accepted upload or download rate per second; sizes the deadlines of large transfers, and a streaming upload must sustain it over every sliding window of max(`--read-timeout`, 30s) (§6.1) |
| `--password-memory-budget` | 512 MiB | Memory for concurrent argon2id hashing, which bounds parallel logins and password changes; beyond it requests wait, then get 503 with `Retry-After` |
| `--admin-password` | random | Used only on first start |

**Worker**

| Flag | Default | Notes |
|---|---|---|
| `--server` | **required** | The only required setting |
| `--state` | `/var/lib/whisper-network` | Credentials and model cache. Mount as a volume. |
| `--description` | hostname | |
| `--threads-per-slot` | 4 | |
| `--slots` | derived (§7.3) | Override |
| `--memory-reserve` | 1 GiB | |
| `--disk-reserve` | 1 GiB | Kept free on the `--state` filesystem (disk admission, §7.2) |
| `--prepare-timeout` | 2h | Least time limit of one ffmpeg run (§7.4) |
| `--prepare-min-rate` | 1 MiB | Per second: an ffmpeg run gets at least its input's size divided by this (§7.4). The lease must outlive the run: a prepare lease lives at most the server's `--max-lease-age` (6 h), which at 1 MiB/s covers inputs up to about 20 GiB with time to transfer; raise it if larger uploads are allowed |
| `--model-cache-max` | 0 (unlimited) | LRU cap on the model cache (§7.2) |
| `--max-host-load` | 70 | Percent. Slots stop claiming while the host's CPU use by other processes is above this; running tasks continue. `0` disables. Linux only (§7.3) |
| `--max-host-load-resume` | 60 | Percent; a slot resumes claiming below this. At most `--max-host-load` |
| `--kinds` | `prepare,transcribe` | e.g. `transcribe` only, to keep ffmpeg off a machine |
| `--downloader` | off | Run as a downloader (§7.6): fetch URL jobs with yt-dlp instead of processing media. Fixed at registration; needs yt-dlp, not ffmpeg or whisper-cli. Defaults to 2 slots |
| `--yt-dlp` | on `$PATH` | yt-dlp binary (downloader; 2022.11 or later) |
| `--yt-dlp-cookies` | unset | Operator cookies file passed to yt-dlp (downloader); copied into each lease's directory |
| `--egress-group` | unset | Name of the egress this downloader shares with others that leave through the same address (downloader, §8.6) |
| `--egress-probe` | unset | URL that returns this downloader's public address, e.g. `https://api.ipify.org` (downloader, optional, §8.6) |
| `--allow-private-egress` | off | Disable the SSRF filter: let the downloader reach loopback, private, link-local and similar addresses (downloader, §13) |
| `--fetch-timeout` | 2h | Time limit of one fetch, upload included (downloader) |
| `--fetch-address-space` | 0 (none) | `RLIMIT_AS` of the yt-dlp child and the deno it runs (downloader). deno reserves tens of GiB of address space at startup, so a low limit breaks YouTube; leave it unset unless you have measured (§7.6) |
| `--whisper-cli` / `--ffmpeg` | on `$PATH` | |
| `--drain` | false | Graceful shutdown finishes running tasks first |
| `--debug` / `--quiet` / `--output-format` | off / off / `auto` | Template flags (§10.1) |
| `--metrics-listen` / `--metrics-push-url` | — / — | Optional (§10.2) |

### 11.2 Deployment

```
docker run -d -v wn-data:/data -p 8080:8080 -p 127.0.0.1:9090:9090 \
           ghcr.io/…/whisper-network server
docker run -d -v wn-state:/var/lib/whisper-network --cpu-shares 128 --memory 6g \
           ghcr.io/…/whisper-network worker --server https://wn.example
```

- The server should sit behind a TLS-terminating proxy (Traefik or Caddy) or use `--tls-*`. Worker and client tokens must not cross untrusted networks in cleartext.
- Backups: `sqlite3 .backup` (or `VACUUM INTO`) plus the `blobs/` directory. Media is transient, so backing up transcripts and the DB is enough.
- A downloader (§7.6) runs from the `<tag>-downloader` image: `docker run -d -v wn-dl:/var/lib/whisper-network ghcr.io/…/whisper-network:<tag>-downloader worker --downloader --server https://wn.example`. Put it on its own network with the egress firewall of §13 (outbound TCP only to 80, 443 and the server's port, plus DNS). It then needs the same two approvals as a worker: lifecycle (`worker_admin`) and the `fetch` grant on each queue it may serve (`queue_admin`), and clients need `submit_url` on those queues.
- `deploy/` ships an example compose file with VictoriaMetrics, VictoriaLogs and vmalert, wired to the flags above, plus a downloader service. It doubles as the integration-test environment.

### 11.3 Capacity sanity check

Server load is trivial. 50 workers × 4 slots heartbeating every 30 s is about 7 writes/s, and SQLite in WAL mode handles thousands. The binding constraints are disk for media and the uplink to workers:
- Disk for a hypothetical 500 h backlog is ~25–35 GB of canonical FLAC, plus originals until each is prepared. Bounded by drain rate; watched by the disk alert.
- Upload of video originals to prepare workers is one-time. WAV to transcribe workers is ~0.1 MB/s per slot (§8.1).

---

## 12. Testing strategy

| Layer | Approach |
|---|---|
| `media` | Table tests: valid and invalid WAV and FLAC headers (wrong rate, stereo, float, truncated, oversize chunk lengths), split-point selection on synthetic PCM with planted silences, and a FLAC range → WAV round trip that compares samples. Fuzz both validators (`go test -fuzz`). |
| `sched` | Pure-function tests: two- and three-queue simulations assert that served-cost ratios converge to the weights within one task cost. Covers reactivation (an idle queue does not bank credit), class shares inside a queue, eligibility subsets, and weight changes mid-run. |
| `store` | Runs against a real SQLite file in `t.TempDir()`; no mocks. The **claim race test** runs 200 goroutines claiming from 50 tasks and asserts that no task ever has two live leases. Lease expiry and requeue use an injected clock. |
| `auth` | A table-driven authorization matrix over principal × role × scope × action → allow/deny/404. This is the main guard against privilege bugs. |
| API | `httptest.Server` end-to-end: register → approve → upload (fast path and prepare path) → claim → heartbeat → result → assembly → transcript. Fencing: an expired lease's `PUT result` gets 410 and does not change the stored result. |
| Worker | A fake `whisper-cli`/`ffmpeg` (a test helper binary via `TestMain` re-exec) that emits canned output, sleeps, crashes or hangs on cue. Covers heartbeat-driven cancellation, SIGTERM release, and resuming an interrupted download. |
| Sandbox | Linux-only tests: the child cannot read `credentials.json`, cannot open TCP (on ABI ≥ 4), and cannot write outside tmp. Skipped with a reason where the kernel lacks support. |
| Observability | A golden test captures the log output of a full job run and asserts the event names and required fields, and that no forbidden field (token, prompt, transcript) appears. A metrics test scrapes `/metrics` and asserts the presence and labels of the catalog. |
| Fetch | The egress filter has table tests for every refused range and form (IPv4-mapped, CGNAT, ULA, zone IDs), DNS-rebinding (a name that resolves differently per lookup) and redirect cases; a fake yt-dlp in `workertest` covers invocation, classification (unsupported, rate-limited, permanent, live) and URL redaction; `store` tests cover claim eligibility, rotation, cooldowns and invalid-policy fail-closed; an end-to-end test submits a URL served by a local test server through a downloader started with `--allow-private-egress`. Sandbox tests (Linux, ABI ≥ 4) check that the child connects only to the proxy port, and a BPF matrix test checks the fetch seccomp filter's decisions. Real tools: the downloader image build runs `downloader-selftest` (the real deno and yt-dlp under the exact fetch child spec), and a Linux Go test does the same with whatever yt-dlp and deno are installed (skipped when absent). |
| Integration | The `deploy/` compose stack: one server, two workers, real ffmpeg and whisper.cpp on a 2-minute MP4, with VictoriaLogs/VictoriaMetrics receiving data. Runs nightly in CI, not per-PR. |

Code is structured for testability: clock, ID generator, process runner and scheduler snapshot are interfaces or values injected at construction. There are no package-level globals; the metrics `Set` is injected too, rather than using the library's default global set.

---

## 13. Security summary

| Threat | Mitigation |
|---|---|
| Malicious media exploits ffmpeg | ffmpeg runs only in worker prepare tasks under `sandbox-exec` (§7.4): Landlock filesystem rules and IPC scoping, a seccomp filter that refuses `socket` and `socketpair`, `io_uring`, new namespaces and leaving the process group the worker kills, `no_new_privs`, no capabilities, an allow-listed environment, a dedicated unprivileged UID when the worker is root, rlimits (incl. a shared `RLIMIT_NPROC`) and a timeout. ffmpeg runs with `-loglevel error`, so the input's metadata stays out of logs. The worker creates child files with `O_EXCL|O_NOFOLLOW`, reads outputs without following links, refuses cross-origin redirects, and caps input downloads at the lease's size (refusing leases without one). Prepare tasks go only to workers whose self-test (file read, TCP, MPTCP, UDP, pathname and abstract Unix sockets, Unix datagram via `socketpair`, signals) reaches `--prepare-min-sandbox`. **Residual:** children of one worker share a uid and its process allowance; architectures without the seccomp filter, and kernels without Landlock, report a weaker level. |
| Malicious canonical audio from a compromised worker | Validated by a full pure-Go decode with size and duration bounds. Memory-safe, so it can at worst fail validation or waste CPU. |
| Malicious media exploits whisper-cli | whisper-cli only ever receives server-generated PCM WAV, and runs under the same sandbox as ffmpeg. |
| Rogue worker registers and harvests media | Registration grants nothing. The `pending` state and per-queue `process` grants are explicit human decisions, and every approval is audited. No enrollment tokens. |
| Registration spam | Per-IP rate limit, a cap on total pending workers (default 100), and a TTL reaper. |
| Stale or partitioned worker overwrites a newer result | Lease fencing; non-live leases get 410. |
| Job ID enumeration | UUIDv7 IDs plus 404 for jobs the caller can't see. UUIDv7 is time-ordered and partly predictable, so authorization, not secrecy, is the guarantee. |
| Default credentials | Random first-run admin password; change forced at first login. |
| Token theft from the DB | Only sha256 hashes are stored. Passwords use argon2id. |
| Secrets or PII in logs | Explicit never-log list (§10.1), enforced by a golden test. |
| Metrics exposure | Separate listener, bound privately by default in the examples. |
| CSRF / session fixation | SameSite cookies, a per-session CSRF token, and session rotation on login. |
| Privilege escalation via `POST /client` | Grants handed out must be a subset of the caller's own. |
| Queue owner grabs fleet share | Only `admin` can change queue weights; changes are audited. |
| Cleartext tokens | TLS required outside localhost (warning plus docs). Optional `--require-tls` refuses to start without it. |
| Disk exhaustion | Per-queue upload caps, `/readyz` free-space check, refusing new uploads below a threshold (`507`), and a disk alert. |
| Model-file tampering | Pinned sha256 per model, verified on the server before caching and on the worker before use. |
| SSRF: a client-supplied URL reaches internal services (metadata endpoints, loopback, LAN) | **The server never connects to a client URL.** Only downloaders fetch, and all their egress (plain GET in-process, yt-dlp through `--proxy`) passes an address filter that **resolves the host itself and judges the address actually dialled**, so DNS rebinding and redirects cannot slip past a pre-check: it refuses loopback, RFC 1918, CGNAT (100.64/10), link-local, unique-local IPv6 (fc00::/7), multicast, unspecified, reserved and documentation ranges, SIIT (`::ffff:0:0:0/96`), ORCHIDv2 (`2001:20::/28`), Teredo, and the IPv4 forms embedded in IPv4-mapped, NAT64 and 6to4 addresses. Each redirect hop (at most 10) is filtered again. **Only ports 80 and 443**, for the proxy and the plain GET alike, redirects included (the allowlist is the dialer's). `--allow-private-egress` turns the filter off for operators who need LAN sources (warning at startup). The server does not judge IP literals either: one filter, one policy |
| A compromised or malicious yt-dlp (or a media-site response that exploits it) | yt-dlp runs only on downloaders, under `sandbox-exec` (§7.6): Landlock filesystem rules (work directory only), a seccomp filter that allows only TCP stream sockets and self-connected Unix stream pairs, Landlock network rules limiting `connect` to the proxy port (ABI ≥ 4), no `listen`/`accept`/TCP Fast Open (seccomp), no UDP so no DNS, no other Unix sockets, `no_new_privs`, rlimits and a wall-clock timeout. Its stdout is the media and nothing else; the downloader never decodes it (a prepare worker does, under its own sandbox). Below the fetch sandbox floor a downloader is not offered fetch tasks |
| A downloader or yt-dlp leaks a client's URL (credentials in a query string, private video IDs) | The URL is job content, visible to whoever can see the job. It is never put in a metric label or audit detail, and routine logs carry the site key instead. A failed fetch's reason and stderr tail are kept in full for diagnosis (§4.2), so they can quote the URL in the job's error and in `task.failed` log events: restrict log access accordingly |
| Fetching as a way to hammer or get the fleet blocked by a site | Per-(egress group, site) rate rules, concurrency and cooldowns set by `admin` only, enforced in the claim; rate-limit outcomes escalate a cooldown without burning attempts or tripping the circuit breaker (§8.6). Invalid policy rows fail closed |
| A fetched page (HTML, JSON) is fed to prepare as "media" | The downloader's content check and the server's sniff on receipt both refuse text, HTML, XML and JSON bodies (`422 not-media`); yt-dlp failures never fall back to plain GET (§7.6) |
| A processing worker leases fetch tasks, or a downloader leases media tasks | Worker kind is fixed at registration, grants are refused across kinds, and the claim enforces the split on its own |

**Residual risks of URL downloads**

- **Landlock `ConnectTCP` restricts by port, not by address.** The child may connect to the proxy's port on *any* address. The port is not secret (it is in yt-dlp's argv, and a compromised yt-dlp can read its own command line), so a compromised yt-dlp can have an attacker-controlled host listen on that same port, connect to it directly and exfiltrate over it, **bypassing the proxy and its SSRF filter**: this reaches public hosts, and also private ones that happen to listen on that port. What stays blocked: DNS and every other UDP, `listen`/`accept`, TCP Fast Open, Unix sockets, and every other TCP port. The proxy's port is random and high, so an unrelated internal service is unlikely to listen on it, but that is luck, not a control. **The control is an egress firewall on the downloader's network namespace** that lets out only TCP to ports 80 and 443 and the server's port (and DNS to the resolver, which the proxy needs): the proxy's port is an ephemeral one (Linux's default range is 32768–60999) and the worker never uses 80, 443 or the server's port for it, so the direct path is then closed, while the proxy and the plain GET, held to the same 80/443 by the dialer, keep working. The proxy and the child run as the same uid inside the container, so an `owner` match cannot tell them apart; filter by port. With Docker, for a downloader on its own bridge network (subnet `172.30.0.0/24` here, server on 8080):

  ```sh
  iptables -I DOCKER-USER -s 172.30.0.0/24 -j DROP
  iptables -I DOCKER-USER -s 172.30.0.0/24 -p udp --dport 53 -j ACCEPT
  iptables -I DOCKER-USER -s 172.30.0.0/24 -p tcp -m multiport --dports 53,80,443,8080 -j ACCEPT
  iptables -I DOCKER-USER -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
  ```

  If the network has IPv6 enabled (`enable_ipv6`), do the same with `ip6tables` for its IPv6 subnet (`fd30::/64` here), or the downloader's IPv6 traffic bypasses the IPv4 rules:

  ```sh
  ip6tables -I DOCKER-USER -s fd30::/64 -j DROP
  ip6tables -I DOCKER-USER -s fd30::/64 -p udp --dport 53 -j ACCEPT
  ip6tables -I DOCKER-USER -s fd30::/64 -p tcp -m multiport --dports 53,80,443,8080 -j ACCEPT
  ip6tables -I DOCKER-USER -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
  ```

  (`-I` prepends, so the rules end up in the reverse of this order: established traffic, then the allowed ports, then the drop.) This relies on the port allowlist of the proxy and the plain GET: without it a fetch could legitimately need any port. The rest of the downloader's own confinement still applies: a dedicated network segment, no secrets on the host, an unprivileged uid, a current yt-dlp.
- **The cookies file is an exfiltration target and a confused deputy.** `--yt-dlp-cookies` is copied into each lease's directory, where the child reads it; a compromised yt-dlp can send it out over the path above. And anyone with `submit_url` on a queue the downloader serves fetches with that account's session: private videos, age-gated or members-only content it can see. Use a throwaway account, and grant `submit_url` accordingly.
- **Below Landlock ABI 4** the child gets no sockets at all, so it fails closed, and the downloader **refuses every fetch lease as pre-work** without running yt-dlp (warning at startup); its probe reports a level below `full`. With the default `--fetch-min-sandbox full` such a downloader is simply not offered fetch tasks. Lowering the floor does not make it fetch on Linux: it is then offered fetch tasks, refuses each as pre-work (requeued, no attempt used), and after repeated refusals the circuit breaker auto-pauses it (§4.2). **Lowering `--fetch-min-sandbox` is an informed acceptance** that only matters where no seccomp or Landlock is applied to the child at all (a non-Linux downloader, level `none`): there the child is unconfined network-wise, so a compromised yt-dlp can ignore `--proxy` and connect anywhere.
- **`--allow-private-egress`** removes the SSRF filter: anyone with `submit_url` can then make that downloader fetch from the networks it can reach, and read the result back as a transcript of whatever they can coax into audio. Use it only on a downloader isolated for that purpose.
- **A URL is visible to everyone who can see the job**, like `source` and `prompt` (§5.2); it can carry secrets a client should not put there.
- A fetched file is untrusted media like any upload and is processed under the same sandboxed prepare path; the content check is a guard against feeding web pages in, not a malware scanner.

---

## 14. Open questions

1. **Mixed engine versions in one job.** Accept and record them (as specified above), or pin a job to the first version that touches it? Recommendation: accept and record.
2. **Lease cost refunds.** Should cost charged to an expired or failed lease be refunded? Recommendation: no, unless the `whisper_leases_ended_total{outcome="expired"}` rate shows it matters.
3. **Changing a job's weight after submission** (`PATCH /job/{id} {"weight":"high"}` by the owner while it is queued). Cheap to add. Recommendation: add it when someone asks.
4. **Auto-calibrating `model_factor`** from the `whisper_task_realtime_factor` metric instead of static config. Recommendation: static for v1, revisit with data.
5. **Multiple audio streams** in an upload (e.g. language tracks). v1 takes the first. A `params.audio_stream` index is the obvious extension.
6. **`delete_all` client role.** Is it needed, or is "owner or queue_admin" enough?
7. **Result callbacks.** Webhooks would remove polling, but they don't work for clients behind NAT. Polling is enough until a client needs push.
8. **Re-transcription with a better model** while the audio is retained. Recommendation: client-side resubmit; YAGNI.

Resolved in r2: fairness (§8.2), FLAC (§8.1.1), enrollment tokens (deferred: explicit approval only).

---

## 15. Implementation plan

Logging and metrics go in from phase 1, because instrumenting as each piece is written is cheaper than retrofitting.

| Phase | Scope | Exit criterion |
|---|---|---|
| 0 | Repo skeleton, `cmd/whisper-network`, `internal/{store,media,sched,auth,obs,server,worker,ui}`, migrations, CI (lint, test, fuzz smoke) | `go test ./...` green |
| 1 | Store, WAV/FLAC validate/split/serve, weighted fair claim, client API, worker API, leases, reaper, JSONL logging, `/metrics`. Tokens bootstrapped via CLI (`whisper-network admin create-client`). | E2E test: canonical upload → 2 fake workers → transcript; sched simulation tests pass; log golden test passes |
| 2 | Real whisper-cli runner, prepare via ffmpeg, `sandbox-exec` (Landlock, UID, rlimits), model cache, `bench`, metrics push, heartbeat gauges, container images (amd64/arm64), darwin binary | MP4 → transcript on two real machines; sandbox tests pass on the fleet's kernels; bench numbers recorded and §7.3 defaults updated |
| 3 | UI: login, worker approval, queues and weights, jobs, users/clients, audit, share dashboard | Worker approved and job run without touching the CLI |
| 4 | `deploy/` compose stack with VictoriaMetrics/VictoriaLogs/vmalert and alert rules; nightly integration test | Alerts fire in a staged failure (killed worker, full disk) |
| 5 | First production client integration (§9): validate on a sample, then submit the full backlog at `low` | Sample transcripts are complete and correctly labelled |
| 6 | URL downloads: `fetch` task kind, downloader workers (§7.6), sandbox network profile, egress filter and proxy, fetch pacing and policies (§8.6), `submit_url`, downloader image and yt-dlp bump workflow | Local test server URL → transcript through a downloader; SSRF filter, rebinding and redirect tests pass; policy and pacing tests pass |

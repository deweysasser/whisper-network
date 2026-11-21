// Package api defines the JSON wire contract shared by the server, the worker
// and the Go client. It contains only data types and constants: no behaviour,
// no dependencies beyond the standard library. See DESIGN.md §6.
package api

import (
	"encoding/json"
	"time"
)

// JobState is the client-visible lifecycle state of a job (DESIGN.md §4.2).
type JobState string

const (
	JobFetching  JobState = "fetching"
	JobPreparing JobState = "preparing"
	JobQueued    JobState = "queued"
	JobRunning   JobState = "running"
	JobDone      JobState = "done"
	JobFailed    JobState = "failed"
)

// TaskState is the internal lifecycle state of a task.
type TaskState string

const (
	TaskPending TaskState = "pending"
	TaskLeased  TaskState = "leased"
	TaskDone    TaskState = "done"
	TaskFailed  TaskState = "failed"
)

// TaskKind distinguishes media normalization from transcription.
type TaskKind string

const (
	KindFetch      TaskKind = "fetch"
	KindPrepare    TaskKind = "prepare"
	KindTranscribe TaskKind = "transcribe"
)

// Weight is a job's weight class within its queue (DESIGN.md §8.2).
type Weight string

const (
	WeightLow    Weight = "low"
	WeightMedium Weight = "medium"
	WeightHigh   Weight = "high"
)

// Valid reports whether w is one of the defined weight classes.
func (w Weight) Valid() bool {
	return w == WeightLow || w == WeightMedium || w == WeightHigh
}

// WorkerState is the approval lifecycle state of a worker.
type WorkerState string

const (
	WorkerPending  WorkerState = "pending"
	WorkerActive   WorkerState = "active"
	WorkerPaused   WorkerState = "paused"
	WorkerRejected WorkerState = "rejected"
)

// SandboxLevel is how strongly a worker can confine its child processes
// (DESIGN.md §7.4). Ordered: None < FSOnly < Full.
type SandboxLevel string

const (
	SandboxNone   SandboxLevel = "none"
	SandboxFSOnly SandboxLevel = "fs-only"
	SandboxFull   SandboxLevel = "full"
)

// Rank orders sandbox levels so policies can compare them. Unknown levels rank
// below None.
func (l SandboxLevel) Rank() int {
	switch l {
	case SandboxNone:
		return 0
	case SandboxFSOnly:
		return 1
	case SandboxFull:
		return 2
	}
	return -1
}

// LeaseOutcome records how a lease ended.
type LeaseOutcome string

const (
	OutcomeDone      LeaseOutcome = "done"
	OutcomeExpired   LeaseOutcome = "expired"
	OutcomeReleased  LeaseOutcome = "released"
	OutcomeFailed    LeaseOutcome = "failed"
	OutcomeCancelled LeaseOutcome = "cancelled"
)

// Role names used in grants (DESIGN.md §5.2).
type Role string

const (
	RoleAdmin       Role = "admin"
	RoleQueueAdmin  Role = "queue_admin"
	RoleWorkerAdmin Role = "worker_admin"
	RoleClientAdmin Role = "client_admin"
	RoleSubmit      Role = "submit"
	RoleViewAll     Role = "view_all"
	RoleReadAll     Role = "read_all"
	RoleDeleteAll   Role = "delete_all"
	RoleProcess     Role = "process"
	// RoleFetch lets a downloader worker fetch URLs for jobs on a queue.
	RoleFetch Role = "fetch"
	// RoleSubmitURL lets a client or user submit a URL to fetch on a queue.
	RoleSubmitURL Role = "submit_url"
)

// Grant is a role bound to a scope. Scope is "*", "queue:<name>" or
// "worker:<id>" on the wire (queues are addressed by name, not internal id).
type Grant struct {
	Role  Role   `json:"role"`
	Scope string `json:"scope"`
}

// ---------------------------------------------------------------------------
// Client API (§6.1)

// FetchRequest is the body of POST /queue/{q}/fetch: job parameters plus the
// URL a downloader worker is to fetch.
type FetchRequest struct {
	JobParams
	URL string `json:"url"`
}

// JobParams is the "params" multipart part of POST /queue/{q}.
type JobParams struct {
	Model    string          `json:"model"`
	Language string          `json:"language,omitempty"` // default "en"
	Weight   Weight          `json:"weight,omitempty"`   // default medium
	Prompt   string          `json:"prompt,omitempty"`   // <= MaxPromptLen chars
	Label    string          `json:"label,omitempty"`
	Source   string          `json:"source,omitempty"`   // recorded, never fetched (the fetch URL is FetchRequest.URL)
	Metadata json.RawMessage `json:"metadata,omitempty"` // opaque, <= MaxMetadataBytes
}

const (
	MaxPromptLen     = 800
	MaxMetadataBytes = 8 << 10
	MaxLabelLen      = 200
	MaxSourceLen     = 2048
)

// MediaInfo describes a job's media (§6.1 job status).
type MediaInfo struct {
	OriginalBytes  int64   `json:"original_bytes"`
	OriginalType   string  `json:"original_type,omitempty"`
	DurationS      float64 `json:"duration_s,omitempty"`
	OriginalPurged bool    `json:"original_purged"`
	AudioPurged    bool    `json:"audio_purged"`
	// KeptUntil is, for a failed job whose media is still present, when
	// the failed-job retention (--failed-retention) lets the reaper purge
	// it. A full disk can purge it sooner.
	KeptUntil *time.Time `json:"kept_until,omitempty"`
}

// TaskCounts summarizes a job's tasks by state.
type TaskCounts struct {
	Total   int `json:"total"`
	Pending int `json:"pending"`
	Leased  int `json:"leased"`
	Done    int `json:"done"`
	Failed  int `json:"failed"`
	// Cancelled counts tasks that will not run because the job failed.
	Cancelled int `json:"cancelled"`
}

// Downloads records transcript retrieval.
type Downloads struct {
	Count int        `json:"count"`
	First *time.Time `json:"first"`
	Last  *time.Time `json:"last"`
}

// Submitter identifies who created a job: an API client or a UI user.
type Submitter struct {
	Kind string `json:"kind"` // "client" or "user"
	ID   string `json:"id"`
	Name string `json:"name,omitempty"`
}

// Job is the job status document returned by GET /job/{id} and list endpoints.
type Job struct {
	ID          string          `json:"id"`
	Queue       string          `json:"queue"`
	State       JobState        `json:"state"`
	Weight      Weight          `json:"weight"`
	Model       string          `json:"model"`
	Language    string          `json:"language"`
	Prompt      string          `json:"prompt,omitempty"`
	Label       string          `json:"label,omitempty"`
	Source      string          `json:"source,omitempty"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
	SubmittedBy *Submitter      `json:"submitted_by,omitempty"` // who created the job
	Media       MediaInfo       `json:"media"`
	Tasks       TaskCounts      `json:"tasks"`
	CreatedAt   time.Time       `json:"created_at"`
	StartedAt   *time.Time      `json:"started_at"`
	CompletedAt *time.Time      `json:"completed_at"`
	WaitS       *float64        `json:"wait_s"`
	ElapsedS    *float64        `json:"elapsed_s"`
	ComputeS    float64         `json:"compute_s"`
	Downloads   Downloads       `json:"downloads"`
	Error       *string         `json:"error"`
	// Retries counts how many times the job was retried after failing
	// (POST /job/{id}/retry).
	Retries int `json:"retries"`
	// Retryable is true for a failed job whose retained media is still
	// present, so POST /job/{id}/retry would be accepted.
	Retryable bool `json:"retryable"`
}

// JobList is the response of GET /queue/{q}.
type JobList struct {
	Jobs       []Job  `json:"jobs"`
	NextCursor string `json:"next_cursor,omitempty"`
}

// Bulk job actions (POST /queue/{q}/bulk).
const (
	BulkRetry  = "retry"
	BulkDelete = "delete"
	// MaxBulkJobs caps the jobs one bulk request acts on; a larger
	// selection pages with BulkResult.NextCursor.
	MaxBulkJobs = 500
)

// BulkRequest is the body of POST /queue/{q}/bulk. It names the jobs
// either by JobIDs (at most MaxBulkJobs) or by Filter, never both.
type BulkRequest struct {
	Action string      `json:"action"` // BulkRetry or BulkDelete
	JobIDs []string    `json:"job_ids,omitempty"`
	Filter *BulkFilter `json:"filter,omitempty"`
	// DryRun reports what would happen without changing anything.
	DryRun bool `json:"dry_run,omitempty"`
}

// BulkFilter selects a queue's jobs as GET /queue/{q} would list them
// (oldest first, only those the caller may see). State is required.
type BulkFilter struct {
	State JobState `json:"state"`
	// Since keeps only jobs updated after it (for failed jobs: failed
	// after it).
	Since *time.Time `json:"since,omitempty"`
	// Cursor continues a selection from BulkResult.NextCursor.
	Cursor string `json:"cursor,omitempty"`
}

// Bulk outcomes of one job.
const (
	OutcomeRetried     = "retried"
	OutcomeDeleted     = "deleted"
	OutcomeWouldRetry  = "would_retry"  // dry run
	OutcomeWouldDelete = "would_delete" // dry run
	OutcomeSkipped     = "skipped"      // with a Reason
	OutcomeForbidden   = "forbidden"
	OutcomeNotFound    = "not_found" // unknown, not on this queue, or not visible
)

// BulkJobResult is one job's outcome in a BulkResult.
type BulkJobResult struct {
	JobID   string `json:"job_id"`
	Outcome string `json:"outcome"`
	Reason  string `json:"reason,omitempty"`
}

// BulkResult is the response of POST /queue/{q}/bulk.
type BulkResult struct {
	Action string          `json:"action"`
	DryRun bool            `json:"dry_run,omitempty"`
	Jobs   []BulkJobResult `json:"jobs"`
	// Counts tallies Jobs by outcome.
	Counts map[string]int `json:"counts"`
	// NextCursor, for a filter selection with more matching jobs than
	// MaxBulkJobs, continues it (BulkFilter.Cursor).
	NextCursor string `json:"next_cursor,omitempty"`
}

// Queue is the representation of a queue for admin endpoints and the UI.
type Queue struct {
	Name           string `json:"name"`
	Description    string `json:"description,omitempty"`
	Weight         int    `json:"weight"`
	MaxUploadBytes int64  `json:"max_upload_bytes,omitempty"`
}

// QueuePatch is the body of PATCH /queue/{q}. Nil fields are unchanged.
type QueuePatch struct {
	Description    *string `json:"description,omitempty"`
	Weight         *int    `json:"weight,omitempty"`
	MaxUploadBytes *int64  `json:"max_upload_bytes,omitempty"`
}

// CreateClientRequest is the body of POST /client.
type CreateClientRequest struct {
	Name   string  `json:"name"`
	Grants []Grant `json:"grants"`
}

// Client is the representation of an API client. Token is set only in the
// response that created it.
type Client struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	Grants     []Grant    `json:"grants"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
	RevokedAt  *time.Time `json:"revoked_at,omitempty"`
	Token      string     `json:"token,omitempty"`
}

// Segment is one transcribed span. Times are milliseconds; relative to the
// task start in a TranscribeResult, absolute in GET /job/{id}/result.
type Segment struct {
	StartMS int64  `json:"start_ms"`
	EndMS   int64  `json:"end_ms"`
	Text    string `json:"text"`
}

// JobResult is GET /job/{id}/result.
type JobResult struct {
	JobID      string    `json:"job_id"`
	Provenance string    `json:"provenance"`
	Segments   []Segment `json:"segments"`
	// Partial and Missing are set only by ?partial=1 on a job that is not
	// done: Missing lists the audio the transcript lacks, one entry per
	// transcribe task that is not done.
	Partial bool  `json:"partial,omitempty"`
	Missing []Gap `json:"missing,omitempty"`
}

// Gap is a span of audio a partial transcript lacks: transcribe task Task,
// absolute times in milliseconds.
type Gap struct {
	Task    int   `json:"task"`
	StartMS int64 `json:"start_ms"`
	EndMS   int64 `json:"end_ms"`
}

// ---------------------------------------------------------------------------
// Worker API (§6.2)

// WorkerKind is fixed at registration: what kind of tasks a worker may run.
type WorkerKind string

const (
	// WorkerProcessing runs prepare and transcribe tasks (the default).
	WorkerProcessing WorkerKind = "processing"
	// WorkerDownloader runs fetch tasks only.
	WorkerDownloader WorkerKind = "downloader"
)

// WorkerFacts is what a worker reports about itself at registration and on
// PATCH /worker/{id}.
type WorkerFacts struct {
	Hostname      string       `json:"hostname"`
	Description   string       `json:"description,omitempty"`
	CPUModel      string       `json:"cpu_model,omitempty"`
	Cores         int          `json:"cores"`
	MemBytes      int64        `json:"mem_bytes"`
	OS            string       `json:"os"`
	Arch          string       `json:"arch"`
	EngineVersion string       `json:"engine_version,omitempty"`
	FFmpegVersion string       `json:"ffmpeg_version,omitempty"`
	SandboxLevel  SandboxLevel `json:"sandbox_level"`
	Kinds         []TaskKind   `json:"kinds"`
	Slots         int          `json:"slots"`
	Models        []string     `json:"models"`
	Version       string       `json:"version,omitempty"` // whisper-network version
	// Kind is empty for a processing worker.
	Kind         WorkerKind `json:"kind,omitempty"`
	YtDlpVersion string     `json:"yt_dlp_version,omitempty"`
	EgressGroup  string     `json:"egress_group,omitempty"`
	EgressIP     string     `json:"egress_ip,omitempty"`
}

// RegisterResponse is returned by POST /worker. Token is shown once.
type RegisterResponse struct {
	ID    string      `json:"id"`
	Token string      `json:"token"`
	State WorkerState `json:"state"`
}

// Worker is GET /worker/{id}.
type Worker struct {
	ID         string      `json:"id"`
	State      WorkerState `json:"state"`
	Facts      WorkerFacts `json:"facts"`
	Queues     []string    `json:"queues"` // queues this worker may process (a downloader: fetch for)
	LastSeenAt *time.Time  `json:"last_seen_at,omitempty"`
	// Hints from the server.
	HeartbeatIntervalS int `json:"heartbeat_interval_s"`
	PollIntervalS      int `json:"poll_interval_s"`
	// MaxPrepareOutputBytes bounds the canonical FLAC the server accepts
	// from a prepare task (verbatim FLAC of --max-audio, plus overhead).
	// The worker sizes its disk admission and the prepare child's file-size
	// limit by it before it has seen a prepare lease.
	MaxPrepareOutputBytes int64 `json:"max_prepare_output_bytes,omitempty"`
}

// ClaimRequest is the body of POST /worker/{id}/leases.
type ClaimRequest struct {
	Kinds  []TaskKind `json:"kinds"`
	Models []string   `json:"models"`
	// Gauges are the worker's current readings. Heartbeats only flow while
	// a lease is held, so an idle worker reports through its claims.
	Gauges *Gauges `json:"gauges,omitempty"`
}

// Lease is the claim response (201).
type Lease struct {
	ID          string   `json:"id"`
	Kind        TaskKind `json:"kind"`
	Job         string   `json:"job"`
	Seq         int      `json:"seq"`
	InputURL    string   `json:"input_url"`
	InputBytes  int64    `json:"input_bytes,omitempty"`  // prepare: original size
	InputSHA256 string   `json:"input_sha256,omitempty"` // prepare: original hash
	// MaxOutputBytes (prepare) is the largest canonical FLAC the server
	// will accept, as in Worker.MaxPrepareOutputBytes. For a fetch lease it
	// is the byte cap on the download.
	MaxOutputBytes int64 `json:"max_output_bytes,omitempty"`
	AudioSamples   int64 `json:"audio_samples,omitempty"`
	// URL and Site (fetch) are the URL to download and its policy site key;
	// MaxOutputBytes is the byte cap for the download.
	URL            string `json:"url,omitempty"`
	Site           string `json:"site,omitempty"`
	Model          string `json:"model,omitempty"`
	ModelSHA256    string `json:"model_sha256,omitempty"`
	VADModel       string `json:"vad_model,omitempty"`
	VADModelSHA256 string `json:"vad_model_sha256,omitempty"`
	Language       string `json:"language,omitempty"`
	Prompt         string `json:"prompt,omitempty"`
	// NoContext is set on the retry of a transcribe task whose result was
	// refused as a repetition loop: the worker runs whisper-cli without
	// conditioning on previous text (-mc 0). Such a lease never carries a
	// prompt.
	NoContext          bool      `json:"no_context,omitempty"`
	HeartbeatIntervalS int       `json:"heartbeat_interval_s"`
	ExpiresAt          time.Time `json:"expires_at"`
}

// Gauges are worker readings carried on heartbeats and claims (§10.2).
//
// The disk readings are pointers so that "not reported" (an older worker, or
// a probe that failed) is distinguishable from zero: a worker whose disk is
// full must be able to say so.
type Gauges struct {
	SlotsBusy        int     `json:"slots_busy"`
	ChildRSSBytes    int64   `json:"child_rss_bytes"`
	Load1            float64 `json:"load1"`
	CPUThrottledSecs float64 `json:"cpu_throttled_seconds"`

	// ModelsBytes is the size of the model cache (<state>/models),
	// including partial downloads.
	ModelsBytes *int64 `json:"models_bytes,omitempty"`
	// WorkBytes is the size of all lease work directories (<state>/work).
	WorkBytes *int64 `json:"work_bytes,omitempty"`
	// StateFreeBytes is the free space on the filesystem holding <state>.
	StateFreeBytes *int64 `json:"state_free_bytes,omitempty"`
	// CachedModels lists the complete model files in the cache.
	CachedModels []CachedModel `json:"cached_models,omitempty"`
	// PrepareMaxInputBytes is the largest prepare input (original media
	// size) the worker could admit now, given free disk, its reserve and its
	// running leases. On a claim the server offers no prepare task with a
	// larger original. Nil when unknown (no free-space reading, or an older
	// worker): no prepare task is held back.
	PrepareMaxInputBytes *int64 `json:"prepare_max_input_bytes,omitempty"`
	// PrepareCapacityBytes is the same bound if the worker were idle: what
	// its disk could take at best. A prepare task larger than every
	// worker's capacity is failed after --retry-wait-limit (DESIGN.md §7.2).
	PrepareCapacityBytes *int64 `json:"prepare_capacity_bytes,omitempty"`

	// HostLoad is the host's CPU utilisation in percent over the last
	// minute, excluding the worker's own processes; nil if not measured.
	HostLoad *float64 `json:"host_load,omitempty"`
	// DeferredForLoad is true while a slot is not claiming because
	// HostLoad is above the worker's --max-host-load.
	DeferredForLoad bool `json:"deferred_for_load,omitempty"`
	// ClaimsDeferred counts, since the worker started, the times a slot
	// checked the host load and did not claim; nil when the check is off.
	ClaimsDeferred *int64 `json:"claims_deferred,omitempty"`
}

// MaxCachedModels bounds Gauges.CachedModels; longer lists are dropped by
// the server.
const MaxCachedModels = 64

// CachedModel is one model file in a worker's cache.
type CachedModel struct {
	Name  string `json:"name"`
	Bytes int64  `json:"bytes"`
}

// HeartbeatRequest is the optional body of POST /lease/{id}/heartbeat.
type HeartbeatRequest struct {
	Gauges *Gauges `json:"gauges,omitempty"`
}

// HeartbeatResponse is the 200 body of a heartbeat.
type HeartbeatResponse struct {
	ExpiresAt time.Time `json:"expires_at"`
}

// TranscribeResult is the body of PUT /lease/{id}/result for transcribe
// leases. (Prepare leases PUT the canonical FLAC as audio/flac instead.)
type TranscribeResult struct {
	EngineVersion string    `json:"engine_version"`
	Model         string    `json:"model"`
	ModelSHA256   string    `json:"model_sha256"`
	VAD           string    `json:"vad,omitempty"`
	Language      string    `json:"language"`
	ComputeMS     int64     `json:"compute_ms"`
	Segments      []Segment `json:"segments"`
}

// Result headers: prepare (PUT /lease/{id}/result with Content-Type
// audio/flac) and fetch.
const (
	HeaderComputeMS     = "X-Compute-Ms"
	HeaderFFmpegVersion = "X-Ffmpeg-Version"
	// HeaderFetchMethod on a fetch result says how the media was fetched:
	// FetchMethodYtDlp or FetchMethodHTTP (optional; for logs only).
	HeaderFetchMethod = "X-Fetch-Method"
)

// Values of HeaderFetchMethod.
const (
	FetchMethodYtDlp = "yt-dlp"
	FetchMethodHTTP  = "http"
)

// FailureReport is the body of POST /lease/{id}/failure.
type FailureReport struct {
	Reason     string `json:"reason"`
	StderrTail string `json:"stderr_tail,omitempty"` // <= MaxStderrTail bytes
	Retryable  bool   `json:"retryable"`
	// Stage says how far the worker got before failing. Empty (older
	// workers) is treated like StageInput and StageChild.
	Stage FailureStage `json:"stage,omitempty"`
}

// FailureStage is where in a task a worker failed (DESIGN.md §8.2).
type FailureStage string

const (
	// StagePreWork is before the worker fetched the input or started a
	// child: its own configuration (tools, models, the input URL's origin)
	// failed. It says nothing about the task, so the server requeues the
	// task without counting an attempt or excluding the worker, and counts
	// the failure only toward the worker's failure streak.
	StagePreWork FailureStage = "pre-work"
	// StageInput is while downloading or checking the input.
	StageInput FailureStage = "input"
	// StageChild is while running ffmpeg or whisper-cli, or handling
	// their output.
	StageChild FailureStage = "child"
	// StageLoop is assigned by the server, never reported by a worker:
	// the worker's transcript was refused as a repetition loop (issue #99).
	// It counts as an attempt but not toward the worker's failure streak,
	// and the task's next lease carries NoContext.
	StageLoop FailureStage = "loop"
	// StageRateLimited is a fetch the origin refused for rate (HTTP 429 or
	// similar). The server starts a cooldown for the site and egress.
	StageRateLimited FailureStage = "rate_limited"
)

// Valid reports whether s is a known stage.
func (s FailureStage) Valid() bool {
	return s == StagePreWork || s == StageInput || s == StageChild || s == StageRateLimited
}

const MaxStderrTail = 4 << 10

// ---------------------------------------------------------------------------
// Errors (RFC 9457)

// Problem is an application/problem+json error body.
type Problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`
	// Extension members.
	State string `json:"state,omitempty"` // e.g. worker state on 403/409
}

// Stable problem type slugs.
const (
	ProblemBadRequest     = "bad-request"
	ProblemUnauthorized   = "unauthorized"
	ProblemForbidden      = "forbidden"
	ProblemNotFound       = "not-found"
	ProblemConflict       = "conflict"
	ProblemLeaseGone      = "lease-gone"
	ProblemWorkerPending  = "worker-pending"
	ProblemWorkerPaused   = "worker-paused"
	ProblemWorkerRejected = "worker-rejected"
	ProblemMediaInvalid   = "media-invalid"
	ProblemTooLarge       = "too-large"
	ProblemResultInvalid  = "result-invalid"
	ProblemNotDone        = "not-done"
	ProblemNotRetryable   = "not-retryable"
	ProblemStorageFull    = "storage-full"
	ProblemRateLimited    = "rate-limited"
	ProblemNoDownloader   = "no-downloader"
	ProblemNotMedia       = "not-media"
	ProblemInternal       = "internal"
)

// ProblemContentType is the media type for Problem bodies.
const ProblemContentType = "application/problem+json"

// Token prefixes (§5.1).
const (
	ClientTokenPrefix = "wnc_"
	WorkerTokenPrefix = "wnw_"
)

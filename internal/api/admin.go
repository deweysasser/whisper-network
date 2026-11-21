package api

import "time"

// Admin API wire types (server routes under /admin/, see internal/server
// admin.go). Principals on the wire are "user:<username>",
// "client:<id>" or "worker:<id>"; scopes are "*", "queue:<name>" or
// "worker:<id>".

// ServerVersion is GET /admin/version: the server's own whisper-network
// version, to compare with each worker's facts.version (DESIGN.md §6.3).
type ServerVersion struct {
	Version string `json:"version"`
}

// WorkerSummary is one entry of GET /admin/workers: the worker document
// plus the approval record and latest heartbeat gauges.
type WorkerSummary struct {
	Worker
	RegisteredAt time.Time  `json:"registered_at"`
	DecidedBy    string     `json:"decided_by,omitempty"`
	DecidedAt    *time.Time `json:"decided_at,omitempty"`
	Gauges       *Gauges    `json:"gauges,omitempty"`
	// Stats is what the worker has done. Servers before it was added omit it.
	Stats *WorkerStats `json:"stats,omitempty"`
	// FailureStreak is the worker's failed (retryable) and expired leases
	// since its last completed task or resume, in FailureStreakJobs distinct
	// jobs: the circuit breaker's input (DESIGN.md §4.2).
	FailureStreak     int `json:"failure_streak"`
	FailureStreakJobs int `json:"failure_streak_jobs"`
	// PauseReason is set while the worker is paused because the server's
	// circuit breaker paused it, e.g. "3 consecutive failures across 2 jobs".
	PauseReason string `json:"pause_reason,omitempty"`
}

// WorkerStats are a worker's durable work counters. Completed tasks count
// once each; a release or a cancellation counts as nothing. They cover the
// worker's whole life, except that history from before the counters were
// added is limited to what was then retained (DESIGN.md §4.3).
type WorkerStats struct {
	PrepareTasks    int64 `json:"prepare_tasks"`
	TranscribeTasks int64 `json:"transcribe_tasks"`
	// Jobs counts distinct jobs with at least one task completed by the worker.
	Jobs int64 `json:"jobs"`
	// AudioS is the audio of completed transcribe tasks, in seconds.
	AudioS float64 `json:"audio_s"`
	// ComputeS is the reported compute time of completed tasks, of which
	// TranscribeComputeS was spent on transcribe tasks.
	ComputeS           float64 `json:"compute_s"`
	TranscribeComputeS float64 `json:"transcribe_compute_s"`
	// Failures counts leases the worker reported failed; ExpiredLeases those
	// that expired (crash, lost heartbeats, age limit).
	Failures      int64      `json:"failures"`
	ExpiredLeases int64      `json:"expired_leases"`
	FirstTaskAt   *time.Time `json:"first_task_at,omitempty"`
	LastTaskAt    *time.Time `json:"last_task_at,omitempty"`
}

// TasksDone is the number of tasks completed, of either kind.
func (s *WorkerStats) TasksDone() int64 { return s.PrepareTasks + s.TranscribeTasks }

// RealtimeFactor is audio seconds transcribed per compute second (higher is
// faster), or 0 before the first transcribe task.
func (s *WorkerStats) RealtimeFactor() float64 {
	if s.TranscribeComputeS <= 0 {
		return 0
	}
	return s.AudioS / s.TranscribeComputeS
}

// FailureRate is the fraction of counted lease outcomes (completed, failed,
// expired) that were not completions, or 0 with none.
func (s *WorkerStats) FailureRate() float64 {
	bad := s.Failures + s.ExpiredLeases
	if total := s.TasksDone() + bad; total > 0 {
		return float64(bad) / float64(total)
	}
	return 0
}

// ApproveWorkerRequest is the body of POST /admin/workers/{id}/approve:
// make the worker active and grant it process on each queue.
type ApproveWorkerRequest struct {
	Queues []string `json:"queues"`
}

// CreateUserRequest is the body of POST /admin/users. An empty Password
// makes the server generate one, returned once in User.Password.
type CreateUserRequest struct {
	Username string  `json:"username"`
	Password string  `json:"password,omitempty"`
	Grants   []Grant `json:"grants,omitempty"`
}

// User is a UI login. Password is set only in the response that generated it.
type User struct {
	Username           string    `json:"username"`
	MustChangePassword bool      `json:"must_change_password"`
	Disabled           bool      `json:"disabled"`
	CreatedAt          time.Time `json:"created_at"`
	Grants             []Grant   `json:"grants"`
	Password           string    `json:"password,omitempty"`
}

// PrincipalGrant is a grant bound to a principal, for /admin/grants.
type PrincipalGrant struct {
	Principal string `json:"principal"`
	Role      Role   `json:"role"`
	Scope     string `json:"scope"`
	GrantedBy string `json:"granted_by,omitempty"`
}

// AuditEntry is one audit record.
type AuditEntry struct {
	ID     int64     `json:"id"`
	TS     time.Time `json:"ts"`
	Actor  string    `json:"actor"`
	Action string    `json:"action"`
	Target string    `json:"target,omitempty"`
	Detail string    `json:"detail,omitempty"`
}

// AuditList is GET /admin/audit. Pass NextBefore as ?before= for the next
// (older) page; 0 means there is none.
type AuditList struct {
	Entries    []AuditEntry `json:"entries"`
	NextBefore int64        `json:"next_before,omitempty"`
}

// FetchPolicy is a per-site (optionally per-egress) download policy.
// Durations are Go duration strings.
type FetchPolicy struct {
	Key            string    `json:"key"`
	Site           string    `json:"site"`
	EgressKey      string    `json:"egress_key,omitempty"`
	Rules          []string  `json:"rules"`
	Concurrency    int       `json:"concurrency"`
	BackoffInitial string    `json:"backoff_initial"`
	BackoffCap     string    `json:"backoff_cap"`
	UpdatedBy      string    `json:"updated_by"`
	UpdatedAt      time.Time `json:"updated_at"`
	// Invalid marks a stored row that does not validate (a hand edit). It
	// blocks the site it governs until a PUT repairs it or a DELETE removes
	// it; InvalidReason says why. The other fields are as stored.
	Invalid       bool   `json:"invalid,omitempty"`
	InvalidReason string `json:"invalid_reason,omitempty"`
}

// FetchCooldown is an active pause on fetching a site from an egress.
type FetchCooldown struct {
	EgressKey string    `json:"egress_key"`
	Site      string    `json:"site"`
	Until     time.Time `json:"until"`
	Level     int       `json:"level"`
	Manual    bool      `json:"manual"`
	SetBy     string    `json:"set_by"`
	Reason    string    `json:"reason"`
}

// SetCooldownRequest manually starts a cooldown; Duration is a Go duration.
type SetCooldownRequest struct {
	EgressKey string `json:"egress_key"`
	Site      string `json:"site"`
	Duration  string `json:"duration"`
	Reason    string `json:"reason"`
}

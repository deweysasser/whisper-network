package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/sched"
)

// Eligibility describes what one worker slot may take right now. Which
// queues it may take from is not part of it: Claim reads the worker's
// process or fetch grants inside its transaction, so a grant revoked a
// moment before the claim is always honoured. Nor is the worker's kind:
// Claim reads it too, and offers a processing worker only prepare and
// transcribe tasks and a downloader only fetch tasks, whatever it asks for.
type Eligibility struct {
	// Kinds are the task kinds the slot offers.
	Kinds []api.TaskKind
	// Models are the whisper models the slot can run now; transcribe tasks
	// of other models are skipped.
	Models []string
	// SandboxLevel is the worker's sandbox level; prepare tasks are offered
	// only if it meets Config.PrepareMinSandbox.
	SandboxLevel api.SandboxLevel
	// PrepareBounded limits prepare tasks to originals of at most
	// PrepareMaxInput bytes: the worker reported how large an input its
	// disk can take now. Unbounded when the worker did not say.
	PrepareBounded  bool
	PrepareMaxInput int64
	// EgressKey is the downloader's egress key for fetch pacing. When
	// empty, Claim uses the worker's stored egress_key.
	EgressKey string
	// WaitingGroups are the egress keys of downloaders with a fetch claim
	// waiting now (from the server), for rotation: see blockedFetchSites.
	WaitingGroups []string
}

// Lease is a lease together with what a worker needs to run its task.
type Lease struct {
	ID        string
	JobID     string
	Seq       int
	Kind      api.TaskKind
	WorkerID  string
	QueueID   int64
	QueueName string
	Weight    api.Weight
	// Transcribe parameters.
	Model, Language, Prompt string
	StartSample, EndSample  int64
	// Prepare input.
	OriginalBytes  int64
	OriginalSHA256 string
	// Fetch input: the URL, its site key, and the byte cap on the download
	// (the queue's upload cap, bounded by Config.FetchMaxBytes; 0 = none).
	URL, Site string
	MaxBytes  int64
	// EgressKey is the egress a fetch lease was claimed from ("" for other
	// kinds).
	EgressKey string
	// NoContext is set when an earlier lease on the task (since the job was
	// last retried) was refused as a repetition loop: this one runs without
	// previous-text conditioning or a prompt (issue #99).
	NoContext bool
	// Attempt is the task's attempt count including this lease.
	Attempt   int
	Cost      float64
	CreatedAt time.Time
	ExpiresAt time.Time
	EndedAt   *time.Time
	Outcome   api.LeaseOutcome // "" while live
	// Live is true if the lease had not ended or expired when it was read.
	Live bool
	// VTime is the queue's virtual time after this claim was charged. It is
	// set only on leases returned by Claim (for the task.leased log event).
	VTime float64
}

// API builds the wire lease. The server fills in the model and VAD hashes.
func (l *Lease) API(heartbeatInterval time.Duration) api.Lease {
	out := api.Lease{
		ID: l.ID, Kind: l.Kind, Job: l.JobID, Seq: l.Seq,
		InputURL:           "/lease/" + l.ID + "/input",
		HeartbeatIntervalS: int(heartbeatInterval / time.Second),
		ExpiresAt:          l.ExpiresAt,
	}
	switch l.Kind {
	case api.KindPrepare:
		out.InputBytes, out.InputSHA256 = l.OriginalBytes, l.OriginalSHA256
	case api.KindFetch:
		out.URL, out.Site, out.MaxOutputBytes = l.URL, l.Site, l.MaxBytes
	case api.KindTranscribe:
		out.AudioSamples = l.EndSample - l.StartSample
		out.Model, out.Language, out.Prompt = l.Model, l.Language, l.Prompt
		if l.NoContext {
			// The server drops the prompt itself, so even a worker that
			// predates no_context retries without it.
			out.NoContext, out.Prompt = true, ""
		}
	}
	return out
}

// processGrantSQL is a condition (over j = jobs, with the worker principal
// as its one argument) that the worker holds process on the job's queue.
const processGrantSQL = `EXISTS (SELECT 1 FROM grants g WHERE g.principal = ? AND g.role = 'process'
	AND (g.scope = '*' OR g.scope = 'queue:' || j.queue_id))`

// failedHereSQL is a condition (over t = tasks and j = jobs, with the worker
// id as its one argument) that the worker has a failed (other than before
// work started) or expired lease on the task since the job was last retried.
// The
// t.attempts check skips the lookup for tasks that never failed anywhere: a
// task's attempts count the distinct workers its failed and expired leases
// ran on (plus one while it is leased), so it is 0 for a pending task with
// none.
const failedHereSQL = `(t.attempts > 0 AND EXISTS (SELECT 1 FROM leases x WHERE x.job_id = t.job_id AND x.seq = t.seq
	AND x.worker_id = ? AND x.outcome IN ('failed','expired') AND x.stage IS NOT 'pre-work'
	AND x.retry = j.retries))`

// claimant is what Claim reads about the claiming worker in its
// transaction.
type claimant struct {
	kind api.WorkerKind
	// fetch is set when the worker may be offered fetch tasks now, and
	// blocked then lists the sites pacing holds back (blockedFetchSites).
	fetch   bool
	blocked []string
}

// eligibilitySQL returns the WHERE fragment (over t = tasks, j = jobs) that
// selects pending tasks workerID may take, or ok = false if none can match.
// The queue condition reads the worker's grants, so it is evaluated inside
// the claim's transaction. Each task kind has its own branch:
//
//	(t.kind = 'transcribe' AND <model> AND <process grant>)
//	OR (t.kind = 'prepare' AND <sandbox, input size> AND <process grant>)
//	OR (t.kind = 'fetch' AND <fetch grant> AND <site not paced out>)
//
// with the transcribe and prepare branches only for processing workers and
// the fetch branch only for downloaders.
//
// It also leaves out every task the worker failed or let expire: a worker
// never gets such a task back (retry elsewhere, DESIGN.md §8.2). Because the
// one fragment drives both the candidate (queue, class) pairs and the task
// pick, an excluded task is never charged to its queue, and a pair whose
// only pending tasks are excluded is not a candidate for this worker.
func (s *Store) eligibilitySQL(workerID string, c claimant, el Eligibility) (where string, args []any, ok bool) {
	principal := WorkerPrincipal(workerID)
	var kinds []string
	if c.kind == api.WorkerProcessing {
		if slices.Contains(el.Kinds, api.KindTranscribe) && len(el.Models) > 0 {
			kinds = append(kinds, "(t.kind = 'transcribe' AND j.model IN ("+placeholders(len(el.Models))+") AND "+
				processGrantSQL+")")
			for _, m := range el.Models {
				args = append(args, m)
			}
			args = append(args, principal)
		}
		if slices.Contains(el.Kinds, api.KindPrepare) && el.SandboxLevel.Rank() >= s.cfg.PrepareMinSandbox.Rank() {
			if el.PrepareBounded {
				kinds = append(kinds, "(t.kind = 'prepare' AND j.original_bytes <= ? AND "+processGrantSQL+")")
				args = append(args, el.PrepareMaxInput, principal)
			} else {
				kinds = append(kinds, "(t.kind = 'prepare' AND "+processGrantSQL+")")
				args = append(args, principal)
			}
		}
	}
	if c.kind == api.WorkerDownloader && c.fetch {
		cond := "(t.kind = 'fetch' AND " + fetchGrantSQL
		args = append(args, principal)
		if len(c.blocked) > 0 {
			cond += " AND COALESCE(j.fetch_site, '') NOT IN (" + placeholders(len(c.blocked)) + ")"
			for _, site := range c.blocked {
				args = append(args, site)
			}
		}
		kinds = append(kinds, cond+")")
	}
	if len(kinds) == 0 {
		return "", nil, false
	}
	where = "t.state = 'pending' AND (" + strings.Join(kinds, " OR ") + ") AND NOT " + failedHereSQL
	args = append(args, workerID)
	return where, args, true
}

// Claim leases one task to workerID, or returns nil, nil if nothing eligible
// is pending. It runs in a single BEGIN IMMEDIATE transaction: find the
// candidate (queue, class) pairs, let the scheduler pick one, charge its
// virtual times, take the oldest job's lowest pending task in that pair, and
// create the lease with expiry now+ttl. A worker that is not active gets
// ErrWorkerInactive.
func (s *Store) Claim(ctx context.Context, workerID string, el Eligibility, ttl time.Duration) (*Lease, error) {
	if ttl <= 0 {
		return nil, fmt.Errorf("%w: lease ttl must be positive", ErrInvalid)
	}
	var lease *Lease
	err := s.write(ctx, func(tx *sql.Tx) error {
		var state api.WorkerState
		var c claimant
		var egress string
		if err := tx.QueryRowContext(ctx, `SELECT state, kind, egress_key FROM workers WHERE id = ?`,
			workerID).Scan(&state, &c.kind, &egress); err != nil {
			return notFound(err, "worker "+workerID)
		}
		if state != api.WorkerActive {
			return fmt.Errorf("claim by %s worker: %w", state, ErrWorkerInactive)
		}
		if el.EgressKey != "" {
			egress = el.EgressKey
		}
		now := s.now()
		c.fetch = c.kind == api.WorkerDownloader && slices.Contains(el.Kinds, api.KindFetch) &&
			el.SandboxLevel.Rank() >= s.cfg.FetchMinSandbox.Rank()
		if c.fetch {
			var err error
			if c.blocked, err = (&Tx{s: s, tx: tx}).blockedFetchSites(ctx, egress, el.WaitingGroups, now); err != nil {
				return err
			}
		}
		where, args, ok := s.eligibilitySQL(workerID, c, el)
		if !ok {
			return nil
		}
		var err error
		if lease, err = s.claim(ctx, tx, workerID, where, args, ttl, now); err != nil || lease == nil {
			return err
		}
		if lease.Kind == api.KindFetch {
			// In the claim's transaction, so a concurrent claim sees this
			// start and cannot take the same slot.
			return (&Tx{s: s, tx: tx}).recordFetchStart(ctx, lease, egress, now)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return lease, nil
}

func (s *Store) claim(ctx context.Context, tx *sql.Tx, workerID, where string, args []any, ttl time.Duration,
	now time.Time) (*Lease, error) {
	cands, weights, err := candidates(ctx, tx, where, args)
	if err != nil || len(cands) == 0 {
		return nil, err
	}
	st, err := loadSched(ctx, tx)
	if err != nil {
		return nil, err
	}
	k, _ := st.Pick(cands)

	var jobID string
	var seq int
	var cost float64
	err = tx.QueryRowContext(ctx, `SELECT t.job_id, t.seq, t.cost FROM tasks t JOIN jobs j ON j.id = t.job_id
		WHERE `+where+` AND j.queue_id = ? AND j.weight = ? ORDER BY j.created_at, j.id, t.seq LIMIT 1`,
		append(slices.Clone(args), k.Queue, string(k.Class))...).Scan(&jobID, &seq, &cost)
	if err != nil {
		return nil, fmt.Errorf("claim: select task: %w", err)
	}

	st.Charge(k, cost, float64(weights[k.Queue]), s.cfg.ClassWeights)
	if err := saveSched(ctx, tx, st, sched.QueueKey(k.Queue), k); err != nil {
		return nil, err
	}

	res, err := tx.ExecContext(ctx, `UPDATE tasks SET state = 'leased', attempts = attempts + 1, worker_id = ?
		WHERE job_id = ? AND seq = ? AND state = 'pending'`, workerID, jobID, seq)
	if err := mustAffect(res, err, "claim: lease task"); err != nil {
		return nil, err
	}
	id := s.cfg.NewID(IDLease)
	if _, err := tx.ExecContext(ctx, `INSERT INTO leases (id, job_id, seq, worker_id, created_at, expires_at, retry)
		VALUES (?,?,?,?,?,?,(SELECT retries FROM jobs WHERE id = ?))`,
		id, jobID, seq, workerID, toMS(now), toMS(now.Add(ttl)), jobID); err != nil {
		return nil, fmt.Errorf("claim: insert lease: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET started_at = COALESCE(started_at, ?), updated_at = ?,
		state = CASE state WHEN 'queued' THEN 'running' ELSE state END WHERE id = ?`,
		toMS(now), toMS(now), jobID); err != nil {
		return nil, fmt.Errorf("claim: update job: %w", err)
	}
	l, err := s.loadLease(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	l.VTime = st.Get(sched.QueueKey(k.Queue))
	return l, nil
}

// candidates returns the (queue, class) pairs with an eligible pending task
// and the weights of their queues.
func candidates(ctx context.Context, tx *sql.Tx, where string, args []any) ([]sched.Key, map[int64]int, error) {
	rows, err := tx.QueryContext(ctx, `SELECT j.queue_id, j.weight, q.weight FROM tasks t
		JOIN jobs j ON j.id = t.job_id JOIN queues q ON q.id = j.queue_id
		WHERE `+where+` GROUP BY j.queue_id, j.weight`, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("claim: candidates: %w", err)
	}
	defer rows.Close()
	var cands []sched.Key
	weights := map[int64]int{}
	for rows.Next() {
		var k sched.Key
		var w int
		if err := rows.Scan(&k.Queue, &k.Class, &w); err != nil {
			return nil, nil, fmt.Errorf("claim: candidates: %w", err)
		}
		cands = append(cands, k)
		weights[k.Queue] = w
	}
	return cands, weights, rows.Err()
}

const leaseSelect = `SELECT l.id, l.job_id, l.seq, t.kind, l.worker_id, j.queue_id, q.name, j.weight, j.model,
	j.language, j.prompt, COALESCE(t.start_sample, 0), COALESCE(t.end_sample, 0), j.original_bytes,
	j.original_sha256, t.attempts, t.cost, l.created_at, l.expires_at, l.ended_at, COALESCE(l.outcome, ''),
	COALESCE(j.fetch_url, ''), COALESCE(j.fetch_site, ''), COALESCE(q.max_upload_bytes, 0), COALESCE(l.egress_key, ''),
	EXISTS (SELECT 1 FROM leases x WHERE x.job_id = l.job_id AND x.seq = l.seq AND x.outcome = 'failed' AND x.stage = 'loop'
		AND x.retry = j.retries AND x.created_at <= l.created_at AND x.id != l.id)
	FROM leases l JOIN tasks t ON t.job_id = l.job_id AND t.seq = l.seq
	JOIN jobs j ON j.id = l.job_id JOIN queues q ON q.id = j.queue_id`

func (s *Store) loadLease(ctx context.Context, q querier, id string) (*Lease, error) {
	var l Lease
	var created, expires int64
	var ended sql.NullInt64
	var queueMax int64
	err := q.QueryRowContext(ctx, leaseSelect+` WHERE l.id = ?`, id).Scan(&l.ID, &l.JobID, &l.Seq, &l.Kind,
		&l.WorkerID, &l.QueueID, &l.QueueName, &l.Weight, &l.Model, &l.Language, &l.Prompt, &l.StartSample,
		&l.EndSample, &l.OriginalBytes, &l.OriginalSHA256, &l.Attempt, &l.Cost, &created, &expires, &ended, &l.Outcome,
		&l.URL, &l.Site, &queueMax, &l.EgressKey, &l.NoContext)
	if err != nil {
		return nil, err
	}
	if l.Kind == api.KindFetch {
		l.MaxBytes = fetchMaxBytes(queueMax, s.cfg.FetchMaxBytes)
	}
	l.CreatedAt = fromMS(created)
	l.ExpiresAt = fromMS(expires)
	l.EndedAt = optTime(ended)
	l.Live = l.EndedAt == nil && l.ExpiresAt.After(s.now())
	return &l, nil
}

// Lease returns a lease held by workerID with its task details, live or not.
// It returns ErrNotFound if the lease is unknown or held by another worker,
// and ErrLeaseGone if its job has been deleted.
func (s *Store) Lease(ctx context.Context, leaseID, workerID string) (*Lease, error) {
	var holder string
	if err := s.r.QueryRowContext(ctx, `SELECT worker_id FROM leases WHERE id = ?`, leaseID).Scan(&holder); err != nil {
		return nil, notFound(err, "lease")
	}
	if holder != workerID {
		return nil, fmt.Errorf("lease: %w", ErrNotFound)
	}
	l, err := s.loadLease(ctx, s.r, leaseID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("lease %s: job deleted: %w", leaseID, ErrLeaseGone)
	}
	if err != nil {
		return nil, fmt.Errorf("lease %s: %w", leaseID, err)
	}
	return l, nil
}

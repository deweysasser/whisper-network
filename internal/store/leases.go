package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
)

// LeaseEnd reports how a lease ended and what happened to its task.
type LeaseEnd struct {
	LeaseID  string
	JobID    string
	Seq      int
	Kind     api.TaskKind
	WorkerID string
	Outcome  api.LeaseOutcome
	// Attempt is the task's counted attempts after this lease ended: the
	// number of distinct workers it has failed or expired on (a release or
	// cancellation does not count).
	Attempt int
	// Requeued is true if the task went back to pending.
	Requeued bool
	// JobFailed is true if this lease end made its job failed: the job
	// was failing (a task of it failed for good, possibly this one) and
	// this was its last live lease.
	JobFailed bool
	// AutoPaused is set if this lease end tripped the worker circuit
	// breaker and the worker was paused in the same transaction.
	AutoPaused *AutoPause
}

// leaseRow is a lease joined with the scheduling facts of its task.
type leaseRow struct {
	ID, JobID, WorkerID string
	Seq                 int
	Kind                api.TaskKind
	Attempts            int
	QueueID             int64
	Weight              api.Weight
	ExpiresAt           int64
	Ended               bool
	Outcome             api.LeaseOutcome
	TaskExists          bool
}

const leaseRowSelect = `SELECT l.id, l.job_id, l.worker_id, l.seq, COALESCE(t.kind, ''), COALESCE(t.attempts, 0),
	COALESCE(j.queue_id, 0), COALESCE(j.weight, ''), l.expires_at, l.ended_at IS NOT NULL, COALESCE(l.outcome, ''),
	t.job_id IS NOT NULL
	FROM leases l LEFT JOIN tasks t ON t.job_id = l.job_id AND t.seq = l.seq LEFT JOIN jobs j ON j.id = l.job_id`

func scanLeaseRow(row interface{ Scan(...any) error }) (leaseRow, error) {
	var l leaseRow
	err := row.Scan(&l.ID, &l.JobID, &l.WorkerID, &l.Seq, &l.Kind, &l.Attempts, &l.QueueID, &l.Weight,
		&l.ExpiresAt, &l.Ended, &l.Outcome, &l.TaskExists)
	return l, err
}

// liveLeases returns the unended leases matching cond (over l = leases).
func liveLeases(ctx context.Context, tx *sql.Tx, cond string, args ...any) ([]leaseRow, error) {
	rows, err := tx.QueryContext(ctx, leaseRowSelect+` WHERE l.ended_at IS NULL AND `+cond+` ORDER BY l.id`, args...)
	if err != nil {
		return nil, fmt.Errorf("live leases: %w", err)
	}
	defer rows.Close()
	var out []leaseRow
	for rows.Next() {
		l, err := scanLeaseRow(rows)
		if err != nil {
			return nil, fmt.Errorf("live leases: %w", err)
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// heldLease loads a lease for its holder: ErrNotFound if unknown or held by
// another worker.
func heldLease(ctx context.Context, tx *sql.Tx, leaseID, workerID string) (leaseRow, error) {
	l, err := scanLeaseRow(tx.QueryRowContext(ctx, leaseRowSelect+` WHERE l.id = ?`, leaseID))
	if err != nil {
		return l, notFound(err, "lease")
	}
	if l.WorkerID != workerID {
		return l, fmt.Errorf("lease: %w", ErrNotFound)
	}
	return l, nil
}

// liveHeldLease is heldLease plus ErrLeaseGone unless the lease is live.
func (s *Store) liveHeldLease(ctx context.Context, tx *sql.Tx, leaseID, workerID string, now time.Time) (leaseRow, error) {
	l, err := heldLease(ctx, tx, leaseID, workerID)
	if err != nil {
		return l, err
	}
	if err := s.checkLive(l, now); err != nil {
		return l, err
	}
	return l, nil
}

func (s *Store) checkLive(l leaseRow, now time.Time) error {
	switch {
	case !l.TaskExists:
		return fmt.Errorf("lease %s: job deleted: %w", l.ID, ErrLeaseGone)
	case l.Ended:
		return fmt.Errorf("lease %s: %s: %w", l.ID, l.Outcome, ErrLeaseGone)
	case l.ExpiresAt <= toMS(now):
		return fmt.Errorf("lease %s: expired: %w", l.ID, ErrLeaseGone)
	}
	return nil
}

// endLease ends a live lease with outcome, counts it in its worker's
// counters (failed and expired only) and failure streak (which may pause
// the worker, see trackFailureStreak), and disposes of its task:
//   - the task's attempts become the number of distinct workers it has
//     failed or expired on, not counting pre-work failures (DESIGN.md
//     §4.2), so a release, cancellation or pre-work failure refunds the
//     attempt its lease was charged, and a second failed or expired lease
//     on the same worker counts nothing;
//   - the task requeues if retryable and those attempts are below
//     MaxAttempts, applying the scheduler's reactivation rule; a pre-work
//     failure always requeues (see countsAsAttempt);
//   - otherwise the task fails for good and its job becomes failing
//     (doomJob);
//   - in a failing job a task that would requeue is cancelled instead, and
//     the job becomes failed once no live lease is left (settleJob).
//
// stage is the stage a failing worker reported ("" if none). It returns nil
// if the lease had already ended.
func (s *Store) endLease(ctx context.Context, tx *sql.Tx, l leaseRow, outcome api.LeaseOutcome,
	retryable bool, reason string, stage api.FailureStage, now time.Time) (*LeaseEnd, error) {
	if outcome != api.OutcomeFailed || !stage.Valid() && stage != api.StageLoop ||
		stage == api.StagePreWork && !s.preWorkRefund() ||
		stage == api.StageRateLimited && l.Kind != api.KindFetch {
		stage = "" // a pre-work failure then counts as an ordinary one
	}
	// A loop is the model's failure, not the worker's: it counts as an
	// attempt but not toward the worker's breaker.
	loop := stage == api.StageLoop
	stageCol := sql.NullString{String: string(stage), Valid: stage != ""}
	res, err := tx.ExecContext(ctx, `UPDATE leases SET ended_at = ?, outcome = ?, stage = ?
		WHERE id = ? AND ended_at IS NULL`, toMS(now), string(outcome), stageCol, l.ID)
	if err != nil {
		return nil, fmt.Errorf("end lease %s: %w", l.ID, err)
	}
	if n, err := res.RowsAffected(); err != nil || n == 0 {
		return nil, err
	}
	preWork := stage == api.StagePreWork
	// A rate-limited fetch is the site's refusal, not the worker's or the
	// input's: like pre-work it requeues with no attempt and no exclusion,
	// but it is not a failure of the worker at all (no streak), and it sets
	// a cooldown on its egress group x site.
	rateLimited := stage == api.StageRateLimited
	if err := countLeaseEnd(ctx, tx, l.WorkerID, outcome); err != nil {
		return nil, err
	}
	pause, err := s.trackFailureStreak(ctx, tx, l, outcome, (retryable || preWork) && !loop && !rateLimited, preWork, now)
	if err != nil {
		return nil, err
	}
	workers, err := failedOn(ctx, tx, l.JobID, l.Seq)
	if err != nil {
		return nil, err
	}
	end := &LeaseEnd{LeaseID: l.ID, JobID: l.JobID, Seq: l.Seq, Kind: l.Kind, WorkerID: l.WorkerID,
		Outcome: outcome, Attempt: len(workers), AutoPaused: pause}
	errText := sql.NullString{String: reason, Valid: reason != ""}
	failing, err := jobFailing(ctx, tx, l.JobID)
	if err != nil {
		return nil, err
	}

	if rateLimited {
		if err := s.startRateLimitCooldown(ctx, tx, l, now); err != nil {
			return nil, err
		}
	}

	switch {
	case (preWork || rateLimited || retryable && end.Attempt < s.cfg.MaxAttempts) && !failing:
		if err := reactivate(ctx, tx, l.QueueID, l.Weight); err != nil {
			return nil, err
		}
		// A rate-limited task keeps its pending_since, so FailStranded
		// measures the wait from when it first became pending.
		pendingSince := sql.NullInt64{Int64: toMS(now), Valid: !rateLimited}
		if _, err := tx.ExecContext(ctx, `UPDATE tasks SET state = 'pending', attempts = ?,
			error = COALESCE(?, error), pending_since = COALESCE(?, pending_since)
			WHERE job_id = ? AND seq = ? AND state = 'leased'`,
			end.Attempt, errText, pendingSince, l.JobID, l.Seq); err != nil {
			return nil, fmt.Errorf("requeue task: %w", err)
		}
		end.Requeued = true
		return end, nil
	case preWork || rateLimited || retryable && end.Attempt < s.cfg.MaxAttempts:
		// The task could run again, but its job is failing.
		if _, err := tx.ExecContext(ctx, `UPDATE tasks SET state = ?, attempts = ?, error = COALESCE(?, error)
			WHERE job_id = ? AND seq = ? AND state = 'leased'`,
			taskCancelled, end.Attempt, errText, l.JobID, l.Seq); err != nil {
			return nil, fmt.Errorf("cancel task: %w", err)
		}
	default:
		if _, err := tx.ExecContext(ctx, `UPDATE tasks SET state = 'failed', attempts = ?, error = COALESCE(?, error)
			WHERE job_id = ? AND seq = ?`, end.Attempt, errText, l.JobID, l.Seq); err != nil {
			return nil, fmt.Errorf("fail task: %w", err)
		}
		msg := failedAttempts(l.Kind, l.Seq, workers)
		if reason != "" {
			msg += ": " + reason
		}
		if err := doomJob(ctx, tx, l.JobID, msg, now); err != nil {
			return nil, err
		}
	}
	end.JobFailed, err = settleJob(ctx, tx, l.JobID, now)
	if err != nil {
		return nil, err
	}
	return end, nil
}

// preWorkRefund reports whether a pre-work failure is refunded (DESIGN.md
// §8.2). Only the circuit breaker stops a worker whose every lease fails
// before work starts, so with the breaker off (WorkerFailureLimit 0) such a
// failure counts as an ordinary one: the worker is then kept from the task
// as before instead of taking it again and again.
func (s *Store) preWorkRefund() bool { return s.cfg.WorkerFailureLimit > 0 }

// failedAttempts begins a failed job's error: which task failed, after how
// many counted attempts, and on which workers.
func failedAttempts(kind api.TaskKind, seq int, workers []string) string {
	msg := fmt.Sprintf("%s task %d failed after %d attempt(s)", kind, seq, len(workers))
	switch len(workers) {
	case 0:
	case 1:
		msg += " (on worker " + workers[0] + ")"
	default:
		msg += " (on workers " + strings.Join(workers, ", ") + ")"
	}
	return msg
}

// countedFailureSQL is a condition (over leases, unqualified) that a lease
// counts as a failed attempt: it failed, other than before work started, or
// expired, since the job was last retried (leases.retry, DESIGN.md §4.2).
const countedFailureSQL = `outcome IN ('failed','expired') AND stage IS NOT 'pre-work' AND stage IS NOT 'rate_limited'
	AND retry = (SELECT retries FROM jobs WHERE jobs.id = leases.job_id)`

// failedOn returns the distinct workers that failed a task (not counting
// pre-work failures) or let a lease on it expire, in the order of their first such lease. Their number is the
// task's counted attempts, and a failed job's error names them.
func failedOn(ctx context.Context, tx *sql.Tx, jobID string, seq int) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT worker_id FROM leases WHERE job_id = ? AND seq = ?
		AND `+countedFailureSQL+` GROUP BY worker_id ORDER BY MIN(created_at), MIN(rowid)`, jobID, seq)
	if err != nil {
		return nil, fmt.Errorf("failed attempts: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var w string
		if err := rows.Scan(&w); err != nil {
			return nil, fmt.Errorf("failed attempts: %w", err)
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// LeaseAgeLimit bounds how long heartbeats can keep a lease alive: the
// limit is max(Min, PerCost × the task's cost in seconds). Task cost is
// audio seconds × model factor (prepare: a fixed cost), so long and slow
// tasks get proportionally longer. A zero LeaseAgeLimit sets no limit.
type LeaseAgeLimit struct {
	Min     time.Duration
	PerCost float64
}

func (a LeaseAgeLimit) of(cost float64) time.Duration {
	if a.Min <= 0 && a.PerCost <= 0 {
		return 0
	}
	return max(a.Min, time.Duration(a.PerCost*cost*float64(time.Second)))
}

// Heartbeat extends a live lease to now+ttl and returns the new expiry.
// ErrLeaseGone if the lease is no longer live, or may no longer be kept
// alive: its worker is neither active nor paused, the worker no longer
// holds process on the lease's queue, or the lease is older than age
// allows. The worker must stop; the lease then expires within ttl.
func (s *Store) Heartbeat(ctx context.Context, leaseID, workerID string, ttl time.Duration, age LeaseAgeLimit) (time.Time, error) {
	now := s.now()
	exp := fromMS(toMS(now.Add(ttl)))
	err := s.write(ctx, func(tx *sql.Tx) error {
		l, err := s.liveHeldLease(ctx, tx, leaseID, workerID, now)
		if err != nil {
			return err
		}
		var state api.WorkerState
		var granted bool
		var created int64
		var cost float64
		if err := tx.QueryRowContext(ctx, `SELECT w.state, `+processGrantSQL+`, l.created_at, t.cost
			FROM leases l JOIN workers w ON w.id = l.worker_id JOIN jobs j ON j.id = l.job_id
			JOIN tasks t ON t.job_id = l.job_id AND t.seq = l.seq WHERE l.id = ?`,
			WorkerPrincipal(workerID), leaseID).Scan(&state, &granted, &created, &cost); err != nil {
			return fmt.Errorf("heartbeat: %w", err)
		}
		switch {
		case state != api.WorkerActive && state != api.WorkerPaused:
			return fmt.Errorf("lease %s: worker is %s: %w", l.ID, state, ErrLeaseGone)
		case !granted:
			return fmt.Errorf("lease %s: process no longer granted on its queue: %w", l.ID, ErrLeaseGone)
		}
		if limit := age.of(cost); limit > 0 && now.Sub(fromMS(created)) >= limit {
			return fmt.Errorf("lease %s: older than its %v limit: %w", l.ID, limit, ErrLeaseGone)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE leases SET expires_at = ?, heartbeat_at = ? WHERE id = ?`,
			toMS(exp), toMS(now), leaseID); err != nil {
			return fmt.Errorf("heartbeat: %w", err)
		}
		return nil
	})
	if err != nil {
		return time.Time{}, err
	}
	return exp, nil
}

// TranscribeCompletion is a validated transcribe result.
type TranscribeCompletion struct {
	// Result is the segments JSON as stored for assembly (times relative to
	// the task start). It is stored compacted.
	Result        json.RawMessage
	EngineVersion string
	ComputeMS     int64
}

// CompleteTranscribe records a transcribe result and ends the lease as done.
// It reports whether every transcribe task of the job is now done, so the
// caller can assemble, or whether the job, failing because a sibling failed
// for good, is now failed. A failing job's leased tasks still complete and
// their results are kept.
//
// It is idempotent: repeating it for a lease that already completed with an
// identical result succeeds again. A different result for a completed lease
// is ErrConflict; any other non-live lease is ErrLeaseGone.
func (s *Store) CompleteTranscribe(ctx context.Context, leaseID, workerID string, c TranscribeCompletion) (TranscribeEnd, error) {
	var buf bytes.Buffer
	if err := json.Compact(&buf, c.Result); err != nil {
		return TranscribeEnd{}, fmt.Errorf("%w: result is not JSON: %w", ErrInvalid, err)
	}
	result := buf.String()
	now := s.now()
	var end TranscribeEnd
	err := s.write(ctx, func(tx *sql.Tx) error {
		l, err := heldLease(ctx, tx, leaseID, workerID)
		if err != nil {
			return err
		}
		if l.TaskExists && l.Kind != api.KindTranscribe {
			return fmt.Errorf("%w: lease %s is a %s lease", ErrInvalid, leaseID, l.Kind)
		}
		if l.TaskExists && l.Ended && l.Outcome == api.OutcomeDone {
			var stored sql.NullString
			if err := tx.QueryRowContext(ctx, `SELECT result FROM tasks WHERE job_id = ? AND seq = ?`,
				l.JobID, l.Seq).Scan(&stored); err != nil {
				return fmt.Errorf("complete: %w", err)
			}
			if stored.String != result {
				return fmt.Errorf("lease %s already completed with a different result: %w", leaseID, ErrConflict)
			}
			end.JobComplete, err = transcriptionComplete(ctx, tx, l.JobID)
			return err
		}
		if err := s.checkLive(l, now); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `UPDATE tasks SET state = 'done', result = ?, engine_version = ?,
			compute_ms = ?, worker_id = ? WHERE job_id = ? AND seq = ? AND state = 'leased'`,
			result, c.EngineVersion, c.ComputeMS, workerID, l.JobID, l.Seq)
		if err := mustAffect(res, err, "complete task"); err != nil {
			return err
		}
		if err := countCompletion(ctx, tx, l, c.ComputeMS, now); err != nil {
			return err
		}
		if err := finishLease(ctx, tx, l, now); err != nil {
			return err
		}
		if end.JobComplete, err = transcriptionComplete(ctx, tx, l.JobID); err != nil || end.JobComplete {
			return err
		}
		// A sibling may have failed for good while this task ran.
		end.JobFailed, err = settleJob(ctx, tx, l.JobID, now)
		return err
	})
	return end, err
}

// TranscribeEnd says what a transcribe completion did to its job.
type TranscribeEnd struct {
	// JobComplete is true once every transcribe task of the job is done;
	// the caller can assemble.
	JobComplete bool
	// JobFailed is true if the job was failing (a sibling failed for good)
	// and this was its last live lease, so the job is now failed.
	JobFailed bool
}

// finishLease ends a lease as done and bumps its job's updated_at.
func finishLease(ctx context.Context, tx *sql.Tx, l leaseRow, now time.Time) error {
	if _, err := tx.ExecContext(ctx, `UPDATE leases SET ended_at = ?, outcome = 'done' WHERE id = ?`,
		toMS(now), l.ID); err != nil {
		return fmt.Errorf("finish lease: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET updated_at = ? WHERE id = ?`, toMS(now), l.JobID); err != nil {
		return fmt.Errorf("finish lease: %w", err)
	}
	return nil
}

func transcriptionComplete(ctx context.Context, q querier, jobID string) (bool, error) {
	var total, done int
	err := q.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(state = 'done'), 0) FROM tasks
		WHERE job_id = ? AND kind = 'transcribe'`, jobID).Scan(&total, &done)
	if err != nil {
		return false, fmt.Errorf("transcription complete: %w", err)
	}
	return total > 0 && total == done, nil
}

// PrepareCompletion is validated canonical audio produced by a prepare task.
type PrepareCompletion struct {
	Canonical     Canonical
	Ranges        []SampleRange
	ComputeMS     int64
	FFmpegVersion string
}

// AcceptPrepare atomically completes a prepare lease: the prepare task is
// done, the canonical audio is recorded, one transcribe task per range is
// created (with reactivation), and the job becomes queued. Repeating it for
// a lease that already completed with the same canonical hash succeeds
// again; otherwise a non-live lease is ErrLeaseGone.
func (s *Store) AcceptPrepare(ctx context.Context, leaseID, workerID string, p PrepareCompletion) error {
	if err := validRanges(p.Ranges, p.Canonical.Samples); err != nil {
		return err
	}
	now := s.now()
	return s.write(ctx, func(tx *sql.Tx) error {
		l, err := heldLease(ctx, tx, leaseID, workerID)
		if err != nil {
			return err
		}
		if l.TaskExists && l.Kind != api.KindPrepare {
			return fmt.Errorf("%w: lease %s is a %s lease", ErrInvalid, leaseID, l.Kind)
		}
		if l.TaskExists && l.Ended && l.Outcome == api.OutcomeDone {
			var sha sql.NullString
			if err := tx.QueryRowContext(ctx, `SELECT canonical_sha256 FROM jobs WHERE id = ?`, l.JobID).Scan(&sha); err != nil {
				return fmt.Errorf("accept prepare: %w", err)
			}
			if sha.String == p.Canonical.SHA256 {
				return nil
			}
			return fmt.Errorf("lease %s already completed with different audio: %w", leaseID, ErrConflict)
		}
		if err := s.checkLive(l, now); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `UPDATE tasks SET state = 'done', engine_version = ?, compute_ms = ?,
			worker_id = ? WHERE job_id = ? AND seq = ? AND state = 'leased'`,
			p.FFmpegVersion, p.ComputeMS, workerID, l.JobID, l.Seq)
		if err := mustAffect(res, err, "complete prepare task"); err != nil {
			return err
		}
		if err := countCompletion(ctx, tx, l, p.ComputeMS, now); err != nil {
			return err
		}
		if err := finishLease(ctx, tx, l, now); err != nil {
			return err
		}
		var model string
		if err := tx.QueryRowContext(ctx, `SELECT model FROM jobs WHERE id = ?`, l.JobID).Scan(&model); err != nil {
			return fmt.Errorf("accept prepare: %w", err)
		}
		if err := reactivate(ctx, tx, l.QueueID, l.Weight); err != nil {
			return err
		}
		if err := s.insertTranscribeTasks(ctx, tx, l.JobID, model, p.Ranges); err != nil {
			return err
		}
		res, err = tx.ExecContext(ctx, `UPDATE jobs SET state = 'queued', canonical_bytes = ?, canonical_sha256 = ?,
			audio_samples = ? WHERE id = ? AND state = 'preparing'`,
			p.Canonical.Bytes, p.Canonical.SHA256, p.Canonical.Samples, l.JobID)
		return mustAffect(res, err, "accept prepare: job not preparing")
	})
}

// Fail ends a live lease as failed. The task requeues if retryable and it
// has failed on fewer than MaxAttempts distinct workers; otherwise the task
// fails for good and its job fails once no lease of it is live. The worker
// is never offered the task again.
func (s *Store) Fail(ctx context.Context, leaseID, workerID, reason string, retryable bool) (*LeaseEnd, error) {
	return s.FailStage(ctx, leaseID, workerID, reason, retryable, "")
}

// FailStage is Fail with the stage the worker reported. A pre-work failure
// requeues the task without counting an attempt and without keeping the
// worker from it, but counts toward the worker's failure streak (unless the
// breaker is off, see preWorkRefund). Any other stage is recorded and
// behaves as Fail.
func (s *Store) FailStage(ctx context.Context, leaseID, workerID, reason string, retryable bool,
	stage api.FailureStage) (*LeaseEnd, error) {
	return s.endHeld(ctx, leaseID, workerID, api.OutcomeFailed, retryable, reason, stage)
}

// Release ends a live lease voluntarily (graceful worker shutdown). The task
// returns to pending and the attempt the lease was charged is refunded, so
// a release never counts toward MaxAttempts and does not keep the worker
// from taking the task again. Scheduler cost is not refunded.
func (s *Store) Release(ctx context.Context, leaseID, workerID string) (*LeaseEnd, error) {
	return s.endHeld(ctx, leaseID, workerID, api.OutcomeReleased, true, "", "")
}

func (s *Store) endHeld(ctx context.Context, leaseID, workerID string, outcome api.LeaseOutcome,
	retryable bool, reason string, stage api.FailureStage) (*LeaseEnd, error) {
	now := s.now()
	var end *LeaseEnd
	err := s.write(ctx, func(tx *sql.Tx) error {
		l, err := s.liveHeldLease(ctx, tx, leaseID, workerID, now)
		if err != nil {
			return err
		}
		end, err = s.endLease(ctx, tx, l, outcome, retryable, reason, stage, now)
		return err
	})
	return end, err
}

// ExpireLeases ends every live lease whose expiry has passed. Each task
// requeues (with reactivation) or, once it has failed or expired on
// MaxAttempts distinct workers, fails for good (see endLease). The lease's worker is never
// offered the task again.
func (s *Store) ExpireLeases(ctx context.Context) ([]LeaseEnd, error) {
	now := s.now()
	var out []LeaseEnd
	err := s.write(ctx, func(tx *sql.Tx) error {
		out = nil
		ls, err := liveLeases(ctx, tx, `l.expires_at <= ?`, toMS(now))
		if err != nil {
			return err
		}
		for _, l := range ls {
			if !l.TaskExists {
				continue // unreachable: deleting a job ends its leases
			}
			end, err := s.endLease(ctx, tx, l, api.OutcomeExpired, true, "lease expired", "", now)
			if err != nil {
				return err
			}
			if end != nil {
				out = append(out, *end)
			}
		}
		return nil
	})
	return out, err
}

// ExtendLiveLeases pushes the expiry of every live lease to at least now+by.
// The server calls it at startup, before the reaper, so workers that
// reconnect within one TTL carry on. It returns how many leases it touched.
func (s *Store) ExtendLiveLeases(ctx context.Context, by time.Duration) (int64, error) {
	var n int64
	err := s.write(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE leases SET expires_at = MAX(expires_at, ?) WHERE ended_at IS NULL`,
			toMS(s.now().Add(by)))
		if err != nil {
			return fmt.Errorf("extend leases: %w", err)
		}
		n, err = res.RowsAffected()
		return err
	})
	return n, err
}

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
)

// FetchSeq is the task sequence number of a fetch task (the download that
// precedes prepare). It is negative so that seq 0 stays the prepare task
// everywhere else, and so that a fetch task sorts before it.
const FetchSeq = -1

// fetchGrantOn is a condition that the worker whose principal is the SQL
// expression principal holds fetch on the queue whose id is the SQL
// expression queue ("?" for an argument, in that order).
func fetchGrantOn(principal, queue string) string {
	return `EXISTS (SELECT 1 FROM grants g WHERE g.principal = ` + principal + ` AND g.role = 'fetch'
	AND (g.scope = '*' OR g.scope = 'queue:' || ` + queue + `))`
}

// fetchGrantFor is fetchGrantOn the job's queue (over j = jobs).
func fetchGrantFor(principal string) string { return fetchGrantOn(principal, "j.queue_id") }

// fetchGrantSQL is fetchGrantFor with the principal as its one argument.
var fetchGrantSQL = fetchGrantFor("?")

// fetchBranchSQL is the fetch branch of the claim predicate (over t = tasks,
// j = jobs) for the downloader whose principal is the SQL expression
// principal: a fetch task on a queue it holds fetch on. Claim adds the
// pacing and failed-here exclusions; rotation (pacer.canServe) uses the
// same branch for other groups' downloaders.
func fetchBranchSQL(principal string) string {
	return "t.kind = 'fetch' AND " + fetchGrantFor(principal)
}

// QueueHasDownloader reports whether the queue has an eligible downloader:
// a downloader worker that is active or paused, whose sandbox level ranks at
// least minSandbox, and that holds fetch on the queue (or on "*").
func (s *Store) QueueHasDownloader(ctx context.Context, queueID int64, minSandbox api.SandboxLevel) (bool, error) {
	return queueHasDownloader(ctx, s.r, queueID, minSandbox)
}

func queueHasDownloader(ctx context.Context, q querier, queueID int64, minSandbox api.SandboxLevel) (bool, error) {
	rows, err := q.QueryContext(ctx, `SELECT w.sandbox_level FROM workers w
		WHERE w.kind = 'downloader' AND w.state IN ('active','paused')
		AND `+fetchGrantOn("'worker:' || w.id", "?"), queueID)
	if err != nil {
		return false, fmt.Errorf("queue has downloader: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var lvl api.SandboxLevel
		if err := rows.Scan(&lvl); err != nil {
			return false, fmt.Errorf("queue has downloader: %w", err)
		}
		if lvl.Rank() >= minSandbox.Rank() {
			return true, nil
		}
	}
	return false, rows.Err()
}

// OrphanFailures are the fetching jobs failed for want of an eligible
// downloader, and the leases ended by failing them (which can belong to any
// worker, e.g. a downloader below the sandbox floor).
type OrphanFailures struct{ Jobs, Leases []string }

// failOrphanedFetchJobs fails the fetching jobs of a queue that no eligible
// downloader (see QueueHasDownloader) can serve, and returns them with the
// leases it ended.
func (t *Tx) failOrphanedFetchJobs(ctx context.Context, queueID int64, minSandbox api.SandboxLevel) (out OrphanFailures, err error) {
	ok, err := queueHasDownloader(ctx, t.tx, queueID, minSandbox)
	if err != nil || ok {
		return out, err
	}
	var name string
	if err := t.tx.QueryRowContext(ctx, `SELECT name FROM queues WHERE id = ?`, queueID).Scan(&name); err != nil {
		return out, notFound(err, fmt.Sprintf("queue %d", queueID))
	}
	rows, err := t.tx.QueryContext(ctx, `SELECT id FROM jobs WHERE queue_id = ? AND state = 'fetching' ORDER BY id`, queueID)
	if err != nil {
		return out, fmt.Errorf("orphaned fetch jobs: %w", err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return out, fmt.Errorf("orphaned fetch jobs: %w", err)
		}
		out.Jobs = append(out.Jobs, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return out, err
	}
	rows.Close()
	now := t.s.now()
	for _, id := range out.Jobs {
		ended, err := failJob(ctx, t.tx, id, "no downloader holds fetch on queue "+name, now)
		if err != nil {
			return out, err
		}
		out.Leases = append(out.Leases, ended...)
	}
	return out, nil
}

// failAllOrphanedFetchJobs applies failOrphanedFetchJobs, at the store's
// FetchMinSandbox, to every queue with a fetching job.
func (t *Tx) failAllOrphanedFetchJobs(ctx context.Context) (out OrphanFailures, err error) {
	rows, err := t.tx.QueryContext(ctx, `SELECT DISTINCT queue_id FROM jobs WHERE state = 'fetching' ORDER BY queue_id`)
	if err != nil {
		return out, fmt.Errorf("orphaned fetch jobs: %w", err)
	}
	var qs []int64
	for rows.Next() {
		var q int64
		if err := rows.Scan(&q); err != nil {
			rows.Close()
			return out, fmt.Errorf("orphaned fetch jobs: %w", err)
		}
		qs = append(qs, q)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return out, err
	}
	rows.Close()
	for _, q := range qs {
		o, err := t.failOrphanedFetchJobs(ctx, q, t.s.cfg.FetchMinSandbox)
		if err != nil {
			return out, err
		}
		out.Jobs, out.Leases = append(out.Jobs, o.Jobs...), append(out.Leases, o.Leases...)
	}
	return out, nil
}

// ByteCap is the one rule for a media byte cap, an upload's or a fetch
// lease's: the queue's cap if it has one, never above the server's
// (--max-upload-bytes); 0 at either level means no cap there.
func ByteCap(queue, server int64) int64 {
	switch {
	case queue > 0 && server > 0:
		return min(queue, server)
	case queue > 0:
		return queue
	}
	return server
}

// FetchCompletion is the original media a fetch task downloaded, already
// stored by the server, with the server's ingest decision: Canonical (and
// its Ranges) when the original is itself canonical audio, as for an upload
// on the fast path; nil to have it prepared.
type FetchCompletion struct {
	Original  Media
	Canonical *Canonical
	Ranges    []SampleRange
}

// AcceptFetch atomically completes a fetch lease: the fetch task is done,
// the job's original is recorded, the automatic backoff of the lease's
// (egress key, site) is reset (see resetFetchBackoff), and the job either
// becomes queued with one transcribe task per range (Canonical set) or
// preparing with a seq-0 prepare task. Repeating it for a lease that
// already completed with the same original hash succeeds again; another
// hash is ErrConflict; otherwise a non-live lease is ErrLeaseGone.
func (s *Store) AcceptFetch(ctx context.Context, leaseID, workerID string, c FetchCompletion) error {
	if c.Canonical != nil {
		if err := validRanges(c.Ranges, c.Canonical.Samples); err != nil {
			return err
		}
	} else if len(c.Ranges) > 0 {
		return fmt.Errorf("%w: task ranges without canonical audio", ErrInvalid)
	}
	now := s.now()
	return s.write(ctx, func(tx *sql.Tx) error {
		l, err := heldLease(ctx, tx, leaseID, workerID)
		if err != nil {
			return err
		}
		if l.TaskExists && l.Kind != api.KindFetch {
			return fmt.Errorf("%w: lease %s is a %s lease", ErrInvalid, leaseID, l.Kind)
		}
		if l.TaskExists && l.Ended && l.Outcome == api.OutcomeDone {
			var sha string
			if err := tx.QueryRowContext(ctx, `SELECT original_sha256 FROM jobs WHERE id = ?`, l.JobID).Scan(&sha); err != nil {
				return fmt.Errorf("accept fetch: %w", err)
			}
			if sha == c.Original.SHA256 {
				return nil
			}
			return fmt.Errorf("lease %s already completed with different media: %w", leaseID, ErrConflict)
		}
		if err := s.checkLive(l, now); err != nil {
			return err
		}
		ms := toMS(now)
		state := api.JobPreparing
		var canonBytes, canonSamples sql.NullInt64
		var canonSHA sql.NullString
		if cn := c.Canonical; cn != nil {
			state = api.JobQueued
			canonBytes = sql.NullInt64{Int64: cn.Bytes, Valid: true}
			canonSamples = sql.NullInt64{Int64: cn.Samples, Valid: true}
			canonSHA = sql.NullString{String: cn.SHA256, Valid: true}
		}
		var model, site string
		err = tx.QueryRowContext(ctx, `UPDATE jobs SET state = ?, original_bytes = ?, original_sha256 = ?,
			original_type = ?, canonical_bytes = ?, canonical_sha256 = ?, audio_samples = ?, updated_at = ?
			WHERE id = ? AND state = 'fetching' RETURNING model, COALESCE(fetch_site, '')`,
			string(state), c.Original.Bytes, c.Original.SHA256, c.Original.Type, canonBytes, canonSHA, canonSamples,
			ms, l.JobID).Scan(&model, &site)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("accept fetch: job %s not fetching: %w", l.JobID, ErrConflict)
		}
		if err != nil {
			return fmt.Errorf("accept fetch: %w", err)
		}
		res, err := tx.ExecContext(ctx, `UPDATE tasks SET state = 'done', worker_id = ?
			WHERE job_id = ? AND seq = ? AND state = 'leased'`, workerID, l.JobID, l.Seq)
		if err := mustAffect(res, err, "complete fetch task"); err != nil {
			return err
		}
		if err := countCompletion(ctx, tx, l, 0, now); err != nil {
			return err
		}
		if err := finishLease(ctx, tx, l, now); err != nil {
			return err
		}
		var egress string
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(egress_key, '') FROM leases WHERE id = ?`,
			l.ID).Scan(&egress); err != nil {
			return fmt.Errorf("accept fetch: %w", err)
		}
		if err := resetFetchBackoff(ctx, tx, egress, site, now); err != nil {
			return err
		}
		if err := reactivate(ctx, tx, l.QueueID, l.Weight); err != nil {
			return err
		}
		if c.Canonical != nil {
			return s.insertTranscribeTasks(ctx, tx, l.JobID, model, c.Ranges)
		}
		return s.insertPrepareTask(ctx, tx, l.JobID)
	})
}

// resetFetchBackoff resets the automatic backoff of (egress, site) after a
// successful fetch: an expired automatic cooldown is removed, and an active
// one keeps its until (a fetch that started before the cooldown was set does
// not lift it) but drops to level 0, so the next rate limit starts again at
// the policy's initial backoff. A manual cooldown is left alone.
func resetFetchBackoff(ctx context.Context, tx *sql.Tx, egress, site string, now time.Time) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM fetch_cooldowns WHERE egress_key = ? AND site = ? AND manual = 0
		AND until <= ?`, egress, site, toMS(now)); err != nil {
		return fmt.Errorf("reset fetch backoff: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE fetch_cooldowns SET level = 0 WHERE egress_key = ? AND site = ?
		AND manual = 0`, egress, site); err != nil {
		return fmt.Errorf("reset fetch backoff: %w", err)
	}
	return nil
}

// FailAllOrphanedFetchJobs fails the fetching jobs of every queue without an
// eligible downloader (at the store's FetchMinSandbox) and returns them with
// the leases that ended. It is the reaper's backstop for floors raised or
// downloaders removed behind the store's back.
func (s *Store) FailAllOrphanedFetchJobs(ctx context.Context) (OrphanFailures, error) {
	return inTx1(ctx, s, func(tx *Tx) (OrphanFailures, error) { return tx.failAllOrphanedFetchJobs(ctx) })
}

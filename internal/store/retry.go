package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
)

// StrandedTask is a task FailStranded failed: it had failed on Workers, and
// no other eligible worker took it within the wait limit.
type StrandedTask struct {
	JobID   string
	Seq     int
	Kind    api.TaskKind
	Workers []string
	// Reason is the job error FailStranded recorded.
	Reason string
	// JobFailed is true if the job is now failed; false while sibling
	// leases of the failing job are still live (DESIGN.md §4.2).
	JobFailed bool
}

// FailStranded fails every pending task that failed on some worker and has
// been waiting for limit since it last went back to pending: every worker
// that took it since then failed it, and no other eligible worker has taken
// it (DESIGN.md §8.2). Each such task fails for good and makes its job
// failing, with a reason naming the workers that failed it. The reaper calls it; limit must be positive.
//
// It also fails a pending fetch task that has been rate limited for limit (since
// the task first was refused: the first rate_limited lease end of the job's
// current retry, so a task that merely waited its turn for pacing is not
// failed by one refusal): every downloader that tried the site was refused. Its job error is
// "rate limited by <site> on every downloader for <limit>".
func (s *Store) FailStranded(ctx context.Context, limit time.Duration) ([]StrandedTask, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("%w: retry wait limit must be positive", ErrInvalid)
	}
	now := s.now()
	cutoff := toMS(now.Add(-limit))
	var out []StrandedTask
	err := s.write(ctx, func(tx *sql.Tx) error {
		out = nil
		// A pending task with attempts > 0 has a failed or expired lease
		// (endLease sets attempts to the number of workers it failed on).
		// It went back to pending when its last lease ended, of whatever
		// outcome, so it has waited since then. The first subquery is
		// served by leases_task_failures and rules out most tasks before
		// the second, which looks at leases ended since the cutoff.
		//
		// The rate-limited branch needs no attempts: such a lease is
		// refunded.
		const rateLimitedSQL = `(t.kind = 'fetch' AND (SELECT MIN(r.ended_at) FROM leases r
			WHERE r.job_id = t.job_id AND r.seq = t.seq AND r.stage = 'rate_limited' AND r.retry = j.retries) <= ?)`
		rows, err := tx.QueryContext(ctx, `SELECT t.job_id, t.seq, t.kind, COALESCE(t.error, ''),
			COALESCE(j.fetch_site, ''), `+rateLimitedSQL+` FROM tasks t JOIN jobs j ON j.id = t.job_id
			WHERE t.state = 'pending' AND (`+rateLimitedSQL+` OR t.attempts > 0
			AND (SELECT MAX(x.ended_at) FROM leases x WHERE x.job_id = t.job_id AND x.seq = t.seq
				AND x.outcome IN ('failed','expired') AND x.stage IS NOT 'pre-work' AND x.stage IS NOT 'rate_limited'
				AND x.retry = j.retries) <= ?
			AND NOT EXISTS (SELECT 1 FROM leases y WHERE y.job_id = t.job_id AND y.seq = t.seq
				AND (y.ended_at IS NULL OR y.ended_at > ?)))
			ORDER BY t.job_id, t.seq`, cutoff, cutoff, cutoff, cutoff)
		if err != nil {
			return fmt.Errorf("stranded tasks: %w", err)
		}
		var errs, sites []string
		var rl []bool
		for rows.Next() {
			var st StrandedTask
			var e, site string
			var isRL bool
			if err := rows.Scan(&st.JobID, &st.Seq, &st.Kind, &e, &site, &isRL); err != nil {
				rows.Close()
				return fmt.Errorf("stranded tasks: %w", err)
			}
			out = append(out, st)
			errs, sites, rl = append(errs, e), append(sites, site), append(rl, isRL)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("stranded tasks: %w", err)
		}
		kept := out[:0]
		for i, st := range out {
			// An earlier task of the same job already failed it.
			if i > 0 && out[i-1].JobID == st.JobID {
				continue
			}
			if st.Workers, err = failedOn(ctx, tx, st.JobID, st.Seq); err != nil {
				return err
			}
			if rl[i] {
				st.Reason = "rate limited by " + sites[i] + " on every downloader for " + shortDuration(limit)
			} else {
				st.Reason = failedAttempts(st.Kind, st.Seq, st.Workers) + "; no other eligible worker took it within " +
					shortDuration(limit)
				if errs[i] != "" {
					st.Reason += ": " + errs[i]
				}
			}
			if _, err := tx.ExecContext(ctx, `UPDATE tasks SET state = 'failed' WHERE job_id = ? AND seq = ?`,
				st.JobID, st.Seq); err != nil {
				return fmt.Errorf("fail stranded task: %w", err)
			}
			if err := doomJob(ctx, tx, st.JobID, st.Reason, now); err != nil {
				return err
			}
			if st.JobFailed, err = settleJob(ctx, tx, st.JobID, now); err != nil {
				return err
			}
			kept = append(kept, st)
		}
		out = kept
		return nil
	})
	return out, err
}

// shortDuration formats d without trailing zero units: 24h, 90m, 1m30s.
func shortDuration(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = s[:len(s)-2]
	}
	if strings.HasSuffix(s, "h0m") {
		s = s[:len(s)-2]
	}
	return s
}

// RetryExclusion says why the retry-elsewhere rule keeps a task from a
// worker (DESIGN.md §8.2), or returns "" if it does not: the worker failed
// the task, or let a lease on it expire, and so is never offered it again.
func (s *Store) RetryExclusion(ctx context.Context, jobID string, seq int, workerID string) (string, error) {
	var outcome api.LeaseOutcome
	err := s.r.QueryRowContext(ctx, `SELECT outcome FROM leases WHERE job_id = ? AND seq = ? AND worker_id = ?
		AND `+countedFailureSQL+` ORDER BY ended_at DESC, rowid DESC LIMIT 1`,
		jobID, seq, workerID).Scan(&outcome)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", nil
	case err != nil:
		return "", fmt.Errorf("retry exclusion: %w", err)
	case outcome == api.OutcomeExpired:
		return "a lease on this task expired on worker " + workerID, nil
	default:
		return "worker " + workerID + " failed this task", nil
	}
}

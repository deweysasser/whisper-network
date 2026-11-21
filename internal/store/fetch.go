package store

import (
	"context"
	"fmt"

	"github.com/deweysasser/whisper-network/internal/api"
)

// FetchSeq is the task sequence number of a fetch task (the download that
// precedes prepare). It is negative so that seq 0 stays the prepare task
// everywhere else, and so that a fetch task sorts before it.
const FetchSeq = -1

// fetchGrantSQL is a condition (over j = jobs, with the worker principal as
// its one argument) that the worker holds fetch on the job's queue.
const fetchGrantSQL = `EXISTS (SELECT 1 FROM grants g WHERE g.principal = ? AND g.role = 'fetch'
	AND (g.scope = '*' OR g.scope = 'queue:' || j.queue_id))`

// QueueHasDownloader reports whether the queue has an eligible downloader:
// a downloader worker that is active or paused, whose sandbox level ranks at
// least minSandbox, and that holds fetch on the queue (or on "*").
func (s *Store) QueueHasDownloader(ctx context.Context, queueID int64, minSandbox api.SandboxLevel) (bool, error) {
	return queueHasDownloader(ctx, s.r, queueID, minSandbox)
}

func queueHasDownloader(ctx context.Context, q querier, queueID int64, minSandbox api.SandboxLevel) (bool, error) {
	rows, err := q.QueryContext(ctx, `SELECT w.sandbox_level FROM workers w
		WHERE w.kind = 'downloader' AND w.state IN ('active','paused')
		AND EXISTS (SELECT 1 FROM grants g WHERE g.principal = 'worker:' || w.id AND g.role = 'fetch'
			AND (g.scope = '*' OR g.scope = ?))`, QueueScope(queueID))
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

// FailOrphanedFetchJobs fails the fetching jobs of a queue that no eligible
// downloader (see QueueHasDownloader) can serve, and returns their ids.
func (t *Tx) FailOrphanedFetchJobs(ctx context.Context, queueID int64, minSandbox api.SandboxLevel) ([]string, error) {
	jobs, _, err := t.failOrphanedFetchJobs(ctx, queueID, minSandbox)
	return jobs, err
}

// failOrphanedFetchJobs is FailOrphanedFetchJobs that also returns the ids
// of the leases it cancelled.
func (t *Tx) failOrphanedFetchJobs(ctx context.Context, queueID int64, minSandbox api.SandboxLevel) (jobs, leases []string, err error) {
	ok, err := queueHasDownloader(ctx, t.tx, queueID, minSandbox)
	if err != nil || ok {
		return nil, nil, err
	}
	var name string
	if err := t.tx.QueryRowContext(ctx, `SELECT name FROM queues WHERE id = ?`, queueID).Scan(&name); err != nil {
		return nil, nil, notFound(err, fmt.Sprintf("queue %d", queueID))
	}
	rows, err := t.tx.QueryContext(ctx, `SELECT id FROM jobs WHERE queue_id = ? AND state = 'fetching' ORDER BY id`, queueID)
	if err != nil {
		return nil, nil, fmt.Errorf("orphaned fetch jobs: %w", err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, nil, fmt.Errorf("orphaned fetch jobs: %w", err)
		}
		jobs = append(jobs, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, nil, err
	}
	rows.Close()
	now := t.s.now()
	for _, id := range jobs {
		ended, err := failJob(ctx, t.tx, id, "no downloader holds fetch on queue "+name, now)
		if err != nil {
			return nil, nil, err
		}
		leases = append(leases, ended...)
	}
	return jobs, leases, nil
}

// failAllOrphanedFetchJobs applies failOrphanedFetchJobs, at the store's
// FetchMinSandbox, to every queue with a fetching job.
func (t *Tx) failAllOrphanedFetchJobs(ctx context.Context) (jobs, leases []string, err error) {
	rows, err := t.tx.QueryContext(ctx, `SELECT DISTINCT queue_id FROM jobs WHERE state = 'fetching' ORDER BY queue_id`)
	if err != nil {
		return nil, nil, fmt.Errorf("orphaned fetch jobs: %w", err)
	}
	var qs []int64
	for rows.Next() {
		var q int64
		if err := rows.Scan(&q); err != nil {
			rows.Close()
			return nil, nil, fmt.Errorf("orphaned fetch jobs: %w", err)
		}
		qs = append(qs, q)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, nil, err
	}
	rows.Close()
	for _, q := range qs {
		j, l, err := t.failOrphanedFetchJobs(ctx, q, t.s.cfg.FetchMinSandbox)
		if err != nil {
			return nil, nil, err
		}
		jobs, leases = append(jobs, j...), append(leases, l...)
	}
	return jobs, leases, nil
}

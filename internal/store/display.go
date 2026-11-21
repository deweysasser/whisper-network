package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
)

// Lookups for the web UI's labels (DESIGN.md §6.3): each resolves every id
// on a page in one query, so a page costs one query per kind of entity
// whatever its length. Unknown ids are absent from the result (a pruned
// worker, a deleted job).

// idArgs converts ids to query arguments, dropping duplicates and empties.
func idArgs(ids []string) []any {
	seen := make(map[string]bool, len(ids))
	out := make([]any, 0, len(ids))
	for _, id := range ids {
		if id != "" && !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}

// queryIDs runs query (with one "%s" for the id placeholders) over ids and
// calls scan for each row.
func (s *Store) queryIDs(ctx context.Context, what, query string, ids []string, scan func(*sql.Rows) error) error {
	args := idArgs(ids)
	if len(args) == 0 {
		return nil
	}
	rows, err := s.r.QueryContext(ctx, fmt.Sprintf(query, placeholders(len(args))), args...)
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := scan(rows); err != nil {
			return fmt.Errorf("%s: %w", what, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	return nil
}

// WorkerRefs returns the display names of the given workers.
func (s *Store) WorkerRefs(ctx context.Context, ids []string) (map[string]WorkerRef, error) {
	out := map[string]WorkerRef{}
	err := s.queryIDs(ctx, "worker refs", `SELECT id, hostname, description FROM workers WHERE id IN (%s)`, ids,
		func(r *sql.Rows) error {
			var w WorkerRef
			if err := r.Scan(&w.ID, &w.Hostname, &w.Description); err != nil {
				return err
			}
			out[w.ID] = w
			return nil
		})
	return out, err
}

// JobRef names a job for display, with what visibility checks need.
type JobRef struct {
	ID, Label, Source string
	QueueName         string
	ClientID          string // the owner
}

// JobRefs returns the labels of the given jobs.
func (s *Store) JobRefs(ctx context.Context, ids []string) (map[string]JobRef, error) {
	out := map[string]JobRef{}
	err := s.queryIDs(ctx, "job refs", `SELECT j.id, j.label, j.source, q.name, j.client_id FROM jobs j
		JOIN queues q ON q.id = j.queue_id WHERE j.id IN (%s)`, ids, func(r *sql.Rows) error {
		var j JobRef
		if err := r.Scan(&j.ID, &j.Label, &j.Source, &j.QueueName, &j.ClientID); err != nil {
			return err
		}
		out[j.ID] = j
		return nil
	})
	return out, err
}

// ClientRef names an API client for display.
type ClientRef struct {
	ID, Name string
	Revoked  bool
}

// ClientRefs returns the names of the given clients, revoked or not.
func (s *Store) ClientRefs(ctx context.Context, ids []string) (map[string]ClientRef, error) {
	out := map[string]ClientRef{}
	err := s.queryIDs(ctx, "client refs", `SELECT id, name, revoked_at IS NOT NULL FROM clients WHERE id IN (%s)`, ids,
		func(r *sql.Rows) error {
			var c ClientRef
			if err := r.Scan(&c.ID, &c.Name, &c.Revoked); err != nil {
				return err
			}
			out[c.ID] = c
			return nil
		})
	return out, err
}

// pendingSinceSQL is when a task (t, of job j) last became pending; tasks
// from before tasks.pending_since existed fall back to the job's creation.
const pendingSinceSQL = `COALESCE(t.pending_since, j.created_at)`

// PendingGroup is a queue's pending tasks of one kind, model and weight
// class.
type PendingGroup struct {
	Kind   api.TaskKind
	Model  string
	Weight api.Weight
	Tasks  int
	// AudioSamples sums the canonical audio of the transcribe tasks.
	AudioSamples int64
	// OldestSince is when the longest-waiting task became pending.
	OldestSince time.Time
}

// QueuePending summarizes a queue's pending tasks by kind, model and weight
// class, in one aggregate query.
func (s *Store) QueuePending(ctx context.Context, queueID int64) ([]PendingGroup, error) {
	var out []PendingGroup
	err := eachRow(ctx, s.r, `SELECT t.kind, j.model, j.weight, COUNT(*),
		COALESCE(SUM(t.end_sample - t.start_sample), 0), MIN(`+pendingSinceSQL+`)
		FROM tasks t JOIN jobs j ON j.id = t.job_id
		WHERE j.queue_id = ? AND t.state = 'pending' GROUP BY 1, 2, 3 ORDER BY 1, 2, 3`, []any{queueID},
		func(r *sql.Rows) error {
			var g PendingGroup
			var since int64
			if err := r.Scan(&g.Kind, &g.Model, &g.Weight, &g.Tasks, &g.AudioSamples, &since); err != nil {
				return err
			}
			g.OldestSince = fromMS(since)
			out = append(out, g)
			return nil
		})
	return out, err
}

// JobsPendingSince returns, for those of the given jobs with a pending
// task, when their longest-waiting pending task became pending.
func (s *Store) JobsPendingSince(ctx context.Context, jobIDs []string) (map[string]time.Time, error) {
	out := map[string]time.Time{}
	err := s.queryIDs(ctx, "jobs pending since", `SELECT t.job_id, MIN(`+pendingSinceSQL+`) FROM tasks t
		JOIN jobs j ON j.id = t.job_id WHERE t.job_id IN (%s) AND t.state = 'pending' GROUP BY t.job_id`, jobIDs,
		func(r *sql.Rows) error {
			var id string
			var since int64
			if err := r.Scan(&id, &since); err != nil {
				return err
			}
			out[id] = fromMS(since)
			return nil
		})
	return out, err
}

// TaskInfo is one task of a job, in any state, for the job page.
type TaskInfo struct {
	Seq                    int
	Kind                   api.TaskKind
	State                  api.TaskState // may also be "cancelled"
	Attempts               int
	StartSample, EndSample int64
	// Error is the task's last failure reason, empty if none.
	Error string
	// WorkerID is the worker that holds or completed the task, or last
	// held it; empty if none.
	WorkerID string
	// PendingSince is when a pending task became pending.
	PendingSince *time.Time
	// LastFailedOn is the worker of the task's latest counted failure
	// (failed or expired) since the job was last retried, empty if none.
	LastFailedOn string
	// FailedOn are the distinct workers of those failures: the workers the
	// retry-elsewhere rule keeps the task from (DESIGN.md §8.2).
	FailedOn []string
}

// JobTasks returns every task of a job in order, in two indexed queries.
func (s *Store) JobTasks(ctx context.Context, jobID string) ([]TaskInfo, error) {
	var out []TaskInfo
	index := map[int]*TaskInfo{}
	err := eachRow(ctx, s.r, `SELECT t.seq, t.kind, t.state, t.attempts, COALESCE(t.start_sample, 0),
		COALESCE(t.end_sample, 0), COALESCE(t.error, ''), COALESCE(t.worker_id, ''),
		CASE WHEN t.state = 'pending' THEN `+pendingSinceSQL+` END
		FROM tasks t JOIN jobs j ON j.id = t.job_id WHERE t.job_id = ? ORDER BY t.seq`, []any{jobID},
		func(r *sql.Rows) error {
			var t TaskInfo
			var since sql.NullInt64
			if err := r.Scan(&t.Seq, &t.Kind, &t.State, &t.Attempts, &t.StartSample, &t.EndSample, &t.Error,
				&t.WorkerID, &since); err != nil {
				return err
			}
			t.PendingSince = optTime(since)
			out = append(out, t)
			return nil
		})
	if err != nil {
		return nil, err
	}
	for i := range out {
		index[out[i].Seq] = &out[i]
	}
	// Served by leases_task_failures; ordered so the last row per task is
	// its latest failure.
	err = eachRow(ctx, s.r, `SELECT seq, worker_id, MAX(ended_at) FROM leases
		WHERE job_id = ? AND `+countedFailureSQL+` GROUP BY seq, worker_id ORDER BY seq, MAX(ended_at), MIN(rowid)`,
		[]any{jobID}, func(r *sql.Rows) error {
			var seq int
			var w string
			var ended sql.NullInt64
			if err := r.Scan(&seq, &w, &ended); err != nil {
				return err
			}
			if t := index[seq]; t != nil {
				t.FailedOn = append(t.FailedOn, w)
				t.LastFailedOn = w
			}
			return nil
		})
	return out, err
}

// ProcessWorkers returns the active workers seen within seenWithin that
// hold process on the queue (or on "*"): the workers that could take its
// tasks.
func (s *Store) ProcessWorkers(ctx context.Context, queueID int64, seenWithin time.Duration) ([]Worker, error) {
	rows, err := s.r.QueryContext(ctx, workerSelect+` WHERE w.state = 'active' AND w.last_seen_at >= ?
		AND EXISTS (SELECT 1 FROM grants g WHERE g.principal = 'worker:' || w.id AND g.role = 'process'
			AND (g.scope = '*' OR g.scope = ?)) ORDER BY w.registered_at, w.id`,
		toMS(s.now().Add(-seenWithin)), QueueScope(queueID))
	if err != nil {
		return nil, fmt.Errorf("process workers: %w", err)
	}
	defer rows.Close()
	var out []Worker
	for rows.Next() {
		w, err := scanWorker(rows)
		if err != nil {
			return nil, fmt.Errorf("process workers: %w", err)
		}
		out = append(out, *w)
	}
	return out, rows.Err()
}

// PrepareMinSandbox is the lowest sandbox level a worker needs to be
// offered prepare tasks.
func (s *Store) PrepareMinSandbox() api.SandboxLevel { return s.cfg.PrepareMinSandbox }

// FetchMinSandbox is the lowest sandbox level a downloader needs to be
// offered fetch tasks and to count as a queue's downloader
// (Config.FetchMinSandbox).
func (s *Store) FetchMinSandbox() api.SandboxLevel { return s.cfg.FetchMinSandbox }

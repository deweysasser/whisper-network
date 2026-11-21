package store

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
)

// clock is a settable test clock.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// counterIDs generates readable, ordered, unique ids.
func counterIDs() func(IDKind) string {
	var n atomic.Int64
	return func(k IDKind) string {
		prefix := map[IDKind]string{IDJob: "job", IDWorker: "wrk", IDLease: LeasePrefix, IDClient: ClientPrefix}[k]
		return fmt.Sprintf("%s%06d", prefix, n.Add(1))
	}
}

const ttl = 90 * time.Second

type fixture struct {
	t     *testing.T
	ctx   context.Context
	s     *Store
	clk   *clock
	path  string
	tok   atomic.Int64
	pubID int64
}

func newFixture(t *testing.T, mods ...func(*Config)) *fixture {
	t.Helper()
	f := &fixture{t: t, ctx: t.Context(), clk: &clock{t: time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)},
		path: filepath.Join(t.TempDir(), "wn.db")}
	f.s = f.open(mods...)
	_, err := f.s.Bootstrap(f.ctx, "hash")
	require.NoError(t, err)
	q, err := f.s.QueueByName(f.ctx, PublicQueue)
	require.NoError(t, err)
	f.pubID = q.ID
	return f
}

func (f *fixture) open(mods ...func(*Config)) *Store {
	cfg := Config{Path: f.path, Now: f.clk.Now, NewID: counterIDs()}
	for _, m := range mods {
		m(&cfg)
	}
	s, err := Open(f.ctx, cfg)
	require.NoError(f.t, err)
	f.t.Cleanup(func() { _ = s.Close() })
	return s
}

func (f *fixture) token() string { return fmt.Sprintf("tok-%d", f.tok.Add(1)) }

// queue creates a queue with the given weight.
func (f *fixture) queue(name string, weight int) int64 {
	q, err := f.s.CreateQueue(f.ctx, NewQueue{Name: name, Weight: weight})
	require.NoError(f.t, err)
	return q.ID
}

// worker registers an active worker with process on the given queues.
func (f *fixture) worker(sandbox api.SandboxLevel, queues ...int64) string {
	w, err := f.s.RegisterWorker(f.ctx, f.token(), api.WorkerFacts{Hostname: "h", SandboxLevel: sandbox,
		Kinds: []api.TaskKind{api.KindPrepare, api.KindTranscribe}, Models: []string{"small.en"}})
	require.NoError(f.t, err)
	_, err = f.s.SetWorkerState(f.ctx, w.ID, api.WorkerActive, "user:1")
	require.NoError(f.t, err)
	for _, q := range queues {
		require.NoError(f.t, f.s.AddGrant(f.ctx, w.Principal(), api.RoleProcess, QueueScope(q), "user:1"))
	}
	return w.ID
}

// eligibility is what a full-sandbox small.en slot offers (on the queues
// the worker holds process on, which Claim reads itself).
func (f *fixture) eligibility(string) Eligibility {
	return Eligibility{Kinds: []api.TaskKind{api.KindPrepare, api.KindTranscribe},
		Models: []string{"small.en"}, SandboxLevel: api.SandboxFull}
}

// job creates a canonical (fast-path) job with n transcribe tasks of
// secondsPerTask audio each.
func (f *fixture) job(queueID int64, client string, weight api.Weight, n int, secondsPerTask int64) *Job {
	per := secondsPerTask * canonicalSampleRate
	ranges := make([]SampleRange, n)
	for i := range ranges {
		ranges[i] = SampleRange{Start: int64(i) * per, End: int64(i+1) * per}
	}
	j, created, err := f.s.CreateJob(f.ctx, NewJob{QueueID: queueID, ClientID: client,
		Params:    api.JobParams{Model: "small.en", Weight: weight},
		Original:  Media{Bytes: 100, SHA256: "o", Type: "audio/flac"},
		Canonical: &Canonical{Bytes: 100, SHA256: "c", Samples: int64(n) * per},
		Ranges:    ranges})
	require.NoError(f.t, err)
	require.True(f.t, created)
	return j
}

// prepJob creates a job that needs a prepare task.
func (f *fixture) prepJob(queueID int64, client string) *Job {
	j, _, err := f.s.CreateJob(f.ctx, NewJob{QueueID: queueID, ClientID: client,
		Params:   api.JobParams{Model: "small.en"},
		Original: Media{Bytes: 1000, SHA256: "orig", Type: "video/mp4"}})
	require.NoError(f.t, err)
	return j
}

func (f *fixture) claim(workerID string) *Lease {
	f.t.Helper()
	l, err := f.s.Claim(f.ctx, workerID, f.eligibility(workerID), ttl)
	require.NoError(f.t, err)
	return l
}

func (f *fixture) complete(l *Lease) bool {
	f.t.Helper()
	done, err := f.s.CompleteTranscribe(f.ctx, l.ID, l.WorkerID, TranscribeCompletion{
		Result: []byte(`{"segments":[]}`), EngineVersion: "whisper.cpp 1.9.5", ComputeMS: 1000})
	require.NoError(f.t, err)
	return done.JobComplete
}

// task returns (state, attempts) of a task.
func (f *fixture) task(jobID string, seq int) (string, int) {
	f.t.Helper()
	var state string
	var attempts int
	require.NoError(f.t, f.s.r.QueryRowContext(f.ctx,
		`SELECT state, attempts FROM tasks WHERE job_id = ? AND seq = ?`, jobID, seq).Scan(&state, &attempts))
	return state, attempts
}

// downloader registers an active downloader worker with fetch on the given
// queues, from the given egress key.
func (f *fixture) downloader(sandbox api.SandboxLevel, egress string, queues ...int64) string {
	w, err := f.s.RegisterWorker(f.ctx, f.token(), api.WorkerFacts{Hostname: "d", SandboxLevel: sandbox,
		Kind: api.WorkerDownloader, Kinds: []api.TaskKind{api.KindFetch}})
	require.NoError(f.t, err)
	_, err = f.s.SetWorkerState(f.ctx, w.ID, api.WorkerActive, "user:1")
	require.NoError(f.t, err)
	require.NoError(f.t, f.s.SetWorkerEgressKey(f.ctx, w.ID, egress))
	for _, q := range queues {
		require.NoError(f.t, f.s.AddGrant(f.ctx, w.Principal(), api.RoleFetch, QueueScope(q), "user:1"))
	}
	return w.ID
}

// fetchingJob inserts a fetching job with its pending fetch task on queueID.
func (f *fixture) fetchingJob(queueID int64, id string) {
	f.t.Helper()
	_, err := f.s.w.ExecContext(f.ctx, `INSERT INTO jobs (id, queue_id, client_id, state, model, language, created_at,
		updated_at, fetch_url, fetch_site) VALUES (?, ?, 'c', 'fetching', 'm', 'en', 1, 1, 'https://x/y', 'x')`, id, queueID)
	require.NoError(f.t, err)
	_, err = f.s.w.ExecContext(f.ctx, `INSERT INTO tasks (job_id, seq, kind, cost, state) VALUES (?, ?, 'fetch', 1, 'pending')`,
		id, FetchSeq)
	require.NoError(f.t, err)
}

// fetchLease leases a fetching job's fetch task to a worker.
func (f *fixture) fetchLease(jobID, workerID string) string {
	f.t.Helper()
	id := "l-" + jobID + "-" + workerID
	_, err := f.s.w.ExecContext(f.ctx, `INSERT INTO leases (id, job_id, seq, worker_id, created_at, expires_at, egress_key)
		VALUES (?, ?, ?, ?, ?, ?, 'e1')`, id, jobID, FetchSeq, workerID, toMS(f.clk.Now()), toMS(f.clk.Now().Add(time.Hour)))
	require.NoError(f.t, err)
	_, err = f.s.w.ExecContext(f.ctx, `UPDATE tasks SET state = 'leased' WHERE job_id = ? AND seq = ?`, jobID, FetchSeq)
	require.NoError(f.t, err)
	return id
}

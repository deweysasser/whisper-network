package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
)

// stats returns a worker's counters as the store reports them.
func (f *fixture) stats(workerID string) api.WorkerStats {
	f.t.Helper()
	w, err := f.s.WorkerByID(f.ctx, workerID)
	require.NoError(f.t, err)
	return w.Stats
}

func ptime(t time.Time) *time.Time { return &t }

func TestWorkerStatsStartEmpty(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	w := f.worker(api.SandboxFull, f.pubID)
	assert.Equal(t, api.WorkerStats{}, f.stats(w))
	ws, err := f.s.ListWorkers(f.ctx)
	require.NoError(t, err)
	require.Len(t, ws, 1)
	assert.Equal(t, api.WorkerStats{}, ws[0].Stats)
}

func TestWorkerStatsCompleteTranscribe(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	w := f.worker(api.SandboxFull, f.pubID)
	first := f.clk.Now()
	f.job(f.pubID, "c", api.WeightMedium, 2, 10)
	l1 := f.claim(w)
	f.complete(l1)
	f.clk.Advance(time.Minute)
	f.complete(f.claim(w))
	// Repeating a completion succeeds without counting again.
	f.complete(l1)

	f.clk.Advance(time.Minute)
	f.job(f.pubID, "c", api.WeightMedium, 1, 30)
	f.complete(f.claim(w))

	assert.Equal(t, api.WorkerStats{TranscribeTasks: 3, Jobs: 2, AudioS: 50, ComputeS: 3, TranscribeComputeS: 3,
		FirstTaskAt: ptime(first), LastTaskAt: ptime(f.clk.Now())}, f.stats(w))
}

func TestWorkerStatsAcceptPrepare(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	w := f.worker(api.SandboxFull, f.pubID)
	f.prepJob(f.pubID, "c")
	l := f.claim(w)
	require.Equal(t, api.KindPrepare, l.Kind)
	samples := int64(20 * canonicalSampleRate)
	p := PrepareCompletion{Canonical: Canonical{Bytes: 10, SHA256: "c", Samples: samples},
		Ranges: []SampleRange{{0, samples}}, ComputeMS: 500}
	require.NoError(t, f.s.AcceptPrepare(f.ctx, l.ID, w, p))
	require.NoError(t, f.s.AcceptPrepare(f.ctx, l.ID, w, p), "repeat")
	at := f.clk.Now()
	assert.Equal(t, api.WorkerStats{PrepareTasks: 1, Jobs: 1, ComputeS: 0.5, FirstTaskAt: &at, LastTaskAt: &at},
		f.stats(w), "prepare audio is not counted: it would skew the realtime factor")

	// The job's transcribe task is the same job.
	f.complete(f.claim(w))
	got := f.stats(w)
	assert.EqualValues(t, 1, got.Jobs)
	assert.EqualValues(t, 1, got.PrepareTasks)
	assert.EqualValues(t, 1, got.TranscribeTasks)
	assert.InDelta(t, 20, got.AudioS, 0.001)
	assert.InDelta(t, 1.5, got.ComputeS, 0.001)
	assert.InDelta(t, 1, got.TranscribeComputeS, 0.001)
	assert.InDelta(t, 20, got.RealtimeFactor(), 0.001)
}

func TestWorkerStatsDistinctJobsPerWorker(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	a := f.worker(api.SandboxFull, f.pubID)
	b := f.worker(api.SandboxFull, f.pubID)
	f.job(f.pubID, "c", api.WeightMedium, 3, 10)
	la, lb := f.claim(a), f.claim(b)
	f.complete(la)
	f.complete(lb)
	f.complete(f.claim(a))
	assert.EqualValues(t, 1, f.stats(a).Jobs, "two tasks of one job")
	assert.EqualValues(t, 2, f.stats(a).TranscribeTasks)
	assert.EqualValues(t, 1, f.stats(b).Jobs, "the other worker contributed to it too")
}

func TestWorkerStatsFailures(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	w := f.worker(api.SandboxFull, f.pubID)
	f.job(f.pubID, "c", api.WeightMedium, 1, 10)
	f.job(f.pubID, "c", api.WeightMedium, 1, 10) // w never retries the first
	_, err := f.s.Fail(f.ctx, f.claim(w).ID, w, "whisper crashed", true)
	require.NoError(t, err)
	_, err = f.s.Release(f.ctx, f.claim(w).ID, w)
	require.NoError(t, err)
	f.claim(w)
	f.clk.Advance(ttl)
	ends, err := f.s.ExpireLeases(f.ctx)
	require.NoError(t, err)
	require.Len(t, ends, 1)

	got := f.stats(w)
	assert.Equal(t, api.WorkerStats{Failures: 1, ExpiredLeases: 1}, got, "a release counts as nothing")
	assert.InDelta(t, 1, got.FailureRate(), 0.001)
	assert.Zero(t, got.RealtimeFactor())
}

func TestWorkerStatsCancelledNotCounted(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	w := f.worker(api.SandboxFull, f.pubID)
	f.job(f.pubID, "c", api.WeightMedium, 1, 10)
	f.claim(w)
	cancelled, err := f.s.SetWorkerState(f.ctx, w, api.WorkerRejected, "user:1")
	require.NoError(t, err)
	require.Len(t, cancelled, 1)
	assert.Equal(t, api.WorkerStats{}, f.stats(w))
}

func TestWorkerStatsFailureRate(t *testing.T) {
	t.Parallel()
	s := api.WorkerStats{PrepareTasks: 1, TranscribeTasks: 2, Failures: 1}
	assert.InDelta(t, 0.25, s.FailureRate(), 1e-9)
	assert.Zero(t, (&api.WorkerStats{}).FailureRate())
}

func TestPruneRejectedWorkerReturnsAndDeletesStats(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	w := f.worker(api.SandboxFull, f.pubID)
	f.job(f.pubID, "c", api.WeightMedium, 1, 10)
	f.complete(f.claim(w))
	_, err := f.s.SetWorkerState(f.ctx, w, api.WorkerRejected, "user:1")
	require.NoError(t, err)
	want := f.stats(w)
	require.EqualValues(t, 1, want.TranscribeTasks)
	f.clk.Advance(31 * 24 * time.Hour)

	var pruned []PrunedWorker
	require.NoError(t, f.s.InTx(f.ctx, func(tx *Tx) error {
		pruned, err = tx.DeleteRejectedWorkers(f.ctx, f.clk.Now().Add(-30*24*time.Hour))
		return err
	}))
	assert.Equal(t, []PrunedWorker{{ID: w, Stats: want}}, pruned)
	var rows int
	require.NoError(t, f.s.r.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM worker_stats WHERE worker_id = ?`, w).Scan(&rows))
	assert.Zero(t, rows, "counters go with the worker")
}

// TestDeleteRejectedWorker: deleting one rejected worker on demand takes
// its grants, the grants scoped to it and its counters, and only it; a
// worker in any other state, or an unknown one, is not found.
func TestDeleteRejectedWorker(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	w := f.worker(api.SandboxFull, f.pubID)
	other := f.worker(api.SandboxFull, f.pubID)
	active := f.worker(api.SandboxFull, f.pubID)
	f.job(f.pubID, "c", api.WeightMedium, 1, 10)
	f.complete(f.claim(w))
	require.NoError(t, f.s.AddGrant(f.ctx, "user:1", api.RoleWorkerAdmin, WorkerPrincipal(w), "user:1"))
	for _, id := range []string{w, other} {
		_, err := f.s.SetWorkerState(f.ctx, id, api.WorkerRejected, "user:1")
		require.NoError(t, err)
	}
	want := f.stats(w)
	require.EqualValues(t, 1, want.TranscribeTasks)

	var got *PrunedWorker
	require.NoError(t, f.s.InTx(f.ctx, func(tx *Tx) error {
		var err error
		got, err = tx.DeleteRejectedWorker(f.ctx, w)
		return err
	}))
	assert.Equal(t, &PrunedWorker{ID: w, Stats: want}, got)
	_, err := f.s.WorkerByID(f.ctx, w)
	require.ErrorIs(t, err, ErrNotFound)
	var n int
	require.NoError(t, f.s.r.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM grants WHERE principal = ? OR scope = ?`,
		WorkerPrincipal(w), WorkerPrincipal(w)).Scan(&n))
	assert.Zero(t, n, "its grants and the grants over it go with it")
	require.NoError(t, f.s.r.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM worker_stats WHERE worker_id = ?`, w).Scan(&n))
	assert.Zero(t, n, "counters go with the worker")
	_, err = f.s.WorkerByID(f.ctx, other)
	require.NoError(t, err, "another rejected worker stays")

	for _, id := range []string{active, "no-such-worker"} {
		err := f.s.InTx(f.ctx, func(tx *Tx) error {
			_, err := tx.DeleteRejectedWorker(f.ctx, id)
			return err
		})
		require.ErrorIs(t, err, ErrNotFound, id)
	}
	_, err = f.s.WorkerByID(f.ctx, active)
	require.NoError(t, err, "a worker that is not rejected stays")
}

// rollBackToV2 makes the database look as it did at schema version 2
// (undoing every later migration: 0003 worker_stats, 0004's index, 0006's
// heartbeat_at column, 0007's failure streak table and pause_reason
// column, 0008's leases.stage, 0009's retry columns and index, 0010's
// tasks.pending_since, 0011's throughput table; 0005 read_all only adds grants and re-runs safely),
// then reopens it so migrations 3 onward, including the backfill, run again.
func (f *fixture) rollBackToV2() {
	f.t.Helper()
	_, err := f.s.w.ExecContext(f.ctx, undo0012+`
		DROP TABLE worker_stats;
		DROP INDEX IF EXISTS leases_task_failures;
		ALTER TABLE leases DROP COLUMN heartbeat_at;
		ALTER TABLE leases DROP COLUMN stage;
		DROP TABLE worker_failure_streak;
		ALTER TABLE workers DROP COLUMN pause_reason;
		DROP INDEX jobs_queue_state_order;
		ALTER TABLE jobs DROP COLUMN retries;
		ALTER TABLE leases DROP COLUMN retry;
		ALTER TABLE tasks DROP COLUMN pending_since;
		DROP TABLE throughput;
		PRAGMA user_version = 2`)
	require.NoError(f.t, err)
	require.NoError(f.t, f.s.Close())
	f.s = f.open()
}

func TestWorkerStatsBackfill(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	a := f.worker(api.SandboxFull, f.pubID)
	b := f.worker(api.SandboxFull, f.pubID)
	idle := f.worker(api.SandboxFull, f.pubID)
	c := f.worker(api.SandboxFull, f.pubID) // takes the retries

	// a: a prepare and both tasks of its job, one failure.
	f.prepJob(f.pubID, "c")
	l := f.claim(a)
	samples := int64(20 * canonicalSampleRate)
	require.NoError(t, f.s.AcceptPrepare(f.ctx, l.ID, a, PrepareCompletion{
		Canonical: Canonical{Bytes: 10, SHA256: "c", Samples: samples},
		Ranges:    []SampleRange{{0, samples / 2}, {samples / 2, samples}}, ComputeMS: 250}))
	f.clk.Advance(time.Minute)
	f.complete(f.claim(a))
	f.clk.Advance(time.Minute)
	f.complete(f.claim(a))
	f.job(f.pubID, "c", api.WeightMedium, 1, 7)
	_, err := f.s.Fail(f.ctx, f.claim(a).ID, a, "crashed", true)
	require.NoError(t, err)
	f.clk.Advance(time.Minute)
	f.complete(f.claim(c))
	// b: one expiry; c: one release, then both retries.
	f.job(f.pubID, "c", api.WeightMedium, 1, 5)
	f.claim(b)
	f.clk.Advance(ttl)
	_, err = f.s.ExpireLeases(f.ctx)
	require.NoError(t, err)
	_, err = f.s.Release(f.ctx, f.claim(c).ID, c)
	require.NoError(t, err)
	f.complete(f.claim(c)) // so b's ended leases are not kept for a live task

	live := map[string]api.WorkerStats{a: f.stats(a), b: f.stats(b), c: f.stats(c), idle: f.stats(idle)}
	require.EqualValues(t, 2, live[a].TranscribeTasks)
	require.EqualValues(t, 2, live[c].TranscribeTasks)
	require.EqualValues(t, 1, live[b].ExpiredLeases)

	f.rollBackToV2()
	for id, want := range live {
		assert.Equal(t, want, f.stats(id), "backfill matches the live counters while everything is retained (%s)", id)
	}

	// Once ended leases are pruned, failures and expiries are lost, but
	// completions still come from the retained tasks, timed by their jobs.
	n, err := f.s.DeleteEndedLeases(f.ctx, f.clk.Now().Add(time.Second))
	require.NoError(t, err)
	require.Positive(t, n)
	f.rollBackToV2()
	got := f.stats(a)
	assert.EqualValues(t, 1, got.PrepareTasks)
	assert.EqualValues(t, 2, got.TranscribeTasks)
	assert.EqualValues(t, 1, got.Jobs)
	assert.Equal(t, live[a].AudioS, got.AudioS)
	assert.Equal(t, live[a].ComputeS, got.ComputeS)
	assert.Zero(t, got.Failures)
	assert.NotNil(t, got.FirstTaskAt)
	assert.NotNil(t, got.LastTaskAt)
	assert.Equal(t, api.WorkerStats{}, f.stats(b))
}

package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
)

// throughput returns the work completed in the hour before now.
func (f *fixture) throughput() (Throughput, map[int64]Throughput) {
	f.t.Helper()
	total, byQueue, err := f.s.ThroughputSince(f.ctx, f.clk.Now().Add(-time.Hour))
	require.NoError(f.t, err)
	return total, byQueue
}

// TestThroughputSurvivesJobDeletion: a completed task counts after its job
// is deleted (#87: clients delete jobs as soon as they have the transcript).
func TestThroughputSurvivesJobDeletion(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	w := f.worker(api.SandboxFull, f.pubID)
	j := f.job(f.pubID, "c", api.WeightMedium, 1, 30)
	require.True(t, f.complete(f.claim(w)))
	_, err := f.s.DeleteJob(f.ctx, j.ID)
	require.NoError(t, err)

	total, _ := f.throughput()
	assert.Equal(t, Throughput{Audio: 30 * time.Second, Tasks: 1}, total)
}

// TestThroughputPerTask: a multi-task job contributes as each task
// completes, not only when the job does; repeating a completion does not
// count again; a prepare task's audio is not transcription.
func TestThroughputPerTask(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	other := f.queue("other", 1)
	w := f.worker(api.SandboxFull, f.pubID, other)
	f.prepJob(other, "c")
	l := f.claim(w)
	require.Equal(t, api.KindPrepare, l.Kind)
	samples := int64(20 * canonicalSampleRate)
	require.NoError(t, f.s.AcceptPrepare(f.ctx, l.ID, w, PrepareCompletion{
		Canonical: Canonical{Bytes: 10, SHA256: "c", Samples: samples}, Ranges: []SampleRange{{0, samples}}}))
	total, _ := f.throughput()
	assert.Equal(t, Throughput{}, total, "prepare counts nothing")
	require.True(t, f.complete(f.claim(w)), "the prepared job's one transcribe task")

	f.job(f.pubID, "c", api.WeightMedium, 3, 10)
	var first *Lease
	for range 2 {
		l := f.claim(w)
		if first == nil {
			first = l
		}
		require.False(t, f.complete(l), "the job is not complete")
	}
	f.complete(first) // a repeat
	total, _ = f.throughput()
	assert.Equal(t, Throughput{Audio: 40 * time.Second, Tasks: 3}, total, "two of the job's three tasks")

	require.True(t, f.complete(f.claim(w)))
	total, byQueue := f.throughput()
	assert.Equal(t, Throughput{Audio: 50 * time.Second, Tasks: 4}, total)
	assert.Equal(t, map[int64]Throughput{
		f.pubID: {Audio: 30 * time.Second, Tasks: 3},
		other:   {Audio: 20 * time.Second, Tasks: 1},
	}, byQueue)
}

// TestThroughputWindowAndPrune: completions older than the window drop out
// of it, and pruning deletes buckets before its cutoff only.
func TestThroughputWindowAndPrune(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	w := f.worker(api.SandboxFull, f.pubID)
	f.job(f.pubID, "c", api.WeightMedium, 1, 60)
	f.complete(f.claim(w))
	f.clk.Advance(30 * time.Minute)
	f.job(f.pubID, "c", api.WeightMedium, 1, 6)
	f.complete(f.claim(w))

	total, _ := f.throughput()
	assert.Equal(t, Throughput{Audio: 66 * time.Second, Tasks: 2}, total)
	f.clk.Advance(32 * time.Minute)
	total, _ = f.throughput()
	assert.Equal(t, Throughput{Audio: 6 * time.Second, Tasks: 1}, total, "the first completion left the window")

	n, err := f.s.PruneThroughput(f.ctx, f.clk.Now().Add(-45*time.Minute))
	require.NoError(t, err)
	assert.EqualValues(t, 1, n, "only the older bucket")
	all, _, err := f.s.ThroughputSince(f.ctx, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, Throughput{Audio: 6 * time.Second, Tasks: 1}, all)
}

// TestThroughputMigration: 0011 applies on an existing database and
// backfills from the done leases still kept, by the minute they ended.
func TestThroughputMigration(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	w := f.worker(api.SandboxFull, f.pubID)
	f.job(f.pubID, "c", api.WeightMedium, 2, 10)
	f.complete(f.claim(w))
	f.clk.Advance(2 * time.Hour)
	f.complete(f.claim(w))
	f.fail(func() *Lease { f.job(f.pubID, "c", api.WeightMedium, 1, 99); return f.claim(w) }(), true)

	_, err := f.s.w.ExecContext(f.ctx, undo0012+`DROP TABLE throughput; PRAGMA user_version = 10`)
	require.NoError(t, err)
	require.NoError(t, f.s.Close())
	f.s = f.open()

	total, _ := f.throughput()
	assert.Equal(t, Throughput{Audio: 10 * time.Second, Tasks: 1}, total, "the recent completion")
	all, _, err := f.s.ThroughputSince(f.ctx, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, Throughput{Audio: 20 * time.Second, Tasks: 2}, all, "a failed lease counts nothing")
}

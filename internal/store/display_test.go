package store

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
)

// TestPendingSince: a task is pending since it was created, and since its
// failed lease ended once it requeues; the job page and queue summary read
// that time, the retry-elsewhere workers and the last failure.
func TestPendingSince(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	t0 := f.clk.Now()
	j := f.job(f.pubID, "c", api.WeightMedium, 2, 30)
	w := f.worker(api.SandboxFull, f.pubID)

	since, err := f.s.JobsPendingSince(f.ctx, []string{j.ID, "missing"})
	require.NoError(t, err)
	assert.Equal(t, map[string]time.Time{j.ID: t0}, since)

	f.clk.Advance(time.Minute)
	l := f.claim(w)
	f.clk.Advance(time.Minute)
	t2 := f.clk.Now()
	f.fail(l, true)

	tasks, err := f.s.JobTasks(f.ctx, j.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	assert.Equal(t, api.TaskPending, tasks[0].State)
	assert.Equal(t, t2, *tasks[0].PendingSince, "requeued: pending since the lease ended")
	assert.Equal(t, []string{w}, tasks[0].FailedOn)
	assert.Equal(t, w, tasks[0].LastFailedOn)
	assert.Equal(t, "whisper crashed", tasks[0].Error)
	assert.Equal(t, 1, tasks[0].Attempts)
	assert.Equal(t, t0, *tasks[1].PendingSince)
	assert.Empty(t, tasks[1].FailedOn)
	assert.Equal(t, int64(30*canonicalSampleRate), tasks[1].EndSample-tasks[1].StartSample)

	since, err = f.s.JobsPendingSince(f.ctx, []string{j.ID})
	require.NoError(t, err)
	assert.Equal(t, t0, since[j.ID], "the oldest pending task")

	f.prepJob(f.pubID, "c")
	groups, err := f.s.QueuePending(f.ctx, f.pubID)
	require.NoError(t, err)
	assert.Equal(t, []PendingGroup{
		{Kind: api.KindPrepare, Model: "small.en", Weight: api.WeightMedium, Tasks: 1, OldestSince: t2},
		{Kind: api.KindTranscribe, Model: "small.en", Weight: api.WeightMedium, Tasks: 2,
			AudioSamples: 60 * canonicalSampleRate, OldestSince: t0},
	}, groups)
}

// TestPendingSinceMigration: 0010 backfills pending tasks from their last
// ended lease, else their job's creation.
func TestPendingSinceMigration(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	t0 := f.clk.Now()
	j := f.job(f.pubID, "c", api.WeightMedium, 2, 30)
	w := f.worker(api.SandboxFull, f.pubID)
	f.clk.Advance(time.Minute)
	f.fail(f.claim(w), true)

	_, err := f.s.w.ExecContext(f.ctx, undo0012+`ALTER TABLE tasks DROP COLUMN pending_since; DROP TABLE throughput; PRAGMA user_version = 9`)
	require.NoError(t, err)
	require.NoError(t, f.s.Close())
	f.s = f.open()

	tasks, err := f.s.JobTasks(f.ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, t0.Add(time.Minute), *tasks[0].PendingSince, "from the failed lease")
	assert.Equal(t, t0, *tasks[1].PendingSince, "from the job")
}

func TestProcessWorkers(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	other := f.queue("other", 100)
	seen := f.worker(api.SandboxFull, f.pubID)
	stale := f.worker(api.SandboxFull, f.pubID)
	elsewhere := f.worker(api.SandboxFull, other)
	paused := f.worker(api.SandboxFull, f.pubID)
	_, err := f.s.SetWorkerState(f.ctx, paused, api.WorkerPaused, "user:1")
	require.NoError(t, err)
	require.NoError(t, f.s.TouchWorker(f.ctx, stale, nil))
	f.clk.Advance(10 * time.Minute)
	for _, id := range []string{seen, elsewhere, paused} {
		require.NoError(t, f.s.TouchWorker(f.ctx, id, nil))
	}

	ws, err := f.s.ProcessWorkers(f.ctx, f.pubID, 5*time.Minute)
	require.NoError(t, err)
	require.Len(t, ws, 1)
	assert.Equal(t, seen, ws[0].ID)
}

func TestDisplayRefs(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	w := f.worker(api.SandboxFull)
	j := f.job(f.pubID, "cli_1", api.WeightMedium, 1, 1)
	c, err := f.s.CreateClient(f.ctx, NewClient{Name: "transcriber", TokenHash: "h1", CreatedBy: "user:root"})
	require.NoError(t, err)
	r, err := f.s.CreateClient(f.ctx, NewClient{Name: "old", TokenHash: "h2", CreatedBy: "user:root"})
	require.NoError(t, err)
	require.NoError(t, f.s.RevokeClient(f.ctx, r.ID))

	ws, err := f.s.WorkerRefs(f.ctx, []string{w, w, "gone", ""})
	require.NoError(t, err)
	assert.Equal(t, map[string]WorkerRef{w: {ID: w, Hostname: "h"}}, ws)

	js, err := f.s.JobRefs(f.ctx, []string{j.ID, "gone"})
	require.NoError(t, err)
	assert.Equal(t, map[string]JobRef{j.ID: {ID: j.ID, QueueName: "public", ClientID: "cli_1"}}, js)

	cs, err := f.s.ClientRefs(f.ctx, []string{c.ID, r.ID})
	require.NoError(t, err)
	assert.Equal(t, map[string]ClientRef{c.ID: {ID: c.ID, Name: "transcriber"}, r.ID: {ID: r.ID, Name: "old", Revoked: true}}, cs)

	none, err := f.s.ClientRefs(f.ctx, nil)
	require.NoError(t, err)
	assert.Empty(t, none)
}

// TestStatsEligibleSlots: a queue's eligible slots are those of its
// eligible workers, with busy slots from their last heartbeat, capped at
// their slots.
func TestStatsEligibleSlots(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	a := f.worker(api.SandboxFull, f.pubID)
	b := f.worker(api.SandboxFull, f.pubID)
	require.NoError(t, f.s.AddGrant(f.ctx, WorkerPrincipal(b), api.RoleProcess, "*", "user:1"))
	for id, slots := range map[string]int{a: 2, b: 3} {
		require.NoError(t, f.s.UpdateWorkerFacts(f.ctx, id, api.WorkerFacts{Hostname: "h", Slots: slots,
			Kinds: []api.TaskKind{api.KindTranscribe}}))
	}
	require.NoError(t, f.s.TouchWorker(f.ctx, a, &api.Gauges{SlotsBusy: 5}))
	require.NoError(t, f.s.TouchWorker(f.ctx, b, &api.Gauges{SlotsBusy: 1}))

	st, err := f.s.Stats(f.ctx, time.Minute)
	require.NoError(t, err)
	q := st.Queues[0]
	require.Equal(t, "public", q.Name)
	assert.Equal(t, 2, q.EligibleWorkers, "b holds process twice but counts once")
	assert.Equal(t, 5, q.EligibleSlots)
	assert.Equal(t, 3, q.EligibleSlotsBusy, "a's busy count is capped at its 2 slots")
}

func TestFleetCapacity(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	recent, old := now.Add(-time.Minute), now.Add(-time.Hour)
	gauges := func(g api.Gauges) json.RawMessage {
		b, err := json.Marshal(g)
		require.NoError(t, err)
		return b
	}
	both := []api.TaskKind{api.KindPrepare, api.KindTranscribe}
	ws := []Worker{
		// 4 slots, both kinds at full sandbox, 3 busy.
		{State: api.WorkerActive, LastSeenAt: &recent, Gauges: gauges(api.Gauges{SlotsBusy: 3}),
			Facts: api.WorkerFacts{Slots: 4, Kinds: both, SandboxLevel: api.SandboxFull}},
		// 2 slots offering prepare but below the sandbox minimum, deferring for load.
		{State: api.WorkerActive, LastSeenAt: &recent, Gauges: gauges(api.Gauges{DeferredForLoad: true}),
			Facts: api.WorkerFacts{Slots: 2, Kinds: both, SandboxLevel: api.SandboxFSOnly}},
		// No heartbeat yet: all free.
		{State: api.WorkerActive, LastSeenAt: &recent,
			Facts: api.WorkerFacts{Slots: 1, Kinds: []api.TaskKind{api.KindTranscribe}}},
		{State: api.WorkerActive, LastSeenAt: &old, Facts: api.WorkerFacts{Slots: 8}},
		{State: api.WorkerPaused, LastSeenAt: &recent, Facts: api.WorkerFacts{Slots: 3}},
		{State: api.WorkerPending, Facts: api.WorkerFacts{Slots: 5}},
		{State: api.WorkerRejected, Facts: api.WorkerFacts{Slots: 7}},
	}
	c := FleetCapacity(ws, now, 3*time.Minute, api.SandboxFull)
	assert.Equal(t, SlotCount{Total: 7, InUse: 3, Deferred: 2}, c.All)
	assert.Equal(t, 2, c.All.Available())
	assert.Equal(t, SlotCount{Total: 7, InUse: 3, Deferred: 2}, c.Transcribe)
	assert.Equal(t, SlotCount{Total: 4, InUse: 3}, c.Prepare, "the fs-only worker cannot prepare")
	assert.Equal(t, 3, c.Paused)
	assert.Equal(t, 5, c.Pending)
	assert.Equal(t, 8, c.Offline)
}

package worker

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/media"
	"github.com/deweysasser/whisper-network/internal/worker/workertest"
)

// fakeState is a state directory with an injected free-space probe.
type fakeState struct {
	t            *testing.T
	dir          string
	free         atomic.Int64
	freeErr      atomic.Bool
	clock        time.Time
	meter        *diskMeter
	models, work string
}

func newFakeState(t *testing.T) *fakeState {
	s := &fakeState{t: t, dir: t.TempDir(), clock: time.Unix(1700000000, 0)}
	s.models, s.work = filepath.Join(s.dir, "models"), filepath.Join(s.dir, "work")
	require.NoError(t, os.MkdirAll(s.models, 0o755))
	require.NoError(t, os.MkdirAll(s.work, 0o755))
	s.meter = newDiskMeter(s.dir, s.models, s.work, testCatalog(), func(p string) (int64, error) {
		assert.Equal(t, s.dir, p, "free space is probed on the state directory")
		if s.freeErr.Load() {
			return 0, errors.New("statfs failed")
		}
		return s.free.Load(), nil
	})
	s.meter.now = func() time.Time { return s.clock }
	return s
}

func (s *fakeState) write(path string, n int) {
	s.t.Helper()
	require.NoError(s.t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(s.t, os.WriteFile(path, make([]byte, n), 0o644))
}

func (s *fakeState) gauges(r diskReading) api.Gauges {
	var g api.Gauges
	r.gauges(&g)
	return g
}

func TestDiskGaugesFollowFiles(t *testing.T) {
	s := newFakeState(t)
	s.free.Store(5 << 30)

	g := s.gauges(s.meter.read())
	assert.Equal(t, ptr(int64(0)), g.ModelsBytes)
	assert.Equal(t, ptr(int64(0)), g.WorkBytes)
	assert.Equal(t, ptr(int64(5<<30)), g.StateFreeBytes)
	assert.Empty(t, g.CachedModels)

	tiny := testCatalog()[0]
	s.write(filepath.Join(s.models, tiny.File), 1000)
	s.write(filepath.Join(s.models, "."+tiny.File+".part-123"), 300) // a download in progress
	s.write(filepath.Join(s.work, "lse_1-abc", "input.wav"), 4000)
	s.write(filepath.Join(s.work, "lse_2-def", "input"), 500)
	s.write(filepath.Join(s.work, "lse_2-def", "sub", "out.flac"), 250) // nested, e.g. a crashed child's leftovers
	s.free.Store(4 << 30)

	g = s.gauges(s.meter.read())
	assert.Equal(t, ptr(int64(0)), g.ModelsBytes, "within the TTL the cached reading is reused")

	s.clock = s.clock.Add(diskTTL)
	g = s.gauges(s.meter.read())
	assert.Equal(t, ptr(int64(1300)), g.ModelsBytes, "partial downloads count toward disk use")
	assert.Equal(t, ptr(int64(4750)), g.WorkBytes)
	assert.Equal(t, ptr(int64(4<<30)), g.StateFreeBytes)
	assert.Equal(t, []api.CachedModel{{Name: tiny.Name, Bytes: 1000}}, g.CachedModels,
		"only complete files the catalog knows are listed")

	require.NoError(t, os.RemoveAll(filepath.Join(s.work, "lse_1-abc")))
	require.NoError(t, os.Remove(filepath.Join(s.models, tiny.File)))
	s.freeErr.Store(true)
	g = s.gauges(s.meter.fresh())
	assert.Equal(t, ptr(int64(300)), g.ModelsBytes)
	assert.Equal(t, ptr(int64(750)), g.WorkBytes)
	assert.Nil(t, g.StateFreeBytes, "a failed probe is not reported as zero free")
	assert.Empty(t, g.CachedModels)
}

func TestDiskGateAdmission(t *testing.T) {
	s := newFakeState(t)
	gate := newDiskGate(s.meter, 1000)
	now := s.clock
	gate.now = func() time.Time { return now }
	s.free.Store(11000) // 10000 above the reserve

	rel1, avail, ok := gate.admit(api.KindTranscribe, 6000)
	require.True(t, ok)
	assert.Equal(t, int64(10000), avail)

	// The first lease has written nothing yet, so its 6000 are still owed.
	_, avail, ok = gate.admit(api.KindTranscribe, 6000)
	assert.False(t, ok, "two leases must not be promised the same space")
	assert.Equal(t, int64(4000), avail)

	off := offer{kinds: []api.TaskKind{api.KindPrepare, api.KindTranscribe}, models: []string{"tiny.test"}}
	assert.Equal(t, offer{kinds: []api.TaskKind{api.KindPrepare}}, gate.filter(off), "transcribe withheld")

	// Once the first lease has written its files, free space has dropped
	// by what it wrote and it owes less.
	s.write(filepath.Join(s.work, "lse_1", "input.wav"), 5000)
	s.free.Store(6000)
	assert.Equal(t, offer{kinds: []api.TaskKind{api.KindPrepare}}, gate.filter(off), "still 4000 available")

	// The lease ends: its files go and its promise is returned.
	rel1()
	rel1() // idempotent
	require.NoError(t, os.RemoveAll(filepath.Join(s.work, "lse_1")))
	s.free.Store(11000)
	assert.Equal(t, off, gate.filter(off), "the space needed is available: the kind is offered again")
	_, _, ok = gate.admit(api.KindTranscribe, 6000)
	assert.True(t, ok)

	// A block lapses after diskBlockMax even if space never frees, so a
	// smaller task of the kind gets a chance.
	_, _, ok = gate.admit(api.KindPrepare, 1<<40)
	require.False(t, ok)
	assert.Equal(t, offer{kinds: []api.TaskKind{api.KindTranscribe}, models: off.models}, gate.filter(off))
	now = now.Add(diskBlockMax + time.Second)
	assert.Equal(t, off, gate.filter(off))

	// Without a free-space reading nothing is refused.
	s.freeErr.Store(true)
	_, _, ok = gate.admit(api.KindPrepare, 1<<40)
	assert.True(t, ok)
}

func TestPrepareFit(t *testing.T) {
	t.Parallel()
	const outCap = 2 * gib
	cases := []struct {
		name  string
		avail int64
		want  int64
	}{
		{"no space", 0, 0},
		{"only the slack", diskSlack, 0},
		{"small: input and an equal output", diskSlack + 2*gib, gib},
		{"large: input and the capped output", diskSlack + 14*gib, 12 * gib},
		{"exactly two caps", diskSlack + 2*outCap, outCap},
	}
	for _, c := range cases {
		got := prepareFit(c.avail, outCap)
		assert.Equal(t, c.want, got, c.name)
		if got > 0 {
			assert.LessOrEqual(t, got+min(got, outCap)+diskSlack, c.avail, "%s: the fit is admitted", c.name)
		}
	}
}

func TestLeaseDisk(t *testing.T) {
	h := newHarness(t)
	w, err := New(h.cfg)
	require.NoError(t, err)
	tiny, vad := testCatalog()[0], testCatalog()[2]
	w.cache = NewModelCache(t.Context(), filepath.Join(h.cfg.StateDir, "models"), nil)

	assert.Equal(t, 2*int64(1000)+diskSlack, w.leaseDisk(&api.Lease{Kind: api.KindPrepare, InputBytes: 1000}),
		"a small original: its output is at most its size")
	assert.Equal(t, 12*gib+4*gib+diskSlack, w.leaseDisk(&api.Lease{Kind: api.KindPrepare, InputBytes: 12 * gib}),
		"a large original: the output is at most the child's file-size limit, not the original's size")
	assert.Equal(t, 12*gib+1536*mib+diskSlack,
		w.leaseDisk(&api.Lease{Kind: api.KindPrepare, InputBytes: 12 * gib, MaxOutputBytes: 1536 * mib}),
		"the server's bound on the result lowers it further")
	w.serverOutCap.Store(2 * gib)
	assert.Equal(t, 12*gib+2*gib+diskSlack, w.leaseDisk(&api.Lease{Kind: api.KindPrepare, InputBytes: 12 * gib}),
		"a lease without the bound uses the last one the server gave")

	wav := int64(len(media.WAVHeader(0))) + 2*16000
	l := &api.Lease{Kind: api.KindTranscribe, AudioSamples: 16000, Model: tiny.Name, VADModel: vad.Name}
	assert.Equal(t, wav+diskSlack+tiny.Size+vad.Size, w.leaseDisk(l), "uncached models must be downloaded first")

	require.NoError(t, os.MkdirAll(filepath.Join(h.cfg.StateDir, "models"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(h.cfg.StateDir, "models", tiny.File), tinyData, 0o644))
	assert.Equal(t, wav+diskSlack+vad.Size, w.leaseDisk(l))
}

// A transcribe lease that does not fit on disk is released at once with no
// attempt consumed, transcribe is withheld from claims while prepare is still
// offered, and the task runs once space frees.
func TestDiskAdmissionReleasesLease(t *testing.T) {
	h := newHarness(t)
	var free atomic.Int64
	free.Store(gib + mib) // 1 MiB above the default 1 GiB reserve
	h.cfg.FreeBytes = func(string) (int64, error) { return free.Load(), nil }
	task := h.srv.AddTask(workertest.Task{Kind: api.KindTranscribe, Input: workertest.WAV(2), Model: "tiny.test"})
	_, done := h.run()
	h.activate()

	h.waitEvent("lease.released")
	h.eventually(func() bool {
		ls := h.srv.LeasesFor(task)
		return len(ls) == 1 && !ls[0].Live
	}, "lease never released")
	assert.Equal(t, api.OutcomeReleased, h.srv.LeasesFor(task)[0].Outcome)
	assert.Equal(t, 0, h.srv.Task(task).Attempts, "a release consumes no attempt")
	assert.Equal(t, api.TaskPending, h.srv.Task(task).State)
	assert.Equal(t, 0, h.srv.ModelDownloads("tiny.test"), "nothing was downloaded for a refused lease")

	ev := findEvent(t, h.logs.Bytes(), "lease.released")
	assert.Equal(t, "insufficient disk", ev["reason"])
	assert.Equal(t, h.srv.LeasesFor(task)[0].ID, ev["lease_id"])

	// Later claims offer prepare only.
	n := len(h.srv.Claims())
	h.eventually(func() bool { return len(h.srv.Claims()) >= n+2 }, "the worker stopped claiming")
	last := h.srv.Claims()[len(h.srv.Claims())-1]
	assert.Equal(t, []api.TaskKind{api.KindPrepare}, last.Kinds)
	assert.Empty(t, last.Models)
	assert.Len(t, h.srv.LeasesFor(task), 1, "the task is not claimed again while it cannot fit")

	free.Store(1 << 40)
	h.eventually(h.taskState(task, api.TaskDone), "task never ran after space freed")
	require.Len(t, h.srv.LeasesFor(task), 2)
	assert.Equal(t, 1, h.srv.Task(task).Attempts)
	_ = done
}

// An idle worker reports its disk readings on every claim; heartbeats carry
// them too.
func TestClaimsAndHeartbeatsCarryDiskGauges(t *testing.T) {
	h := newHarness(t)
	h.cfg.ChildEnv = []string{workertest.WhisperMode(workertest.SleepMode("150ms"))}
	_, _ = h.run()
	h.activate()
	h.eventually(func() bool { return len(h.srv.Claims()) > 0 }, "never claimed")
	g := h.srv.Claims()[0].Gauges
	require.NotNil(t, g, "an idle claim carries gauges")
	assert.Equal(t, ptr(int64(1<<40)), g.StateFreeBytes)
	assert.Equal(t, ptr(int64(0)), g.WorkBytes)
	assert.Equal(t, ptr(int64(0)), g.ModelsBytes)
	// Idle, with 1 TiB free, a 1 GiB reserve and the default 4 GiB output cap.
	fit := int64(1<<40) - gib - diskSlack - 4*gib
	assert.Equal(t, ptr(fit), g.PrepareMaxInputBytes)
	assert.Equal(t, ptr(fit), g.PrepareCapacityBytes)

	task := h.srv.AddTask(workertest.Task{Kind: api.KindTranscribe, Input: workertest.WAV(2), Model: "tiny.test"})
	h.eventually(h.taskState(task, api.TaskDone), "task never finished")
	ls := h.srv.LeasesFor(task)
	require.NotEmpty(t, ls[0].Gauges)
	hb := ls[0].Gauges[len(ls[0].Gauges)-1]
	require.NotNil(t, hb.StateFreeBytes)
	require.NotNil(t, hb.ModelsBytes)
	assert.Positive(t, *hb.ModelsBytes, "the model was downloaded by then")
	assert.Equal(t, []api.CachedModel{{Name: "tiny.test", Bytes: int64(len(tinyData))}}, hb.CachedModels)
}

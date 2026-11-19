package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
)

// sizedPrepJob creates a job needing a prepare task, with an original of n
// bytes.
func (f *fixture) sizedPrepJob(queueID int64, n int64) *Job {
	f.t.Helper()
	j, _, err := f.s.CreateJob(f.ctx, NewJob{QueueID: queueID, ClientID: "c",
		Params:   api.JobParams{Model: "small.en"},
		Original: Media{Bytes: n, SHA256: "orig", Type: "video/mp4"}})
	require.NoError(f.t, err)
	return j
}

// TestClaimBoundsPrepareInput: a slot that reports how large an input it
// can take is offered no prepare task with a larger original; without a
// bound every prepare task is offered.
func TestClaimBoundsPrepareInput(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	w := f.worker(api.SandboxFull, f.pubID)
	big := f.sizedPrepJob(f.pubID, 12<<30)
	f.clk.Advance(time.Second)
	small := f.sizedPrepJob(f.pubID, 1<<30)

	el := f.eligibility(w)
	el.PrepareBounded, el.PrepareMaxInput = true, 2<<30
	l, err := f.s.Claim(f.ctx, w, el, ttl)
	require.NoError(t, err)
	require.NotNil(t, l)
	assert.Equal(t, small.ID, l.JobID, "the older job is too large for this slot")

	el.PrepareMaxInput = 0
	l, err = f.s.Claim(f.ctx, w, el, ttl)
	require.NoError(t, err)
	assert.Nil(t, l, "a slot with no room takes no prepare task")

	l, err = f.s.Claim(f.ctx, w, f.eligibility(w), ttl)
	require.NoError(t, err)
	require.NotNil(t, l)
	assert.Equal(t, big.ID, l.JobID, "unbounded: the large task is offered")
}

// TestFailUnplaceable: a prepare task pending past the limit fails when its
// original exceeds the idle capacity of every online worker that may take
// it, and only then.
func TestFailUnplaceable(t *testing.T) {
	gib := func(n int64) *int64 { v := n << 30; return &v }
	const limit = 24 * time.Hour
	cases := []struct {
		name    string
		workers []*int64 // reported PrepareCapacityBytes, one worker each; nil: not reported
		other   bool     // the workers hold process on another queue only
		offline bool     // the workers were last seen long ago
		wait    time.Duration
		fails   bool
	}{
		{"too large for every worker", []*int64{gib(5), gib(8)}, false, false, limit, true},
		{"not yet waited long enough", []*int64{gib(5)}, false, false, limit - time.Minute, false},
		{"one worker can take it", []*int64{gib(5), gib(20)}, false, false, limit, false},
		{"a worker did not report", []*int64{gib(5), nil}, false, false, limit, false},
		{"no worker online", []*int64{gib(5)}, false, true, limit, false},
		{"no worker on its queue", []*int64{gib(5)}, true, false, limit, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			other := f.queue("other", 100)
			j := f.sizedPrepJob(f.pubID, 12<<30)
			f.clk.Advance(c.wait)
			for _, capacity := range c.workers {
				q := f.pubID
				if c.other {
					q = other
				}
				w := f.worker(api.SandboxFull, q)
				require.NoError(t, f.s.TouchWorker(f.ctx, w, &api.Gauges{PrepareCapacityBytes: capacity}))
			}
			if c.offline {
				f.clk.Advance(time.Hour)
			}

			failed, err := f.s.FailUnplaceable(f.ctx, limit, 10*time.Minute)
			require.NoError(t, err)
			state, _ := f.task(j.ID, 0)
			if !c.fails {
				assert.Empty(t, failed)
				assert.Equal(t, "pending", state)
				return
			}
			require.Len(t, failed, 1)
			assert.Equal(t, j.ID, failed[0].JobID)
			assert.True(t, failed[0].JobFailed)
			assert.Contains(t, failed[0].Reason, "12.0 GiB")
			assert.Contains(t, failed[0].Reason, "8.0 GiB")
			assert.Equal(t, "failed", state)
			got, err := f.s.Job(f.ctx, j.ID)
			require.NoError(t, err)
			assert.Equal(t, api.JobFailed, got.State)
		})
	}
}

package store

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/fetchpolicy"
)

// rateLimit claims a fetch for d and reports it rate limited.
func (f *fixture) rateLimit(d string) *LeaseEnd {
	f.t.Helper()
	l := f.claimFetch(d)
	require.NotNil(f.t, l)
	return f.failStage(l, true, api.StageRateLimited)
}

func (f *fixture) cooldown(egress, site string) (api.FetchCooldown, bool) {
	f.t.Helper()
	cs, err := f.s.FetchCooldowns(f.ctx, f.clk.Now())
	require.NoError(f.t, err)
	for _, c := range cs {
		if c.EgressKey == egress && c.Site == site {
			return c, true
		}
	}
	return api.FetchCooldown{}, false
}

func TestRateLimitedRequeuesWithoutAttempt(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	j := f.urlJob(f.pubID, "example.com")
	var since0 int64
	require.NoError(t, f.s.r.QueryRowContext(f.ctx, `SELECT pending_since FROM tasks WHERE job_id = ? AND seq = ?`,
		j.ID, FetchSeq).Scan(&since0))
	f.clk.Advance(time.Minute)
	end := f.rateLimit(d)
	assert.True(t, end.Requeued)
	assert.Equal(t, 0, end.Attempt)
	assert.False(t, end.JobFailed)
	state, attempts := f.task(j.ID, FetchSeq)
	assert.Equal(t, "pending", state)
	assert.Equal(t, 0, attempts)
	var since int64
	require.NoError(t, f.s.r.QueryRowContext(f.ctx, `SELECT pending_since FROM tasks WHERE job_id = ? AND seq = ?`,
		j.ID, FetchSeq).Scan(&since))
	assert.Equal(t, since0, since, "pending_since unchanged")
	// The same worker is not excluded: once the (default 5m) cooldown ends it takes the task again.
	assert.Nil(t, f.claimFetch(d))
	f.clk.Advance(5 * time.Minute)
	assert.NotNil(t, f.claimFetch(d))
}

func TestRateLimitedNotInFailureStreak(t *testing.T) {
	t.Parallel()
	f := newFixture(t, func(c *Config) { c.WorkerFailureLimit = 1 })
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	for range 3 {
		f.urlJob(f.pubID, "example.com")
		f.rateLimit(d)
		f.clk.Advance(time.Hour) // past the cooldown
	}
	w, err := f.s.WorkerByID(f.ctx, d)
	require.NoError(t, err)
	assert.Equal(t, api.WorkerActive, w.State)
	fails, jobs := f.streak(d)
	assert.Zero(t, fails)
	assert.Zero(t, jobs)
}

func TestRateLimitedSetsBackoffCooldown(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	f.urlJob(f.pubID, "youtube.com")
	for i, want := range []time.Duration{12 * time.Hour, 24 * time.Hour, 24 * time.Hour} {
		f.rateLimit(d)
		c, ok := f.cooldown("e1", "youtube.com")
		require.True(t, ok)
		assert.Equal(t, f.clk.Now().Add(want), c.Until, "report %d", i+1)
		assert.Equal(t, i+1, c.Level)
		assert.False(t, c.Manual)
		f.clk.Advance(want)
	}
	_, ok := f.cooldown("e2", "youtube.com")
	assert.False(t, ok)
}

func TestRateLimitedBackoffFallbacks(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	// Invalid youtube policy: falls back to '*' (5m).
	_, err := f.s.w.ExecContext(f.ctx, `UPDATE fetch_policies SET rules = 'bad' WHERE key = 'youtube.com'`)
	require.NoError(t, err)
	f.urlJob(f.pubID, "youtube.com")
	// The invalid row blocks the claim, so lease it directly.
	f.fetchingJobLease(d, "youtube.com")
	c, ok := f.cooldown("e1", "youtube.com")
	require.True(t, ok)
	assert.Equal(t, f.clk.Now().Add(5*time.Minute), c.Until)

	// No usable '*' either: 12h.
	_, err = f.s.w.ExecContext(f.ctx, `DELETE FROM fetch_policies WHERE key = '*'`)
	require.NoError(t, err)
	f.clk.Advance(time.Hour)
	f.fetchingJobLease(d, "other.com")
	c, ok = f.cooldown("e1", "other.com")
	require.True(t, ok)
	assert.Equal(t, f.clk.Now().Add(12*time.Hour), c.Until)
}

// fetchingJobLease puts a fetching job for site under a live lease of d (e1)
// and reports it rate limited.
func (f *fixture) fetchingJobLease(d, site string) {
	f.t.Helper()
	id := "j-" + f.token()
	f.fetchingJob(f.pubID, id)
	_, err := f.s.w.ExecContext(f.ctx, `UPDATE jobs SET fetch_site = ? WHERE id = ?`, site, id)
	require.NoError(f.t, err)
	lid := f.fetchLease(id, d)
	_, err = f.s.FailStage(f.ctx, lid, d, "429", true, api.StageRateLimited)
	require.NoError(f.t, err)
}

func TestManualCooldownNotShortened(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	f.urlJob(f.pubID, "example.com")
	// A manual cooldown past the backoff is left alone.
	long := f.clk.Now().Add(48 * time.Hour)
	require.NoError(t, f.s.InTx(f.ctx, func(tx *Tx) error {
		return tx.SetCooldown(f.ctx, "e1", "example.com", long, "user:1", "manual")
	}))
	f.clk.Advance(48*time.Hour - 10*time.Minute)
	f.fetchingJobLease(d, "example.com")
	c, ok := f.cooldown("e1", "example.com")
	require.True(t, ok)
	assert.True(t, c.Manual)
	assert.Equal(t, long, c.Until)
	assert.Equal(t, 0, c.Level)

	// A shorter manual cooldown is replaced by the automatic one.
	f.clk.Advance(9 * time.Minute)
	f.fetchingJobLease(d, "example.com")
	c, ok = f.cooldown("e1", "example.com")
	require.True(t, ok)
	assert.False(t, c.Manual)
	assert.Equal(t, f.clk.Now().Add(5*time.Minute), c.Until)
	assert.Equal(t, 1, c.Level)
}

func TestAcceptFetchResetsAutoLevel(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	f.urlJob(f.pubID, "example.com")
	f.rateLimit(d)
	f.clk.Advance(time.Hour)
	f.rateLimit(d)
	c, _ := f.cooldown("e1", "example.com")
	require.Equal(t, 2, c.Level)
	f.clk.Advance(time.Hour)
	l := f.claimFetch(d)
	require.NotNil(t, l)
	f.acceptFetch(l, "s")
	_, ok := f.cooldown("e1", "example.com") // expired auto row removed
	assert.False(t, ok)
	f.urlJob(f.pubID, "example.com")
	f.rateLimit(d)
	c, _ = f.cooldown("e1", "example.com")
	assert.Equal(t, 1, c.Level)
	assert.Equal(t, f.clk.Now().Add(5*time.Minute), c.Until, "starts again at the initial backoff")
}

func TestRateLimitedStageIgnoredOnTranscribe(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	w := f.worker(api.SandboxFull, f.pubID)
	f.job(f.pubID, "c", api.WeightMedium, 1, 10)
	l := f.claim(w)
	require.NotNil(t, l)
	end := f.failStage(l, true, api.StageRateLimited)
	assert.Equal(t, 1, end.Attempt, "an ordinary failure")
	var stage *string
	require.NoError(t, f.s.r.QueryRowContext(f.ctx, `SELECT stage FROM leases WHERE id = ?`, l.ID).Scan(&stage))
	assert.Nil(t, stage)
	cs, err := f.s.FetchCooldowns(f.ctx, f.clk.Now())
	require.NoError(t, err)
	assert.Empty(t, cs)
}

func TestFailStrandedRateLimitedFetch(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	limited := f.urlJob(f.pubID, "example.com")
	f.rateLimit(d)
	// A pending fetch never rate limited is left alone.
	fresh := f.urlJob(f.pubID, "other.com")
	limit := 24 * time.Hour
	f.clk.Advance(limit - time.Minute)
	out, err := f.s.FailStranded(f.ctx, limit)
	require.NoError(t, err)
	assert.Empty(t, out)
	f.clk.Advance(time.Minute)
	out, err = f.s.FailStranded(f.ctx, limit)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, limited.ID, out[0].JobID)
	assert.Equal(t, api.KindFetch, out[0].Kind)
	assert.Equal(t, "rate limited by example.com on every downloader for 24h", out[0].Reason)
	assert.True(t, out[0].JobFailed)
	j, err := f.s.Job(f.ctx, limited.ID)
	require.NoError(t, err)
	assert.Equal(t, api.JobFailed, j.State)
	j, err = f.s.Job(f.ctx, fresh.ID)
	require.NoError(t, err)
	assert.Equal(t, api.JobFetching, j.State)
}

func TestPutFetchPolicyValidates(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	r, err := fetchpolicy.ParseRule("2/1m")
	require.NoError(t, err)
	ok := fetchpolicy.Policy{Key: "example.com", Site: "example.com", Rules: []fetchpolicy.Rule{r}, Concurrency: 3,
		BackoffInitial: time.Minute, BackoffCap: time.Hour}
	put := func(p fetchpolicy.Policy) (bool, error) {
		var created bool
		err := f.s.InTx(f.ctx, func(tx *Tx) (err error) {
			created, err = tx.PutFetchPolicy(f.ctx, p, "user:1")
			return err
		})
		return created, err
	}
	created, err := put(ok)
	require.NoError(t, err)
	assert.True(t, created)
	ps, err := f.s.FetchPolicies(f.ctx)
	require.NoError(t, err)
	var got fetchpolicy.Policy
	for _, p := range ps {
		if p.Key == "example.com" {
			got = p
		}
	}
	assert.Equal(t, ok, got)
	var rules, by string
	require.NoError(t, f.s.r.QueryRowContext(f.ctx, `SELECT rules, updated_by FROM fetch_policies WHERE key = 'example.com'`).
		Scan(&rules, &by))
	assert.Equal(t, `["2/1m"]`, rules)
	assert.Equal(t, "user:1", by)

	ok.Concurrency = 1
	created, err = put(ok)
	require.NoError(t, err)
	assert.False(t, created)

	eg := ok
	eg.Key, eg.EgressKey = "example.com@e1", "e1"
	_, err = put(eg)
	require.NoError(t, err)

	for name, mod := range map[string]func(*fetchpolicy.Policy){
		"zero concurrency": func(p *fetchpolicy.Policy) { p.Concurrency = 0 },
		"cap below init":   func(p *fetchpolicy.Policy) { p.BackoffCap = time.Second },
		"bad rule":         func(p *fetchpolicy.Policy) { p.Rules = []fetchpolicy.Rule{{N: 0, Window: time.Second}} },
		"window too small": func(p *fetchpolicy.Policy) { p.Rules = []fetchpolicy.Rule{{N: 1, Window: time.Millisecond}} },
		"key mismatch":     func(p *fetchpolicy.Policy) { p.Key = "other.com" },
		"egress mismatch":  func(p *fetchpolicy.Policy) { p.EgressKey = "e1" },
	} {
		p := ok
		mod(&p)
		_, err := put(p)
		assert.True(t, errors.Is(err, ErrInvalid), "%s: %v", name, err)
	}
	frac := ok
	frac.BackoffInitial = 1500 * time.Millisecond
	_, err = put(frac)
	assert.ErrorIs(t, err, ErrInvalid, "fractional backoff")
	bad, err := f.s.InvalidFetchPolicies(f.ctx)
	require.NoError(t, err)
	assert.Empty(t, bad)
}

func TestDeleteDefaultPolicyRefused(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	err := f.s.InTx(f.ctx, func(tx *Tx) error { return tx.DeleteFetchPolicy(f.ctx, "*") })
	assert.ErrorIs(t, err, ErrConflict)
	require.NoError(t, f.s.InTx(f.ctx, func(tx *Tx) error { return tx.DeleteFetchPolicy(f.ctx, "youtube.com") }))
	err = f.s.InTx(f.ctx, func(tx *Tx) error { return tx.DeleteFetchPolicy(f.ctx, "youtube.com") })
	assert.ErrorIs(t, err, ErrNotFound)
	ps, err := f.s.FetchPolicies(f.ctx)
	require.NoError(t, err)
	require.Len(t, ps, 1)
	assert.Equal(t, "*", ps[0].Key)
}

func TestClearCooldown(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	now := f.clk.Now()
	require.NoError(t, f.s.InTx(f.ctx, func(tx *Tx) error {
		if err := tx.SetCooldown(f.ctx, "e1", "a.com", now.Add(2*time.Hour), "user:1", "why"); err != nil {
			return err
		}
		if err := tx.SetCooldown(f.ctx, "e1", "b.com", now.Add(time.Hour), "user:2", ""); err != nil {
			return err
		}
		return nil
	}))
	_, err := f.s.w.ExecContext(f.ctx, `INSERT INTO fetch_cooldowns (egress_key, site, until, level, manual, set_by)
		VALUES ('e1', 'old.com', ?, 0, 1, 'user:2')`, toMS(now.Add(-time.Hour)))
	require.NoError(t, err)
	for _, c := range []struct {
		e, s string
		u    time.Time
	}{{"", "a.com", now.Add(time.Hour)}, {"e1", "", now.Add(time.Hour)},
		{"e1", "a.com", now}, {"e1", "a.com", now.Add(-time.Hour)}} {
		err := f.s.InTx(f.ctx, func(tx *Tx) error { return tx.SetCooldown(f.ctx, c.e, c.s, c.u, "u", "") })
		assert.ErrorIs(t, err, ErrInvalid)
	}
	cs, err := f.s.FetchCooldowns(f.ctx, now)
	require.NoError(t, err)
	require.Len(t, cs, 2, "active only")
	assert.Equal(t, "b.com", cs[0].Site, "ordered by until")
	assert.Equal(t, api.FetchCooldown{EgressKey: "e1", Site: "a.com", Until: now.Add(2 * time.Hour), Manual: true,
		SetBy: "user:1", Reason: "why"}, cs[1])

	// A set over an automatic row keeps its level.
	_, err = f.s.w.ExecContext(f.ctx, `UPDATE fetch_cooldowns SET level = 3, manual = 0 WHERE site = 'a.com'`)
	require.NoError(t, err)
	require.NoError(t, f.s.InTx(f.ctx, func(tx *Tx) error {
		return tx.SetCooldown(f.ctx, "e1", "a.com", now.Add(3*time.Hour), "user:3", "again")
	}))
	cs, _ = f.s.FetchCooldowns(f.ctx, now)
	assert.Equal(t, 3, cs[1].Level)
	assert.True(t, cs[1].Manual)

	require.NoError(t, f.s.InTx(f.ctx, func(tx *Tx) error { return tx.ClearCooldown(f.ctx, "e1", "a.com") }))
	cs, _ = f.s.FetchCooldowns(f.ctx, now)
	require.Len(t, cs, 1)
	assert.Equal(t, "b.com", cs[0].Site)
}

func TestRateLimitedBurstEscalatesOnce(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	f.fetchingJobLease(d, "example.com")
	f.fetchingJobLease(d, "example.com")
	f.clk.Advance(10 * time.Second)
	f.fetchingJobLease(d, "example.com")
	c, ok := f.cooldown("e1", "example.com")
	require.True(t, ok)
	assert.Equal(t, 1, c.Level)
	assert.Equal(t, f.clk.Now().Add(5*time.Minute-10*time.Second), c.Until)

	// Past the burst window a further refusal escalates.
	f.clk.Advance(time.Minute)
	f.fetchingJobLease(d, "example.com")
	c, _ = f.cooldown("e1", "example.com")
	assert.Equal(t, 2, c.Level)
	assert.Equal(t, f.clk.Now().Add(10*time.Minute), c.Until)
}

func TestFailStrandedRateLimitWaitFromFirstRefusal(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	j := f.urlJob(f.pubID, "example.com")
	limit := 24 * time.Hour
	// Paced for longer than the limit, then refused once: not stranded.
	f.clk.Advance(2 * limit)
	f.rateLimit(d)
	out, err := f.s.FailStranded(f.ctx, limit)
	require.NoError(t, err)
	assert.Empty(t, out)
	f.clk.Advance(limit)
	out, err = f.s.FailStranded(f.ctx, limit)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, j.ID, out[0].JobID)

	// A refusal from an earlier retry does not count.
	g := f.urlJob(f.pubID, "other.com")
	f.rateLimit(d)
	_, err = f.s.w.ExecContext(f.ctx, `UPDATE jobs SET retries = retries + 1 WHERE id = ?`, g.ID)
	require.NoError(t, err)
	f.clk.Advance(2 * limit)
	out, err = f.s.FailStranded(f.ctx, limit)
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestRateLimitedOnFailingJobCancelsTaskSetsCooldown(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	f.fetchingJob(f.pubID, "j1")
	_, err := f.s.w.ExecContext(f.ctx, `INSERT INTO tasks (job_id, seq, kind, cost, state) VALUES ('j1', 0, 'prepare', 1, 'failed')`)
	require.NoError(t, err)
	lid := f.fetchLease("j1", d)
	end, err := f.s.FailStage(f.ctx, lid, d, "429", true, api.StageRateLimited)
	require.NoError(t, err)
	assert.False(t, end.Requeued)
	state, _ := f.task("j1", FetchSeq)
	assert.Equal(t, string(taskCancelled), state)
	_, ok := f.cooldown("e1", "x")
	assert.True(t, ok)
}

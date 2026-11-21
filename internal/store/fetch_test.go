package store

import (
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
)

// urlJob creates a URL job for site on queueID.
func (f *fixture) urlJob(queueID int64, site string) *Job {
	f.t.Helper()
	j, created, err := f.s.CreateFetchJob(f.ctx, NewFetchJob{QueueID: queueID, ClientID: "c",
		Params: api.JobParams{Model: "small.en"}, URL: "https://" + site + "/v/" + f.token(), Site: site})
	require.NoError(f.t, err)
	require.True(f.t, created)
	return j
}

// fetchEl is what a full-sandbox downloader slot offers.
func fetchEl(waiting ...FetchWaiter) Eligibility {
	return Eligibility{Kinds: []api.TaskKind{api.KindFetch}, SandboxLevel: api.SandboxFull, Waiting: waiting}
}

// waiters are the given downloaders as waiting fetch claims, with their
// stored egress keys and sandbox levels.
func (f *fixture) waiters(workerIDs ...string) []FetchWaiter {
	f.t.Helper()
	var out []FetchWaiter
	for _, id := range workerIDs {
		w := FetchWaiter{WorkerID: id}
		require.NoError(f.t, f.s.r.QueryRowContext(f.ctx, `SELECT egress_key, sandbox_level FROM workers WHERE id = ?`,
			id).Scan(&w.EgressKey, &w.SandboxLevel))
		out = append(out, w)
	}
	return out
}

// claimFetch claims for workerID while the downloaders waiting (worker ids)
// have fetch claims waiting.
func (f *fixture) claimFetch(workerID string, waiting ...string) *Lease {
	f.t.Helper()
	l, err := f.s.Claim(f.ctx, workerID, fetchEl(f.waiters(waiting...)...), ttl)
	require.NoError(f.t, err)
	return l
}

func (f *fixture) acceptFetch(l *Lease, sha string) {
	f.t.Helper()
	require.NoError(f.t, f.s.AcceptFetch(f.ctx, l.ID, l.WorkerID, FetchCompletion{
		Original: Media{Bytes: 1234, SHA256: sha, Type: "audio/webm"}}))
}

func TestCreateFetchJob(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	j, created, err := f.s.CreateFetchJob(f.ctx, NewFetchJob{QueueID: f.pubID, ClientID: "c",
		Params: api.JobParams{Model: "small.en"}, URL: "https://youtu.be/abc", Site: "youtube.com"})
	require.NoError(t, err)
	require.True(t, created)
	assert.Equal(t, api.JobFetching, j.State)
	assert.Equal(t, "https://youtu.be/abc", j.FetchURL)
	assert.Equal(t, "youtube.com", j.FetchSite)
	assert.Equal(t, 1, j.Tasks.Total)
	assert.Equal(t, 1, j.Tasks.Pending)

	var kind, state string
	var seq int
	var cost float64
	require.NoError(t, f.s.r.QueryRowContext(f.ctx, `SELECT seq, kind, state, cost FROM tasks WHERE job_id = ?`,
		j.ID).Scan(&seq, &kind, &state, &cost))
	assert.Equal(t, FetchSeq, seq)
	assert.Equal(t, "fetch", kind)
	assert.Equal(t, "pending", state)
	assert.Equal(t, DefaultPrepareCost, cost, "FetchCost defaults to PrepareCost")

	got, err := f.s.Job(f.ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, "youtube.com", got.FetchSite)

	_, _, err = f.s.CreateFetchJob(f.ctx, NewFetchJob{QueueID: f.pubID, ClientID: "c",
		Params: api.JobParams{Model: "small.en"}, Site: "youtube.com"})
	assert.ErrorIs(t, err, ErrInvalid, "no url")
	_, _, err = f.s.CreateFetchJob(f.ctx, NewFetchJob{QueueID: f.pubID, ClientID: "c",
		Params: api.JobParams{Model: "small.en"}, URL: "https://x/"})
	assert.ErrorIs(t, err, ErrInvalid, "no site")
}

func TestCreateFetchJobCost(t *testing.T) {
	t.Parallel()
	f := newFixture(t, func(c *Config) { c.FetchCost = 7 })
	j := f.urlJob(f.pubID, "youtube.com")
	var cost float64
	require.NoError(t, f.s.r.QueryRowContext(f.ctx, `SELECT cost FROM tasks WHERE job_id = ?`, j.ID).Scan(&cost))
	assert.Equal(t, 7.0, cost)
}

func TestCreateFetchJobIdempotent(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	nj := NewFetchJob{QueueID: f.pubID, ClientID: "c", IdempotencyKey: "k1",
		Params: api.JobParams{Model: "small.en"}, URL: "https://youtu.be/abc", Site: "youtube.com"}
	a, created, err := f.s.CreateFetchJob(f.ctx, nj)
	require.NoError(t, err)
	require.True(t, created)
	nj.URL = "https://youtu.be/other"
	b, created, err := f.s.CreateFetchJob(f.ctx, nj)
	require.NoError(t, err)
	assert.False(t, created)
	assert.Equal(t, a.ID, b.ID)
	assert.Equal(t, "https://youtu.be/abc", b.FetchURL)
	var n int
	require.NoError(t, f.s.r.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM tasks`).Scan(&n))
	assert.Equal(t, 1, n)
}

func TestAcceptFetchToPrepare(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	j := f.urlJob(f.pubID, "example.com")
	l := f.claimFetch(d)
	require.NotNil(t, l)
	assert.Equal(t, api.KindFetch, l.Kind)
	assert.Equal(t, FetchSeq, l.Seq)
	assert.Equal(t, "e1", l.EgressKey)

	f.acceptFetch(l, "sha-orig")

	got, err := f.s.Job(f.ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, api.JobPreparing, got.State)
	assert.Equal(t, Media{Bytes: 1234, SHA256: "sha-orig", Type: "audio/webm"}, got.Original)
	assert.Nil(t, got.Canonical)
	st, _ := f.task(j.ID, FetchSeq)
	assert.Equal(t, "done", st)
	st, att := f.task(j.ID, 0)
	assert.Equal(t, "pending", st)
	assert.Equal(t, 0, att)
	var kind string
	var cost float64
	require.NoError(t, f.s.r.QueryRowContext(f.ctx, `SELECT kind, cost FROM tasks WHERE job_id = ? AND seq = 0`,
		j.ID).Scan(&kind, &cost))
	assert.Equal(t, "prepare", kind)
	assert.Equal(t, DefaultPrepareCost, cost)
	ld, err := f.s.Lease(f.ctx, l.ID, d)
	require.NoError(t, err)
	assert.Equal(t, api.OutcomeDone, ld.Outcome)

	// A processing worker now takes the prepare task.
	w := f.worker(api.SandboxFull, f.pubID)
	pl := f.claim(w)
	require.NotNil(t, pl)
	assert.Equal(t, api.KindPrepare, pl.Kind)
	assert.Equal(t, j.ID, pl.JobID)
}

func TestAcceptFetchFastPath(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	j := f.urlJob(f.pubID, "example.com")
	l := f.claimFetch(d)
	require.NotNil(t, l)
	require.NoError(t, f.s.AcceptFetch(f.ctx, l.ID, d, FetchCompletion{
		Original:  Media{Bytes: 100, SHA256: "o", Type: "audio/flac"},
		Canonical: &Canonical{Bytes: 100, SHA256: "o", Samples: 32000},
		Ranges:    []SampleRange{{0, 16000}, {16000, 32000}}}))

	got, err := f.s.Job(f.ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, api.JobQueued, got.State)
	require.NotNil(t, got.Canonical)
	assert.Equal(t, Canonical{Bytes: 100, SHA256: "o", Samples: 32000}, *got.Canonical)
	assert.Equal(t, 3, got.Tasks.Total)
	assert.Equal(t, 2, got.Tasks.Pending)
	var n int
	require.NoError(t, f.s.r.QueryRowContext(f.ctx,
		`SELECT COUNT(*) FROM tasks WHERE job_id = ? AND kind = 'prepare'`, j.ID).Scan(&n))
	assert.Equal(t, 0, n)

	// The done fetch task is not a transcript part.
	outs, err := f.s.JobOutputs(f.ctx, j.ID)
	require.NoError(t, err)
	assert.Empty(t, outs.Transcribe)
	assert.Nil(t, outs.Prepare)
	assert.Len(t, outs.Missing, 2)

	// Bad ranges are refused before anything changes.
	j2 := f.urlJob(f.pubID, "example.com")
	l2 := f.claimFetch(d)
	require.NotNil(t, l2)
	require.Equal(t, j2.ID, l2.JobID)
	err = f.s.AcceptFetch(f.ctx, l2.ID, d, FetchCompletion{Original: Media{SHA256: "x"},
		Canonical: &Canonical{Samples: 10}, Ranges: []SampleRange{{0, 20}}})
	assert.ErrorIs(t, err, ErrInvalid)
	err = f.s.AcceptFetch(f.ctx, l2.ID, d, FetchCompletion{Original: Media{SHA256: "x"},
		Ranges: []SampleRange{{0, 20}}})
	assert.ErrorIs(t, err, ErrInvalid, "ranges without canonical")
}

func TestAcceptFetchIdempotent(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	j := f.urlJob(f.pubID, "example.com")
	l := f.claimFetch(d)
	require.NotNil(t, l)
	f.acceptFetch(l, "sha-1")
	f.acceptFetch(l, "sha-1")
	err := f.s.AcceptFetch(f.ctx, l.ID, d, FetchCompletion{Original: Media{SHA256: "sha-2"}})
	assert.ErrorIs(t, err, ErrConflict)
	var n int
	require.NoError(t, f.s.r.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM tasks WHERE job_id = ?`, j.ID).Scan(&n))
	assert.Equal(t, 2, n, "fetch + one prepare")

	// Not the holder; not a fetch lease.
	assert.ErrorIs(t, f.s.AcceptFetch(f.ctx, l.ID, "other", FetchCompletion{Original: Media{SHA256: "sha-1"}}), ErrNotFound)
	w := f.worker(api.SandboxFull, f.pubID)
	pl := f.claim(w)
	require.NotNil(t, pl)
	assert.ErrorIs(t, f.s.AcceptFetch(f.ctx, pl.ID, w, FetchCompletion{Original: Media{SHA256: "z"}}), ErrInvalid)
}

func TestAcceptFetchGuards(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	j := f.urlJob(f.pubID, "example.com")
	l := f.claimFetch(d)
	require.NotNil(t, l)
	// The job leaves fetching under the lease (as no store path does today:
	// the guard is what keeps a stray completion from re-entering prepare).
	_, err := f.s.w.ExecContext(f.ctx, `UPDATE jobs SET state = 'preparing' WHERE id = ?`, j.ID)
	require.NoError(t, err)
	err = f.s.AcceptFetch(f.ctx, l.ID, d, FetchCompletion{Original: Media{SHA256: "x"}})
	assert.ErrorIs(t, err, ErrConflict)
	st, _ := f.task(j.ID, FetchSeq)
	assert.Equal(t, "leased", st, "rolled back")

	f.urlJob(f.pubID, "example.com")
	l2 := f.claimFetch(d)
	require.NotNil(t, l2)
	f.clk.Advance(ttl + time.Second)
	err = f.s.AcceptFetch(f.ctx, l2.ID, d, FetchCompletion{Original: Media{SHA256: "y"}})
	assert.ErrorIs(t, err, ErrLeaseGone)
}

func TestAcceptFetchResetsCooldownLevel(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	f.urlJob(f.pubID, "example.com")
	l := f.claimFetch(d)
	require.NotNil(t, l)
	now := toMS(f.clk.Now())
	_, err := f.s.w.ExecContext(f.ctx, `INSERT INTO fetch_cooldowns (egress_key, site, until, level, manual, set_by) VALUES
		('e1', 'example.com', ?, 3, 0, 'system'), ('e1', 'other.com', ?, 3, 0, 'system'),
		('e2', 'example.com', ?, 3, 0, 'system')`, now-1000, now-1000, now-1000)
	require.NoError(t, err)
	f.acceptFetch(l, "s")
	level := func(key, site string) int {
		var lv int
		err := f.s.r.QueryRowContext(f.ctx, `SELECT level FROM fetch_cooldowns WHERE egress_key = ? AND site = ?`,
			key, site).Scan(&lv)
		if err == sql.ErrNoRows {
			return -1
		}
		require.NoError(t, err)
		return lv
	}
	assert.Equal(t, -1, level("e1", "example.com"), "expired automatic cooldown removed")
	assert.Equal(t, 3, level("e1", "other.com"))
	assert.Equal(t, 3, level("e2", "example.com"))

	// An active automatic cooldown keeps its until but drops its level; a
	// manual one is left alone.
	f.urlJob(f.pubID, "example.com")
	f.urlJob(f.pubID, "manual.com")
	l1 := f.claimFetch(d)
	l2 := f.claimFetch(d)
	require.NotNil(t, l1)
	require.NotNil(t, l2)
	_, err = f.s.w.ExecContext(f.ctx, `INSERT INTO fetch_cooldowns (egress_key, site, until, level, manual, set_by) VALUES
		('e1', 'example.com', ?, 2, 0, 'system'), ('e1', 'manual.com', ?, 5, 1, 'user:1')`, now+60000, now+60000)
	require.NoError(t, err)
	f.acceptFetch(l1, "s1")
	f.acceptFetch(l2, "s2")
	assert.Equal(t, 0, level("e1", "example.com"))
	assert.Equal(t, 5, level("e1", "manual.com"))
	var until int64
	require.NoError(t, f.s.r.QueryRowContext(f.ctx,
		`SELECT until FROM fetch_cooldowns WHERE egress_key = 'e1' AND site = 'example.com'`).Scan(&until))
	assert.Equal(t, now+60000, until)
}

func TestClaimFetchRequiresFetchGrant(t *testing.T) {
	t.Parallel()
	f := newFixture(t, func(c *Config) { c.FetchMaxBytes = 1000 })
	small, err := f.s.CreateQueue(f.ctx, NewQueue{Name: "small", MaxUploadBytes: 500})
	require.NoError(t, err)
	d := f.downloader(api.SandboxFull, "e1")
	j := f.urlJob(f.pubID, "example.com")
	assert.Nil(t, f.claimFetch(d), "no fetch grant")
	require.NoError(t, f.s.AddGrant(f.ctx, WorkerPrincipal(d), api.RoleProcess, QueueScope(f.pubID), "user:1"))
	assert.Nil(t, f.claimFetch(d), "process is not fetch")
	require.NoError(t, f.s.AddGrant(f.ctx, WorkerPrincipal(d), api.RoleFetch, QueueScope(f.pubID), "user:1"))

	el := fetchEl()
	el.SandboxLevel = api.SandboxFSOnly
	l, err := f.s.Claim(f.ctx, d, el, ttl)
	require.NoError(t, err)
	assert.Nil(t, l, "below FetchMinSandbox")

	l = f.claimFetch(d)
	require.NotNil(t, l)
	assert.Equal(t, j.ID, l.JobID)
	assert.Equal(t, j.FetchURL, l.URL)
	assert.Equal(t, "example.com", l.Site)
	a := l.API(10 * time.Second)
	assert.Equal(t, api.KindFetch, a.Kind)
	assert.Equal(t, j.FetchURL, a.URL)
	assert.Equal(t, "example.com", a.Site)
	assert.Equal(t, int64(1000), a.MaxOutputBytes, "server default cap")
	var egress string
	require.NoError(t, f.s.r.QueryRowContext(f.ctx, `SELECT egress_key FROM leases WHERE id = ?`, l.ID).Scan(&egress))
	assert.Equal(t, "e1", egress)
	got, err := f.s.Job(f.ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, api.JobFetching, got.State, "a fetch claim does not make the job running")

	require.NoError(t, f.s.AddGrant(f.ctx, WorkerPrincipal(d), api.RoleFetch, QueueScope(small.ID), "user:1"))
	f.urlJob(small.ID, "example.com")
	l = f.claimFetch(d)
	require.NotNil(t, l)
	assert.Equal(t, int64(500), l.API(time.Second).MaxOutputBytes, "queue cap below server cap")
}

func TestProcessingWorkerNeverClaimsFetch(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	w := f.worker(api.SandboxFull, f.pubID)
	require.NoError(t, f.s.AddGrant(f.ctx, WorkerPrincipal(w), api.RoleFetch, "*", "user:1"))
	f.urlJob(f.pubID, "example.com")
	el := f.eligibility(w)
	el.Kinds = append(el.Kinds, api.KindFetch)
	l, err := f.s.Claim(f.ctx, w, el, ttl)
	require.NoError(t, err)
	assert.Nil(t, l)
	l, err = f.s.Claim(f.ctx, w, fetchEl(), ttl)
	require.NoError(t, err)
	assert.Nil(t, l)
}

func TestDownloaderNeverClaimsPrepare(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	require.NoError(t, f.s.AddGrant(f.ctx, WorkerPrincipal(d), api.RoleProcess, "*", "user:1"))
	f.prepJob(f.pubID, "c")
	f.job(f.pubID, "c", api.WeightMedium, 1, 10)
	el := f.eligibility(d)
	el.Kinds = append(el.Kinds, api.KindFetch)
	l, err := f.s.Claim(f.ctx, d, el, ttl)
	require.NoError(t, err)
	assert.Nil(t, l)
}

func TestFetchPacingOnePerThreeMinutes(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	f.urlJob(f.pubID, "youtube.com")
	f.urlJob(f.pubID, "youtube.com")
	l := f.claimFetch(d)
	require.NotNil(t, l)
	assert.Nil(t, f.claimFetch(d), "concurrency 1")
	f.acceptFetch(l, "a")
	assert.Nil(t, f.claimFetch(d), "1/3m")
	f.clk.Advance(3*time.Minute - time.Second)
	assert.Nil(t, f.claimFetch(d), "1/3m")
	f.clk.Advance(time.Second)
	l = f.claimFetch(d)
	require.NotNil(t, l)
	f.acceptFetch(l, "b")

	// Another site is not held back by youtube's pacing.
	f.urlJob(f.pubID, "youtube.com")
	f.urlJob(f.pubID, "example.com")
	l = f.claimFetch(d)
	require.NotNil(t, l)
	assert.Equal(t, "example.com", l.Site)

	// A cooldown blocks the site.
	f.clk.Advance(3 * time.Minute)
	_, err := f.s.w.ExecContext(f.ctx, `INSERT INTO fetch_cooldowns (egress_key, site, until, level, manual, set_by)
		VALUES ('e1', 'youtube.com', ?, 0, 1, 'user:1')`, toMS(f.clk.Now().Add(time.Hour)))
	require.NoError(t, err)
	assert.Nil(t, f.claimFetch(d), "cooldown")
	f.clk.Advance(time.Hour)
	require.NotNil(t, f.claimFetch(d))
}

func TestFetchPacingPerEgressGroup(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	a := f.downloader(api.SandboxFull, "label:a", f.pubID)
	b := f.downloader(api.SandboxFull, "label:b", f.pubID)
	a2 := f.downloader(api.SandboxFull, "label:a", f.pubID)
	for range 3 {
		f.urlJob(f.pubID, "youtube.com")
	}
	l := f.claimFetch(a)
	require.NotNil(t, l)
	f.acceptFetch(l, "a1") // ended: only the 1/3m rule can block a2
	assert.Nil(t, f.claimFetch(a2), "same group: 1/3m")
	require.NotNil(t, f.claimFetch(b), "another group")
	f.clk.Advance(3 * time.Minute)
	require.NotNil(t, f.claimFetch(a2))
}

func TestFetchPacingAcrossQueues(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	other := f.queue("other", 0)
	d := f.downloader(api.SandboxFull, "e1", f.pubID, other)
	f.urlJob(f.pubID, "youtube.com")
	f.urlJob(other, "youtube.com")
	l := f.claimFetch(d)
	require.NotNil(t, l)
	f.acceptFetch(l, "a")
	assert.Nil(t, f.claimFetch(d), "the group's 1/3m spans queues")
	f.clk.Advance(3 * time.Minute)
	l = f.claimFetch(d)
	require.NotNil(t, l)
	assert.Equal(t, other, l.QueueID)
}

func TestFetchRotationIgnoresGroupThatCannotClaim(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	a := f.downloader(api.SandboxFull, "A", f.pubID)
	b := f.downloader(api.SandboxFull, "B", f.pubID)
	j1 := f.urlJob(f.pubID, "example.com")
	f.urlJob(f.pubID, "example.com")
	lb := f.claimFetch(b)
	require.NotNil(t, lb)
	require.Equal(t, j1.ID, lb.JobID)
	f.clk.Advance(time.Second)
	require.NotNil(t, f.claimFetch(a))
	// B fails job1 retryably: it is pending again, but never for b.
	_, err := f.s.Fail(f.ctx, lb.ID, b, "net", true)
	require.NoError(t, err)
	f.clk.Advance(3 * time.Minute)
	assert.Nil(t, f.claimFetch(b, a, b), "b failed the only pending task")
	l := f.claimFetch(a, a, b)
	require.NotNil(t, l, "B cannot take job1, so it must not hold A back")
	assert.Equal(t, j1.ID, l.JobID)
}

func TestFetchRotationIgnoresGroupBelowSandboxFloor(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	a := f.downloader(api.SandboxFull, "A", f.pubID)
	b := f.downloader(api.SandboxFSOnly, "B", f.pubID)
	f.urlJob(f.pubID, "example.com")
	f.urlJob(f.pubID, "example.com")
	l := f.claimFetch(a)
	require.NotNil(t, l)
	f.acceptFetch(l, "x")
	require.NotNil(t, f.claimFetch(a, a, b), "B (never fetched) is below the fetch sandbox floor")
}

// TestFetchRotationCountsOnlyWaitingDownloaders: rotation defers to another
// group only for a downloader of it that is waiting now and could take the
// task. A group whose waiting downloader holds no grant, while its granted
// one is not waiting (busy, offline but not yet stale), must not hold the
// site back from everyone else.
func TestFetchRotationCountsOnlyWaitingDownloaders(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	a1 := f.downloader(api.SandboxFull, "A")            // waiting, no fetch grant
	f.downloader(api.SandboxFull, "A", f.pubID)         // granted, not waiting
	a3 := f.downloader(api.SandboxFSOnly, "A", f.pubID) // waiting, granted, stored level below the floor
	b := f.downloader(api.SandboxFull, "B", f.pubID)
	f.urlJob(f.pubID, "example.com")
	f.urlJob(f.pubID, "example.com")
	l := f.claimFetch(b)
	require.NotNil(t, l)
	f.acceptFetch(l, "b1") // B fetched the site; A never did
	f.clk.Advance(3 * time.Minute)
	require.NotNil(t, f.claimFetch(b, a1, a3, b), "no waiting downloader of A can take the task")

	// A waiting downloader that can serve it does hold B back.
	f.urlJob(f.pubID, "example.com")
	f.clk.Advance(3 * time.Minute)
	a4 := f.downloader(api.SandboxFull, "A", f.pubID)
	assert.Nil(t, f.claimFetch(b, a4, b), "A's waiting a4 goes first")
	// Its reported level, not the stored one, decides.
	lowered := f.waiters(a4)
	lowered[0].SandboxLevel = api.SandboxFSOnly
	el := fetchEl(append(lowered, f.waiters(b)...)...)
	got, err := f.s.Claim(f.ctx, b, el, ttl)
	require.NoError(t, err)
	assert.NotNil(t, got, "a4's waiting claim reports a level below the floor")
}

func TestFetchInvalidPolicyFailsClosed(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	_, err := f.s.w.ExecContext(f.ctx, `UPDATE fetch_policies SET rules = 'not json' WHERE key = 'youtube.com'`)
	require.NoError(t, err)
	f.urlJob(f.pubID, "youtube.com")
	f.urlJob(f.pubID, "example.com")
	l := f.claimFetch(d)
	require.NotNil(t, l)
	assert.Equal(t, "example.com", l.Site, "other sites still use '*'")
	f.acceptFetch(l, "x")
	f.clk.Advance(time.Hour)
	assert.Nil(t, f.claimFetch(d), "youtube.com is blocked, not '*'")
	bad, err := f.s.InvalidFetchPolicies(f.ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"youtube.com"}, bad)
	_, ok, err := f.s.NextFetchEligible(f.ctx, f.clk.Now())
	require.NoError(t, err)
	assert.False(t, ok, "blocked with no time it clears")
}

func TestFetchInvalidEgressPolicyFailsClosed(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d1 := f.downloader(api.SandboxFull, "e1", f.pubID)
	d2 := f.downloader(api.SandboxFull, "e2", f.pubID)
	_, err := f.s.w.ExecContext(f.ctx, `INSERT INTO fetch_policies (key, site, egress_key, rules, concurrency,
		backoff_initial_s, backoff_cap_s, updated_by, updated_at) VALUES
		('example.com@e1', 'example.com', 'e1', '["0/1s"]', 1, 60, 60, 'x', 0)`)
	require.NoError(t, err)
	f.urlJob(f.pubID, "example.com")
	assert.Nil(t, f.claimFetch(d1), "(example.com, e1) blocked")
	require.NotNil(t, f.claimFetch(d2), "e2 is unaffected")
}

func TestFetchInvalidDefaultPolicyBlocksAll(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	_, err := f.s.w.ExecContext(f.ctx, `UPDATE fetch_policies SET concurrency = 0 WHERE key = '*'`)
	require.NoError(t, err)
	f.urlJob(f.pubID, "youtube.com")
	f.urlJob(f.pubID, "example.com")
	assert.Nil(t, f.claimFetch(d))
	bad, err := f.s.InvalidFetchPolicies(f.ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"*"}, bad)
}

func TestFetchConcurrencyLimit(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	d2 := f.downloader(api.SandboxFull, "e1", f.pubID)
	for range 3 {
		f.urlJob(f.pubID, "example.com")
	}
	l1 := f.claimFetch(d)
	require.NotNil(t, l1)
	require.NotNil(t, f.claimFetch(d2))
	assert.Nil(t, f.claimFetch(d), "'*' concurrency 2 per group")
	f.acceptFetch(l1, "x")
	require.NotNil(t, f.claimFetch(d))
}

func TestFetchRotationPrefersLeastRecentGroup(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	a := f.downloader(api.SandboxFull, "A", f.pubID)
	b := f.downloader(api.SandboxFull, "B", f.pubID)
	for range 2 {
		f.urlJob(f.pubID, "youtube.com")
	}
	l := f.claimFetch(b)
	require.NotNil(t, l)
	f.acceptFetch(l, "b1")
	f.clk.Advance(time.Minute)
	l = f.claimFetch(a)
	require.NotNil(t, l)
	f.acceptFetch(l, "a1")
	f.clk.Advance(3 * time.Minute)

	f.urlJob(f.pubID, "youtube.com")
	assert.Nil(t, f.claimFetch(a, a, b), "B fetched youtube less recently")
	assert.Nil(t, f.claimFetch(a, b), "the claimer's own waiting claim changes nothing")
	l = f.claimFetch(a)
	require.NotNil(t, l, "no one else waiting")
	f.acceptFetch(l, "a2")

	// A group that has never fetched the site goes first; a waiting group
	// with no downloader able to serve it is ignored.
	f.clk.Advance(3 * time.Minute)
	f.urlJob(f.pubID, "youtube.com")
	c := f.downloader(api.SandboxFull, "C", f.pubID)
	d := f.downloader(api.SandboxFull, "D") // no fetch grant
	assert.Nil(t, f.claimFetch(b, a, b, c), "C never fetched")
	require.NotNil(t, f.claimFetch(b, a, b, d), "D (no fetch grant) cannot serve the queue")

	// A waiting group that is itself blocked does not hold others back.
	f.clk.Advance(3 * time.Minute)
	f.urlJob(f.pubID, "youtube.com")
	_, err := f.s.w.ExecContext(f.ctx, `INSERT INTO fetch_cooldowns (egress_key, site, until, level, manual, set_by)
		VALUES ('C', 'youtube.com', ?, 0, 1, 'user:1')`, toMS(f.clk.Now().Add(time.Hour)))
	require.NoError(t, err)
	require.NotNil(t, f.claimFetch(a, a, c))
}

func TestFetchClaimRaceSameGroup(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	const n = 4
	ws := make([]string, n)
	for i := range ws {
		ws[i] = f.downloader(api.SandboxFull, "same", f.pubID)
	}
	for range n {
		f.urlJob(f.pubID, "youtube.com")
	}
	// The policy allows one start per 3m and enough concurrency that only
	// the rate rule can stop the second claim.
	_, err := f.s.w.ExecContext(f.ctx, `UPDATE fetch_policies SET concurrency = 10 WHERE key = 'youtube.com'`)
	require.NoError(t, err)

	var wg sync.WaitGroup
	start := make(chan struct{})
	got := make([]*Lease, n)
	errs := make([]error, n)
	for i := range ws {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			got[i], errs[i] = f.s.Claim(f.ctx, ws[i], fetchEl(), ttl)
		}()
	}
	close(start)
	wg.Wait()
	leased := 0
	for i := range got {
		require.NoError(t, errs[i])
		if got[i] != nil {
			leased++
		}
	}
	assert.Equal(t, 1, leased)
	var starts int
	require.NoError(t, f.s.r.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM fetch_starts`).Scan(&starts))
	assert.Equal(t, 1, starts)
}

func TestFetchStartsSurviveJobDelete(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	j := f.urlJob(f.pubID, "youtube.com")
	f.urlJob(f.pubID, "youtube.com")
	require.NotNil(t, f.claimFetch(d))
	_, err := f.s.DeleteJob(f.ctx, j.ID)
	require.NoError(t, err)
	assert.Nil(t, f.claimFetch(d), "the start still counts")
	f.clk.Advance(3 * time.Minute)
	require.NotNil(t, f.claimFetch(d))
}

func TestFetchStartsPruned(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	old := toMS(f.clk.Now().Add(-fetchStartsKeep - time.Hour))
	_, err := f.s.w.ExecContext(f.ctx, `INSERT INTO fetch_starts (egress_key, site, started_at) VALUES ('e9', 'x.com', ?)`, old)
	require.NoError(t, err)
	f.urlJob(f.pubID, "example.com")
	require.NotNil(t, f.claimFetch(d))
	var n int
	require.NoError(t, f.s.r.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM fetch_starts`).Scan(&n))
	assert.Equal(t, 1, n)
}

func TestFetchPoliciesFailClosed(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	// An invalid row blocks its site (no panic on a 0/1s rule).
	_, err := f.s.w.ExecContext(f.ctx, `INSERT INTO fetch_policies (key, site, egress_key, rules, concurrency,
		backoff_initial_s, backoff_cap_s, updated_by, updated_at) VALUES
		('example.com', 'example.com', NULL, '["0/1s"]', 1, 60, 60, 'x', 0),
		('bad.com', 'bad.com', NULL, 'not json', 1, 60, 60, 'x', 0)`)
	require.NoError(t, err)
	f.urlJob(f.pubID, "example.com")
	f.urlJob(f.pubID, "bad.com")
	assert.Nil(t, f.claimFetch(d))
	bad, err := f.s.InvalidFetchPolicies(f.ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"bad.com", "example.com"}, bad)
	f.urlJob(f.pubID, "fine.com")
	require.NotNil(t, f.claimFetch(d))

	_, err = f.s.w.ExecContext(f.ctx, `DELETE FROM fetch_policies WHERE key = '*'`)
	require.NoError(t, err)
	f.urlJob(f.pubID, "other.com")
	assert.Nil(t, f.claimFetch(d), "no policy applies: blocked")
	_, ok, err := f.s.NextFetchEligible(f.ctx, f.clk.Now())
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestNextFetchEligible(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	next := func() (time.Time, bool) {
		at, ok, err := f.s.NextFetchEligible(f.ctx, f.clk.Now())
		require.NoError(t, err)
		return at, ok
	}
	_, ok := next()
	assert.False(t, ok, "nothing pending")

	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	f.downloader(api.SandboxFull, "e2", f.pubID)
	t0 := f.clk.Now()
	f.urlJob(f.pubID, "youtube.com")
	f.urlJob(f.pubID, "youtube.com")
	_, ok = next()
	assert.False(t, ok, "eligible now")
	l := f.claimFetch(d)
	require.NotNil(t, l)
	f.acceptFetch(l, "a")
	f.clk.Advance(time.Minute)
	at, ok := next()
	require.True(t, ok, "e1 is paced; e2 being free does not matter")
	assert.Equal(t, t0.Add(3*time.Minute), at)

	// A later cooldown moves it.
	_, err := f.s.w.ExecContext(f.ctx, `INSERT INTO fetch_cooldowns (egress_key, site, until, level, manual, set_by)
		VALUES ('e1', 'youtube.com', ?, 0, 1, 'user:1')`, toMS(t0.Add(time.Hour)))
	require.NoError(t, err)
	at, ok = next()
	require.True(t, ok)
	assert.Equal(t, t0.Add(time.Hour), at)

	f.clk.Advance(time.Hour)
	_, ok = next()
	assert.False(t, ok)
}

func TestFetchBlockedTasks(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	blocked := func() map[string]int {
		m, err := f.s.FetchBlockedTasks(f.ctx, f.clk.Now())
		require.NoError(t, err)
		return m
	}
	f.urlJob(f.pubID, "youtube.com")
	assert.Empty(t, blocked(), "no downloader: waiting for one, not for pacing")
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	f.urlJob(f.pubID, "youtube.com")
	f.urlJob(f.pubID, "example.com")
	assert.Empty(t, blocked())
	l := f.claimFetch(d)
	require.NotNil(t, l)
	f.acceptFetch(l, "a")
	// youtube.com: 1 per 3m from e1; example.com (the * policy) is free.
	assert.Equal(t, map[string]int{"youtube.com": 1}, blocked())
	f.downloader(api.SandboxFull, "e2", f.pubID)
	assert.Empty(t, blocked(), "e2 may fetch it")
}

func TestFleetCapacityDownloadersDoNotProcess(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	both := []api.TaskKind{api.KindPrepare, api.KindTranscribe}
	f := api.WorkerFacts{Slots: 2, Kinds: both, SandboxLevel: api.SandboxFull, Kind: api.WorkerDownloader}
	c := FleetCapacity([]Worker{{State: api.WorkerActive, LastSeenAt: &now, Kind: api.WorkerDownloader, Facts: f}},
		now, time.Minute, api.SandboxFull)
	assert.Zero(t, c.Transcribe.Total)
	assert.Zero(t, c.Prepare.Total)
	assert.False(t, CanPrepare(f, api.SandboxNone))
}

// TestHeartbeatFetchLease: a fetch lease is kept alive by the fetch grant
// (a downloader holds no process grant).
func TestHeartbeatFetchLease(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	f.urlJob(f.pubID, "example.com")
	l := f.claimFetch(d)
	require.NotNil(t, l)
	_, err := f.s.Heartbeat(f.ctx, l.ID, d, ttl, LeaseAgeLimit{})
	require.NoError(t, err)
}

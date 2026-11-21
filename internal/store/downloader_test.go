package store

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
)

func TestRegisterDownloaderKind(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d, err := f.s.RegisterWorker(f.ctx, f.token(), api.WorkerFacts{Hostname: "d", Kind: api.WorkerDownloader,
		YtDlpVersion: "2026.1.1", EgressGroup: "home", EgressIP: "203.0.113.7"})
	require.NoError(t, err)
	p, err := f.s.RegisterWorker(f.ctx, f.token(), api.WorkerFacts{Hostname: "p"})
	require.NoError(t, err)

	got, err := f.s.WorkerByID(f.ctx, d.ID)
	require.NoError(t, err)
	assert.Equal(t, api.WorkerDownloader, got.Kind)
	assert.Equal(t, api.WorkerDownloader, got.Facts.Kind)
	assert.Equal(t, "unknown", got.EgressKey)
	assert.Equal(t, [3]string{"2026.1.1", "home", "203.0.113.7"},
		[3]string{got.Facts.YtDlpVersion, got.Facts.EgressGroup, got.Facts.EgressIP})
	got, err = f.s.WorkerByID(f.ctx, p.ID)
	require.NoError(t, err)
	assert.Equal(t, api.WorkerProcessing, got.Kind)

	require.NoError(t, f.s.SetWorkerEgressKey(f.ctx, d.ID, "grp:home"))
	require.NoError(t, f.s.UpdateWorkerFacts(f.ctx, d.ID, api.WorkerFacts{Hostname: "d", Kind: api.WorkerDownloader,
		YtDlpVersion: "2026.2.2", EgressGroup: "home", EgressIP: "203.0.113.8"}))
	got, err = f.s.WorkerByID(f.ctx, d.ID)
	require.NoError(t, err)
	assert.Equal(t, "grp:home", got.EgressKey)
	assert.Equal(t, [2]string{"2026.2.2", "203.0.113.8"}, [2]string{got.Facts.YtDlpVersion, got.Facts.EgressIP})
}

func TestWorkerKindImmutable(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1")
	w := f.worker(api.SandboxFull)
	assert.ErrorIs(t, f.s.UpdateWorkerFacts(f.ctx, d, api.WorkerFacts{Hostname: "d"}), ErrConflict)
	assert.ErrorIs(t, f.s.UpdateWorkerFacts(f.ctx, d, api.WorkerFacts{Hostname: "d", Kind: api.WorkerProcessing}), ErrConflict)
	assert.ErrorIs(t, f.s.UpdateWorkerFacts(f.ctx, w, api.WorkerFacts{Hostname: "h", Kind: api.WorkerDownloader}), ErrConflict)
	require.NoError(t, f.s.UpdateWorkerFacts(f.ctx, w, api.WorkerFacts{Hostname: "h2"}))
	got, err := f.s.WorkerByID(f.ctx, d)
	require.NoError(t, err)
	assert.Equal(t, api.WorkerDownloader, got.Kind)
}

func TestQueueHasDownloader(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	has := func(min api.SandboxLevel) bool {
		ok, err := f.s.QueueHasDownloader(f.ctx, f.pubID, min)
		require.NoError(t, err)
		return ok
	}
	assert.False(t, has(api.SandboxFull))

	// Pending downloader with a grant: not eligible.
	pend, err := f.s.RegisterWorker(f.ctx, f.token(), api.WorkerFacts{Hostname: "d", SandboxLevel: api.SandboxFull,
		Kind: api.WorkerDownloader})
	require.NoError(t, err)
	require.NoError(t, f.s.AddGrant(f.ctx, pend.Principal(), api.RoleFetch, QueueScope(f.pubID), "user:1"))
	assert.False(t, has(api.SandboxFull), "pending")

	// A processing worker holding process: not a downloader.
	f.worker(api.SandboxFull, f.pubID)
	assert.False(t, has(api.SandboxNone), "processing worker")

	// Active below the floor.
	d := f.downloader(api.SandboxFSOnly, "e1", f.pubID)
	assert.False(t, has(api.SandboxFull), "below floor")
	assert.True(t, has(api.SandboxFSOnly))

	// Paused at the floor counts.
	_, err = f.s.SetWorkerState(f.ctx, d, api.WorkerPaused, "user:1")
	require.NoError(t, err)
	assert.True(t, has(api.SandboxFSOnly), "paused")
	_, err = f.s.SetWorkerState(f.ctx, d, api.WorkerRejected, "user:1")
	require.NoError(t, err)
	assert.False(t, has(api.SandboxFSOnly), "rejected")

	// A wildcard fetch grant covers every queue.
	w := f.downloader(api.SandboxFull, "e2")
	require.NoError(t, f.s.AddGrant(f.ctx, WorkerPrincipal(w), api.RoleFetch, "*", "user:1"))
	assert.True(t, has(api.SandboxFull), "wildcard")
}

func TestRevokeFetchCancelsFetchLeasesOnly(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	other := f.queue("other", 1)
	// A downloader that also (artificially) holds process, to prove the two
	// revocations are separate.
	d := f.downloader(api.SandboxFull, "e1", f.pubID, other)
	require.NoError(t, f.s.AddGrant(f.ctx, WorkerPrincipal(d), api.RoleProcess, QueueScope(f.pubID), "user:1"))
	f.fetchingJob(f.pubID, "j1")
	f.fetchingJob(other, "j2")
	l1, l2 := f.fetchLease("j1", d), f.fetchLease("j2", d)
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO jobs (id, queue_id, client_id, state, model, language, created_at, updated_at)
			VALUES ('jp', ?, 'c', 'running', 'm', 'en', 1, 1)`, []any{f.pubID}},
		{`INSERT INTO tasks (job_id, seq, kind, cost, state) VALUES ('jp', 0, 'prepare', 1, 'leased')`, nil},
		{`INSERT INTO leases (id, job_id, seq, worker_id, created_at, expires_at) VALUES ('lp', 'jp', 0, ?, 1, ?)`,
			[]any{d, toMS(f.clk.Now().Add(time.Hour))}},
	} {
		_, err := f.s.w.ExecContext(f.ctx, q.sql, q.args...)
		require.NoError(t, err)
	}
	// Another downloader keeps both queues served, so no job is orphaned.
	f.downloader(api.SandboxFull, "e2", f.pubID, other)

	ended, err := f.s.RemoveGrant(f.ctx, WorkerPrincipal(d), api.RoleFetch, QueueScope(f.pubID))
	require.NoError(t, err)
	assert.Equal(t, []string{l1}, ended, "only the fetch lease on the revoked queue")
	live := func(id string) bool {
		var ended sql.NullInt64
		require.NoError(t, f.s.r.QueryRowContext(f.ctx, `SELECT ended_at FROM leases WHERE id = ?`, id).Scan(&ended))
		return !ended.Valid
	}
	assert.False(t, live(l1))
	assert.True(t, live(l2), "fetch lease on the other queue stays live")
	assert.True(t, live("lp"), "process lease stays live")

	// Revoking process leaves fetch leases alone.
	ended, err = f.s.RemoveGrant(f.ctx, WorkerPrincipal(d), api.RoleProcess, QueueScope(f.pubID))
	require.NoError(t, err)
	assert.Equal(t, []string{"lp"}, ended)
	assert.False(t, live("lp"))
	assert.True(t, live(l2))
}

func TestRevokeLastFetchFailsFetchingJobs(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	f.fetchingJob(f.pubID, "j1")
	f.fetchLease("j1", d)

	_, err := f.s.RemoveGrant(f.ctx, WorkerPrincipal(d), api.RoleFetch, QueueScope(f.pubID))
	require.NoError(t, err)
	j, err := f.s.Job(f.ctx, "j1")
	require.NoError(t, err)
	assert.Equal(t, api.JobFailed, j.State)
	assert.Equal(t, "no downloader holds fetch on queue public", j.Error)
	var tstate string
	require.NoError(t, f.s.r.QueryRowContext(f.ctx, `SELECT state FROM tasks WHERE job_id = 'j1'`).Scan(&tstate))
	assert.Equal(t, "cancelled", tstate)

	// With a downloader remaining, nothing fails.
	f.downloader(api.SandboxFull, "e2", f.pubID)
	d2 := f.downloader(api.SandboxFull, "e3", f.pubID)
	f.fetchingJob(f.pubID, "j2")
	_, err = f.s.RemoveGrant(f.ctx, WorkerPrincipal(d2), api.RoleFetch, QueueScope(f.pubID))
	require.NoError(t, err)
	j, err = f.s.Job(f.ctx, "j2")
	require.NoError(t, err)
	assert.Equal(t, api.JobFetching, j.State)
}

func TestRejectLastDownloaderFailsFetchingJobs(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	f.fetchingJob(f.pubID, "j1")
	l := f.fetchLease("j1", d)

	cancelled, err := f.s.SetWorkerState(f.ctx, d, api.WorkerRejected, "user:1")
	require.NoError(t, err)
	assert.Equal(t, []string{l}, cancelled)
	j, err := f.s.Job(f.ctx, "j1")
	require.NoError(t, err)
	assert.Equal(t, api.JobFailed, j.State)
	assert.Equal(t, "no downloader holds fetch on queue public", j.Error)

	// Direct call: a below-floor downloader does not save a job.
	f.downloader(api.SandboxFSOnly, "e2", f.pubID)
	f.fetchingJob(f.pubID, "j2")
	var ids []string
	require.NoError(t, f.s.InTx(f.ctx, func(tx *Tx) (err error) {
		ids, err = tx.FailOrphanedFetchJobs(f.ctx, f.pubID, api.SandboxFull)
		return
	}))
	assert.Equal(t, []string{"j2"}, ids)
}

func TestRevokeWildcardFetchFailsFetchingJobs(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1")
	require.NoError(t, f.s.AddGrant(f.ctx, WorkerPrincipal(d), api.RoleFetch, "*", "user:1"))
	other := f.queue("other", 1)
	f.fetchingJob(f.pubID, "j1")
	f.fetchingJob(other, "j2")
	var rev Revocation
	require.NoError(t, f.s.InTx(f.ctx, func(tx *Tx) (err error) {
		rev, err = tx.RevokeGrant(f.ctx, WorkerPrincipal(d), api.RoleFetch, "*")
		return
	}))
	assert.Equal(t, []string{"j1", "j2"}, rev.Orphaned.Jobs)
	for _, id := range []string{"j1", "j2"} {
		j, err := f.s.Job(f.ctx, id)
		require.NoError(t, err)
		assert.Equal(t, api.JobFailed, j.State, id)
	}
}

// Failing an orphaned job ends the live fetch lease of a downloader that is
// ineligible (below the floor), and reports it apart from the revoked
// worker's own cancelled leases.
func TestOrphanFailureEndsBelowFloorDownloaderLease(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	good := f.downloader(api.SandboxFull, "e1", f.pubID)
	weak := f.downloader(api.SandboxNone, "e2", f.pubID)
	f.fetchingJob(f.pubID, "j1")
	lw := f.fetchLease("j1", weak)

	var rev Revocation
	require.NoError(t, f.s.InTx(f.ctx, func(tx *Tx) (err error) {
		rev, err = tx.RevokeGrant(f.ctx, WorkerPrincipal(good), api.RoleFetch, QueueScope(f.pubID))
		return
	}))
	assert.Empty(t, rev.Cancelled, "the revoked worker held no lease")
	assert.Equal(t, []string{"j1"}, rev.Orphaned.Jobs)
	assert.Equal(t, []string{lw}, rev.Orphaned.Leases)
	var ended sql.NullInt64
	require.NoError(t, f.s.r.QueryRowContext(f.ctx, `SELECT ended_at FROM leases WHERE id = ?`, lw).Scan(&ended))
	assert.True(t, ended.Valid)
}

func TestFactsBelowFloorFailsFetchingJobs(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	f.fetchingJob(f.pubID, "j1")
	facts := api.WorkerFacts{Hostname: "d", Kind: api.WorkerDownloader, SandboxLevel: api.SandboxFull}
	var o OrphanFailures
	update := func() {
		require.NoError(t, f.s.InTx(f.ctx, func(tx *Tx) (err error) {
			o, err = tx.UpdateWorkerFacts(f.ctx, d, facts)
			return
		}))
	}
	update()
	assert.Empty(t, o.Jobs, "still at the floor")
	facts.SandboxLevel = api.SandboxFSOnly
	update()
	assert.Equal(t, []string{"j1"}, o.Jobs)
	j, err := f.s.Job(f.ctx, "j1")
	require.NoError(t, err)
	assert.Equal(t, api.JobFailed, j.State)
}

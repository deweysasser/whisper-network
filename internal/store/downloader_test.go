package store

import (
	"testing"

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
	f.s.SetWorkerState(f.ctx, d, api.WorkerPaused, "user:1")
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
	f.s.w.ExecContext(f.ctx, `INSERT INTO jobs (id, queue_id, client_id, state, model, language, created_at, updated_at)
		VALUES ('jp', ?, 'c', 'running', 'm', 'en', 1, 1)`, f.pubID)
	f.s.w.ExecContext(f.ctx, `INSERT INTO tasks (job_id, seq, kind, cost, state) VALUES ('jp', 0, 'prepare', 1, 'leased')`)
	f.s.w.ExecContext(f.ctx, `INSERT INTO leases (id, job_id, seq, worker_id, created_at, expires_at) VALUES ('lp', 'jp', 0, ?, 1, ?)`,
		d, toMS(f.clk.Now().Add(1e12)))
	// Another downloader keeps both queues served, so no job is orphaned.
	f.downloader(api.SandboxFull, "e2", f.pubID, other)

	ended, err := f.s.RemoveGrant(f.ctx, WorkerPrincipal(d), api.RoleFetch, QueueScope(f.pubID))
	require.NoError(t, err)
	assert.Equal(t, []string{l1}, ended, "only the fetch lease on the revoked queue")
	var open int
	require.NoError(t, f.s.r.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM leases WHERE ended_at IS NULL`).Scan(&open))
	assert.Equal(t, 2, open, "l2 and lp stay live")
	_ = l2

	// Revoking process leaves fetch leases alone.
	ended, err = f.s.RemoveGrant(f.ctx, WorkerPrincipal(d), api.RoleProcess, QueueScope(f.pubID))
	require.NoError(t, err)
	assert.Equal(t, []string{"lp"}, ended)
	require.NoError(t, f.s.r.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM leases WHERE ended_at IS NULL`).Scan(&open))
	assert.Equal(t, 1, open)
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

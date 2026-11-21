package store

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
)

// failedJob makes a five-task job failed (see failingJob): task 1 failed on
// worker a, 2 done (b), 3 failed (c, non-retryable), 4 and 5 cancelled.
func (f *fixture) failedJob() (j *Job, a, c string) {
	f.t.Helper()
	j, a, lb, lc := f.failingJob()
	f.complete(lb)
	end := f.fail(lc, false)
	require.True(f.t, end.JobFailed)
	got, err := f.s.Job(f.ctx, j.ID)
	require.NoError(f.t, err)
	require.Equal(f.t, api.JobFailed, got.State)
	return got, a, lc.WorkerID
}

// TestRetryJob: a retry keeps the done task and its result, re-queues the
// failed and cancelled ones with a fresh attempt budget, lets the workers
// that failed them take them again, clears the error and counts the retry;
// the job then completes (#56, #62).
func TestRetryJob(t *testing.T) {
	t.Parallel()
	f := newFixture(t, maxAttempts(1))
	j, a, c := f.failedJob()
	assert.True(t, j.Retryable())
	f.clk.Advance(time.Minute)

	r, err := f.s.RetryJob(f.ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, &Retried{PreviousError: j.Error, State: api.JobQueued, Tasks: 4, Retries: 1}, r)

	got, err := f.s.Job(f.ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, api.JobQueued, got.State)
	assert.Empty(t, got.Error)
	assert.Nil(t, got.CompletedAt)
	assert.Equal(t, 1, got.Retries)
	assert.False(t, got.Retryable(), "only failed jobs")
	assert.Equal(t, api.TaskCounts{Total: 5, Pending: 4, Done: 1}, got.Tasks)
	for seq := 1; seq <= 5; seq++ {
		state, attempts := f.task(j.ID, seq)
		if seq == 2 {
			assert.Equal(t, "done", state)
			continue
		}
		assert.Equal(t, [2]any{"pending", 0}, [2]any{state, attempts}, "task %d", seq)
	}

	// The exclusions are cleared: a, which failed task 1, takes it again,
	// and c, which failed task 3, takes that one.
	ex, err := f.s.RetryExclusion(f.ctx, j.ID, 1, a)
	require.NoError(t, err)
	assert.Empty(t, ex)
	la := f.claim(a)
	require.NotNil(t, la)
	assert.Equal(t, 1, la.Seq)
	assert.Equal(t, 1, la.Attempt, "a fresh attempt budget")
	lc := f.claim(c)
	require.NotNil(t, lc)
	assert.Equal(t, 3, lc.Seq)
	f.complete(la)
	f.complete(lc)
	f.complete(f.claim(a))
	assert.True(t, f.complete(f.claim(a)), "the job completes")

	// The leases of the first run stay for history.
	var old int
	require.NoError(t, f.s.r.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM leases WHERE job_id = ? AND retry = 0`,
		j.ID).Scan(&old))
	assert.Equal(t, 3, old)
}

// TestRetryCountsOnlyNewFailures: after a retry a task's attempts and the
// stranded-task rule count only failures since the retry.
func TestRetryCountsOnlyNewFailures(t *testing.T) {
	t.Parallel()
	f := newFixture(t, maxAttempts(1))
	j, a, _ := f.failedJob()
	_, err := f.s.RetryJob(f.ctx, j.ID)
	require.NoError(t, err)

	end := f.fail(f.claim(a), true)
	assert.Equal(t, 1, end.Attempt, "the old failure on a is not counted again")
	assert.False(t, end.Requeued, "max_attempts 1 is spent again")
	got, err := f.s.Job(f.ctx, j.ID)
	require.NoError(t, err)
	assert.Contains(t, got.Error, "transcribe task 1 failed after 1 attempt(s) (on worker "+a+")")
}

// TestRetryJobRefused: only failed jobs with their media retry; a refusal
// changes nothing.
func TestRetryJobRefused(t *testing.T) {
	t.Parallel()
	f := newFixture(t, maxAttempts(1))

	j, _, _ := f.failedJob()
	live := f.job(f.pubID, "c", api.WeightMedium, 1, 10)
	_, err := f.s.RetryJob(f.ctx, live.ID)
	why, ok := IsNotRetryable(err)
	require.True(t, ok, "%v", err)
	assert.Contains(t, why, "job is queued")
	require.ErrorIs(t, err, ErrConflict)

	require.NoError(t, f.s.MarkCanonicalPurged(f.ctx, j.ID))
	got, err := f.s.Job(f.ctx, j.ID)
	require.NoError(t, err)
	assert.True(t, got.API(f.clk.Now()).Media.AudioPurged)
	assert.False(t, got.Retryable())
	_, err = f.s.RetryJob(f.ctx, j.ID)
	why, ok = IsNotRetryable(err)
	require.True(t, ok)
	assert.Contains(t, why, "audio has been purged")
	after, err := f.s.Job(f.ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, got, after, "nothing changed")

	_, err = f.s.RetryJob(f.ctx, "nope")
	require.ErrorIs(t, err, ErrNotFound)
}

// TestRetryPrepareFailed: a job whose prepare failed goes back to
// preparing, unless its original was purged.
func TestRetryPrepareFailed(t *testing.T) {
	t.Parallel()
	f := newFixture(t, maxAttempts(1))
	w := f.worker(api.SandboxFull, f.pubID)
	j := f.prepJob(f.pubID, "c")
	require.True(t, f.fail(f.claim(w), true).JobFailed)

	r, err := f.s.RetryJob(f.ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, api.JobPreparing, r.State)
	l := f.claim(w)
	require.NotNil(t, l, "the worker that failed the prepare may take it again")
	assert.Equal(t, api.KindPrepare, l.Kind)

	require.True(t, f.fail(l, true).JobFailed)
	require.NoError(t, f.s.MarkOriginalPurged(f.ctx, j.ID))
	got, err := f.s.Job(f.ctx, j.ID)
	require.NoError(t, err)
	assert.True(t, got.API(f.clk.Now()).Media.AudioPurged, "nothing left to work from")
	_, err = f.s.RetryJob(f.ctx, j.ID)
	why, ok := IsNotRetryable(err)
	require.True(t, ok)
	assert.Contains(t, why, "original upload has been purged")
}

// TestFailedJobMedia lists failed jobs' media oldest failure first and
// sums the gauges.
func TestFailedJobMedia(t *testing.T) {
	t.Parallel()
	f := newFixture(t, maxAttempts(1))
	w := f.worker(api.SandboxFull, f.pubID)
	first := f.prepJob(f.pubID, "c")
	require.True(t, f.fail(f.claim(w), true).JobFailed)
	f.clk.Advance(time.Minute)
	second := f.job(f.pubID, "c", api.WeightMedium, 1, 10)
	require.True(t, f.fail(f.claim(f.worker(api.SandboxFull, f.pubID)), true).JobFailed)
	f.job(f.pubID, "c", api.WeightMedium, 1, 10) // live: never listed

	ms, err := f.s.FailedJobMedia(f.ctx)
	require.NoError(t, err)
	assert.Equal(t, []FailedMedia{
		{JobID: first.ID, Original: true, Bytes: 1000},
		{JobID: second.ID, Original: true, Canonical: true, Bytes: 200},
	}, ms)
	st, err := f.s.FailedJobStats(f.ctx)
	require.NoError(t, err)
	assert.Equal(t, FailedStats{Bytes: 1200, Retryable: 2}, st)

	require.NoError(t, f.s.MarkCanonicalPurged(f.ctx, second.ID))
	st, err = f.s.FailedJobStats(f.ctx)
	require.NoError(t, err)
	assert.Equal(t, FailedStats{Bytes: 1100, Retryable: 1}, st, "its original alone does not make it retryable")
}

// sortFixture creates jobs in every state, one minute apart, and returns
// their ids by name.
func sortFixture(t *testing.T) (*fixture, map[string]string) {
	f := newFixture(t, maxAttempts(1))
	ids := map[string]string{}
	w := f.worker(api.SandboxFull, f.pubID)
	mk := func(name string) *Job {
		f.clk.Advance(10 * time.Second)
		j := f.job(f.pubID, "c", api.WeightMedium, 1, 10)
		ids[name] = j.ID
		return j
	}
	// done1 finishes after done2 although created first.
	mk("done1")
	l1 := f.claim(w)
	done := func(l *Lease) {
		require.True(t, f.complete(l))
		_, err := f.s.MarkJobDone(f.ctx, l.JobID)
		require.NoError(t, err)
	}
	mk("done2")
	done(f.claim(w))
	f.clk.Advance(10 * time.Second)
	done(l1)
	mk("failed1")
	f.fail(f.claim(w), true)
	mk("failed2")
	f.fail(f.claim(f.worker(api.SandboxFull, f.pubID)), true)
	mk("run1")
	mk("run2")
	f.claim(w)
	f.claim(w)
	mk("queued1")
	f.clk.Advance(10 * time.Second)
	ids["prep1"] = f.prepJob(f.pubID, "c").ID
	mk("queued2")
	return f, ids
}

func listAll(t *testing.T, f *fixture, filt JobFilter, ids map[string]string) []string {
	t.Helper()
	names := map[string]string{}
	for k, v := range ids {
		names[v] = k
	}
	var out []string
	for range 100 {
		page, next, err := f.s.ListJobs(f.ctx, filt)
		require.NoError(t, err)
		for _, j := range page {
			out = append(out, names[j.ID])
		}
		if next == "" {
			return out
		}
		filt.Cursor = next
	}
	t.Fatal("paging did not end")
	return nil
}

// TestListJobsSort: the state order and the creation order both ways,
// paged two at a time, alone and with a state filter (#67).
func TestListJobsSort(t *testing.T) {
	t.Parallel()
	f, ids := sortFixture(t)
	base := JobFilter{QueueID: f.pubID, Limit: 2}

	state := base
	state.Sort = SortState
	assert.Equal(t, []string{"run1", "run2", "queued1", "prep1", "queued2", "failed2", "failed1", "done1", "done2"},
		listAll(t, f, state, ids), "running, queued/preparing oldest first, failed and done newest outcome first")

	all := []string{"done1", "done2", "failed1", "failed2", "run1", "run2", "queued1", "prep1", "queued2"}
	assert.Equal(t, all, listAll(t, f, base, ids), "the default is unchanged: oldest first")
	asc := base
	asc.Sort = SortCreated
	assert.Equal(t, all, listAll(t, f, asc, ids))
	desc := asc
	desc.Desc = true
	rev := make([]string, len(all))
	for i, n := range all {
		rev[len(all)-1-i] = n
	}
	assert.Equal(t, rev, listAll(t, f, desc, ids))

	state.States = []api.JobState{api.JobFailed, api.JobQueued}
	assert.Equal(t, []string{"queued1", "queued2", "failed2", "failed1"}, listAll(t, f, state, ids))
	desc.States = []api.JobState{api.JobDone}
	assert.Equal(t, []string{"done2", "done1"}, listAll(t, f, desc, ids))
	n, err := f.s.CountJobs(f.ctx, state)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
}

// TestListJobsSortCursor: a cursor works only with its own order, and
// paging under the state order neither skips nor repeats jobs that keep
// their state; a job that changes group between pages moves with it
// (documented on ListJobs).
func TestListJobsSortCursor(t *testing.T) {
	t.Parallel()
	f, ids := sortFixture(t)
	filt := JobFilter{QueueID: f.pubID, Limit: 3, Sort: SortState}
	page, next, err := f.s.ListJobs(f.ctx, filt)
	require.NoError(t, err)
	require.Len(t, page, 3)
	require.NotEmpty(t, next)

	for _, other := range []JobFilter{{QueueID: f.pubID}, {QueueID: f.pubID, Desc: true}} {
		other.Cursor = next
		_, _, err := f.s.ListJobs(f.ctx, other)
		require.ErrorIs(t, err, ErrInvalid, "a state cursor in another order")
	}
	_, _, err = f.s.ListJobs(f.ctx, JobFilter{QueueID: f.pubID, Sort: "size"})
	require.ErrorIs(t, err, ErrInvalid)

	// queued2 (not yet listed) fails between the pages: it moves into the
	// failed group, which is still ahead, and is listed once.
	_, err = f.s.FailJob(f.ctx, ids["queued2"], "operator")
	require.NoError(t, err)
	filt.Cursor = next
	rest := listAll(t, f, filt, ids)
	assert.Equal(t, []string{"prep1", "queued2", "failed2", "failed1", "done1", "done2"}, rest)
}

// TestListJobsStateOrderUsesIndex: the state order's page is read from the
// jobs_queue_state_order index, not sorted.
func TestListJobsStateOrderUsesIndex(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	q := `EXPLAIN QUERY PLAN SELECT j.id FROM jobs j WHERE j.queue_id = ? AND (` + jobStateRankSQL + `, ` +
		jobStateKeySQL + `, j.id) > (?, ?, ?) ORDER BY ` + jobStateRankSQL + `, ` + jobStateKeySQL + `, j.id LIMIT 10`
	rows, err := f.s.r.QueryContext(f.ctx, q, f.pubID, 0, 0, "")
	require.NoError(t, err)
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		plan = append(plan, detail)
	}
	require.NoError(t, rows.Err())
	joined := strings.Join(plan, "\n")
	assert.Contains(t, joined, "jobs_queue_state_order", joined)
	assert.NotContains(t, joined, "TEMP B-TREE", joined)
}

// failedURLJob makes a URL job failed at its fetch (never fetched).
func (f *fixture) failedURLJob(d string) *Job {
	f.t.Helper()
	j := f.urlJob(f.pubID, "example.com")
	l := f.claimFetch(d)
	require.NotNil(f.t, l)
	require.True(f.t, f.fail(l, false).JobFailed)
	got, err := f.s.Job(f.ctx, j.ID)
	require.NoError(f.t, err)
	require.Equal(f.t, api.JobFailed, got.State)
	return got
}

// fastURLJob makes a URL job that fetched canonical audio (two transcribe
// tasks), finished one and failed the other.
func (f *fixture) fastURLJob(d string) *Job {
	f.t.Helper()
	j := f.urlJob(f.pubID, "example.com")
	l := f.claimFetch(d)
	require.NoError(f.t, f.s.AcceptFetch(f.ctx, l.ID, d, FetchCompletion{
		Original:  Media{Bytes: 100, SHA256: "o", Type: "audio/flac"},
		Canonical: &Canonical{Bytes: 100, SHA256: "o", Samples: 32000},
		Ranges:    []SampleRange{{0, 16000}, {16000, 32000}}}))
	w := f.worker(api.SandboxFull, f.pubID)
	f.complete(f.claim(w))
	require.True(f.t, f.fail(f.claim(w), false).JobFailed)
	got, err := f.s.Job(f.ctx, j.ID)
	require.NoError(f.t, err)
	require.Equal(f.t, api.JobFailed, got.State)
	return got
}

func (f *fixture) count(q string, args ...any) int {
	f.t.Helper()
	var n int
	require.NoError(f.t, f.s.r.QueryRowContext(f.ctx, q, args...).Scan(&n))
	return n
}

func TestRetryURLJobFetchNeverSucceeded(t *testing.T) {
	t.Parallel()
	f := newFixture(t, maxAttempts(1))
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	j := f.failedURLJob(d)
	assert.True(t, j.Retryable())
	f.clk.Advance(time.Minute)

	r, err := f.s.RetryJob(f.ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, &Retried{PreviousError: j.Error, State: api.JobFetching, Tasks: 1, Retries: 1, Refetch: true}, r)
	got, err := f.s.Job(f.ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, api.JobFetching, got.State)
	assert.Empty(t, got.Error)
	assert.Nil(t, got.CompletedAt)
	assert.Equal(t, 1, got.Retries)
	assert.Equal(t, j.FetchURL, got.FetchURL)
	assert.Equal(t, "example.com", got.FetchSite)
	st, att := f.task(j.ID, FetchSeq)
	assert.Equal(t, [2]any{"pending", 0}, [2]any{st, att})

	// A downloader can claim it again.
	l := f.claimFetch(d)
	require.NotNil(t, l)
	assert.Equal(t, j.ID, l.JobID)
}

func TestRetryURLJobAudioPurgedRefetches(t *testing.T) {
	t.Parallel()
	f := newFixture(t, maxAttempts(1))
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	j := f.fastURLJob(d)
	require.NoError(t, f.s.MarkOriginalPurged(f.ctx, j.ID))
	require.NoError(t, f.s.MarkCanonicalPurged(f.ctx, j.ID))
	got, err := f.s.Job(f.ctx, j.ID)
	require.NoError(t, err)
	assert.True(t, got.Retryable(), "a URL job is retryable after purge")
	assert.Equal(t, 1, f.count(`SELECT COUNT(*) FROM tasks WHERE job_id = ? AND result IS NOT NULL`, j.ID))
	f.clk.Advance(time.Minute)

	r, err := f.s.RetryJob(f.ctx, j.ID)
	require.NoError(t, err)
	assert.True(t, r.Refetch)
	assert.Equal(t, api.JobFetching, r.State)
	assert.Equal(t, 1, r.Retries)
	got, err = f.s.Job(f.ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, api.JobFetching, got.State)
	assert.Nil(t, got.Canonical)
	assert.Nil(t, got.CanonicalPurgedAt)
	assert.Nil(t, got.OriginalPurgedAt)
	assert.Zero(t, got.Original)
	assert.Equal(t, api.TaskCounts{Total: 1, Pending: 1}, got.Tasks)
	assert.Equal(t, 1, f.count(`SELECT COUNT(*) FROM tasks WHERE job_id = ?`, j.ID))
	assert.Equal(t, 0, f.count(`SELECT COUNT(*) FROM tasks WHERE job_id = ? AND result IS NOT NULL`, j.ID))
	st, att := f.task(j.ID, FetchSeq)
	assert.Equal(t, [2]any{"pending", 0}, [2]any{st, att})

	// The re-fetch runs to completion.
	l := f.claimFetch(d)
	require.NotNil(t, l)
	f.acceptFetch(l, "sha2")
	got, err = f.s.Job(f.ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, api.JobPreparing, got.State)
}

func TestRetryURLJobWithMediaReuses(t *testing.T) {
	t.Parallel()
	f := newFixture(t, maxAttempts(1))
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	j := f.fastURLJob(d)
	f.clk.Advance(time.Minute)

	r, err := f.s.RetryJob(f.ctx, j.ID)
	require.NoError(t, err)
	assert.False(t, r.Refetch)
	assert.Equal(t, api.JobQueued, r.State)
	got, err := f.s.Job(f.ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, api.TaskCounts{Total: 3, Pending: 1, Done: 2}, got.Tasks, "fetch and transcribe 1 stay done")
	assert.Equal(t, 1, f.count(`SELECT COUNT(*) FROM tasks WHERE job_id = ? AND result IS NOT NULL`, j.ID))

	// Prepare failed with the original present: back to preparing.
	f = newFixture(t, maxAttempts(1))
	d = f.downloader(api.SandboxFull, "e1", f.pubID)
	j2 := f.urlJob(f.pubID, "example.com")
	l := f.claimFetch(d)
	require.Equal(t, j2.ID, l.JobID)
	f.acceptFetch(l, "sha")
	w := f.worker(api.SandboxFull, f.pubID)
	require.True(t, f.fail(f.claim(w), false).JobFailed)
	r, err = f.s.RetryJob(f.ctx, j2.ID)
	require.NoError(t, err)
	assert.False(t, r.Refetch)
	assert.Equal(t, api.JobPreparing, r.State)
}

func TestURLJobRetryableAfterPurge(t *testing.T) {
	t.Parallel()
	f := newFixture(t, maxAttempts(1))
	d := f.downloader(api.SandboxFull, "e1", f.pubID)
	j := f.fastURLJob(d)
	require.NoError(t, f.s.MarkCanonicalPurged(f.ctx, j.ID))
	got, err := f.s.Job(f.ctx, j.ID)
	require.NoError(t, err)
	assert.Empty(t, got.retryBlocker())
	assert.True(t, got.Retryable())

	// Not failed is still not retryable.
	live := f.urlJob(f.pubID, "example.com")
	_, err = f.s.RetryJob(f.ctx, live.ID)
	_, ok := IsNotRetryable(err)
	assert.True(t, ok)
}

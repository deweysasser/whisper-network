package server

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/blobs"
	"github.com/deweysasser/whisper-network/internal/media"
	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/store"
)

func onQueue(role api.Role, q string) api.Grant { return api.Grant{Role: role, Scope: "queue:" + q} }

// complete uploads a result for l whose one segment reads "seg<seq>".
func (h *harness) complete(l *api.Lease, token string) {
	h.t.Helper()
	resp, b := h.putResult(l, token, result(l, fmt.Sprintf("seg%d", l.Seq)))
	require.Equal(h.t, http.StatusOK, resp.StatusCode, string(b))
}

// failedJob submits seconds of audio as owner and fails its first task on
// a new worker (the harness must use maxAttempts(1)).
func (h *harness) failedJob(owner string, seconds float64) api.Job {
	h.t.Helper()
	wid, wtok := h.activeWorker(api.WorkerFacts{}, "public")
	job := h.submit(owner, "public", api.JobParams{Model: "small.en"}, canonicalWAV(speech(seconds, media.SampleRate)))
	l := h.claim(wid, wtok, "", nil)
	require.NotNil(h.t, l)
	h.fail(l, wtok, "broken worker")
	j := h.job(job.ID)
	require.Equal(h.t, api.JobFailed, j.State)
	return j
}

// transcriptBody is a transcript without its provenance line.
func (h *harness) transcriptBody(id, token string) string {
	h.t.Helper()
	b := string(h.expect("GET", "/job/"+id+"/transcript", token, nil, http.StatusOK, nil))
	_, body, _ := strings.Cut(b, "\n")
	return body
}

// TestRetryJobOverHTTP: the owner retries a failed job; only the tasks that
// did not finish run again, the worker that failed one may take it back,
// and the transcript equals an uninterrupted run's (#56, #62).
func TestRetryJobOverHTTP(t *testing.T) {
	t.Parallel()
	h := newHarness(t, maxAttempts(1))
	owner := h.client(submitOn("public"))
	aID, aTok := h.activeWorker(api.WorkerFacts{}, "public")
	bID, bTok := h.activeWorker(api.WorkerFacts{}, "public")
	audio := canonicalWAV(speech(10, media.SampleRate))

	// The reference: the same audio, uninterrupted.
	ref := h.submit(owner, "public", api.JobParams{Model: "small.en"}, audio)
	for range ref.Tasks.Total {
		h.complete(h.claim(aID, aTok, "", nil), aTok)
	}
	require.Equal(t, api.JobDone, h.job(ref.ID).State)

	job := h.submit(owner, "public", api.JobParams{Model: "small.en"}, audio)
	require.GreaterOrEqual(t, job.Tasks.Total, 3)
	la := h.claim(aID, aTok, "", nil)
	lb := h.claim(bID, bTok, "", nil)
	h.fail(la, aTok, "broken")
	h.complete(lb, bTok)
	failed := h.job(job.ID)
	require.Equal(t, api.JobFailed, failed.State)
	assert.True(t, failed.Retryable)
	require.NotNil(t, failed.Media.KeptUntil)
	assert.Equal(t, failed.CompletedAt.Add(h.srv.cfg.FailedRetention), *failed.Media.KeptUntil)

	mark := h.auditMark()
	var doc api.Job
	h.expect("POST", "/job/"+job.ID+"/retry", owner, nil, http.StatusOK, &doc)
	assert.Equal(t, api.JobQueued, doc.State)
	assert.Nil(t, doc.Error)
	assert.Nil(t, doc.Media.KeptUntil)
	assert.Equal(t, 1, doc.Retries)
	assert.Equal(t, api.TaskCounts{Total: job.Tasks.Total, Done: 1, Pending: job.Tasks.Total - 1}, doc.Tasks)
	e := h.expectOneAudit(mark, "job.retry", later, "job:"+job.ID)
	var detail map[string]any
	require.NoError(t, json.Unmarshal([]byte(e.Detail), &detail))
	assert.Equal(t, *failed.Error, detail["previous_error"])
	require.Len(t, h.logs.events(t, obs.EventJobRetried), 1)

	resp, b := h.call("POST", "/job/"+job.ID+"/retry", owner, nil)
	h.expectProblem(resp, b, http.StatusConflict, api.ProblemNotRetryable)

	// a failed task 1; it gets it back.
	l := h.claim(aID, aTok, "", nil)
	require.NotNil(t, l)
	assert.Equal(t, 1, l.Seq)
	h.complete(l, aTok)
	for {
		l := h.claim(aID, aTok, "", nil)
		if l == nil {
			break
		}
		h.complete(l, aTok)
	}
	require.Equal(t, api.JobDone, h.job(job.ID).State)
	assert.Equal(t, h.transcriptBody(ref.ID, owner), h.transcriptBody(job.ID, owner))
}

// TestRetryJobAuthorization: the owner, queue_admin on the job's queue and
// admin may retry; a reader without ownership is 403 and a client that
// cannot see the job 404.
func TestRetryJobAuthorization(t *testing.T) {
	t.Parallel()
	h := newHarness(t, maxAttempts(1))
	owner := h.client(submitOn("public"))
	other := h.client(submitOn("public"))
	viewer := h.client(onQueue(api.RoleViewAll, "public"), onQueue(api.RoleReadAll, "public"))
	qa := h.client(onQueue(api.RoleQueueAdmin, "public"))
	h.expect("POST", "/admin/queues", h.admin, api.Queue{Name: "other"}, http.StatusCreated, nil)
	qaOther := h.client(onQueue(api.RoleQueueAdmin, "other"))

	j := h.failedJob(owner, 1)
	resp, b := h.call("POST", "/job/"+j.ID+"/retry", other, nil)
	h.expectProblem(resp, b, http.StatusNotFound, api.ProblemNotFound)
	resp, b = h.call("POST", "/job/"+j.ID+"/retry", qaOther, nil)
	h.expectProblem(resp, b, http.StatusNotFound, api.ProblemNotFound)
	resp, b = h.call("POST", "/job/"+j.ID+"/retry", viewer, nil)
	h.expectProblem(resp, b, http.StatusForbidden, api.ProblemForbidden)
	assert.Equal(t, api.JobFailed, h.job(j.ID).State, "refusals change nothing")

	for _, tok := range []string{owner, qa, h.admin} {
		j := h.failedJob(owner, 1)
		h.expect("POST", "/job/"+j.ID+"/retry", tok, nil, http.StatusOK, nil)
		// Out of the way of the next failedJob's claim.
		h.expect("DELETE", "/job/"+j.ID, h.admin, nil, http.StatusNoContent, nil)
	}
}

// TestRetryAfterPurge: once the failed-job retention purged the media, the
// job is not retryable and a retry is a 409 that changes nothing.
func TestRetryAfterPurge(t *testing.T) {
	t.Parallel()
	h := newHarness(t, maxAttempts(1))
	owner := h.client(submitOn("public"))
	j := h.failedJob(owner, 1)
	h.clk.Advance(h.srv.cfg.FailedRetention)
	h.srv.reapOnce(h.ctx)
	got := h.job(j.ID)
	assert.True(t, got.Media.AudioPurged)
	assert.False(t, got.Retryable)
	assert.Nil(t, got.Media.KeptUntil)

	resp, b := h.call("POST", "/job/"+j.ID+"/retry", owner, nil)
	p := h.expectProblem(resp, b, http.StatusConflict, api.ProblemNotRetryable)
	assert.Contains(t, p.Detail, "purged")
	assert.Equal(t, got.Retries, h.job(j.ID).Retries)
	assert.Equal(t, api.JobFailed, h.job(j.ID).State)
}

// TestRetryPrepareFailedOverHTTP: a job that failed preparing goes back to
// preparing, re-reading its retained original.
func TestRetryPrepareFailedOverHTTP(t *testing.T) {
	t.Parallel()
	h := newHarness(t, maxAttempts(1))
	owner := h.client(submitOn("public"))
	wid, wtok := h.activeWorker(api.WorkerFacts{}, "public")
	job := h.submit(owner, "public", api.JobParams{Model: "small.en"}, []byte("opaque video"))
	l := h.claim(wid, wtok, "", nil)
	require.Equal(t, api.KindPrepare, l.Kind)
	h.fail(l, wtok, "ffmpeg missing")
	require.Equal(t, api.JobFailed, h.job(job.ID).State)
	assert.True(t, h.job(job.ID).Retryable)

	var doc api.Job
	h.expect("POST", "/job/"+job.ID+"/retry", owner, nil, http.StatusOK, &doc)
	assert.Equal(t, api.JobPreparing, doc.State)
	l = h.claim(wid, wtok, "", nil)
	require.NotNil(t, l)
	assert.Equal(t, api.KindPrepare, l.Kind)
	resp, b := h.putFLAC(l, wtok, canonicalFLAC(t, speech(2, media.SampleRate)))
	require.Equal(t, http.StatusOK, resp.StatusCode, string(b))
	assert.Equal(t, api.JobQueued, h.job(job.ID).State)
}

// TestFailedMediaDiskCeiling: above the disk ceiling the reaper purges the
// oldest failed job's media first, never a live job's, and stops once under
// it; nearing the upload guard does the same (#56).
func TestFailedMediaDiskCeiling(t *testing.T) {
	t.Parallel()
	h := newHarness(t, maxAttempts(1), func(c *Config, _ *store.Config) { c.MinFreeBytes = 0 })
	owner := h.client(submitOn("public"))
	old := h.failedJob(owner, 1)
	h.clk.Advance(time.Minute)
	newer := h.failedJob(owner, 1)
	live := h.submit(owner, "public", api.JobParams{Model: "small.en"}, canonicalWAV(speech(1, media.SampleRate)))

	// 80% of 10000 bytes is 8000; 8001 are used.
	h.total.Store(10000)
	h.free.Store(1999)
	h.srv.reapOnce(h.ctx)
	assert.True(t, h.job(old.ID).Media.AudioPurged, "the oldest failed job goes first")
	assert.False(t, h.job(newer.ID).Media.AudioPurged, "one job covered the overage")
	assert.False(t, h.job(live.ID).Media.AudioPurged)
	assert.False(t, h.job(old.ID).Retryable)

	// Far above the ceiling: every failed job's media goes, a live job's never.
	h.free.Store(0)
	h.srv.reapOnce(h.ctx)
	assert.True(t, h.job(newer.ID).Media.AudioPurged)
	assert.False(t, h.job(live.ID).Media.AudioPurged)
	f, err := h.blobs.Open(live.ID, blobs.Canonical)
	require.NoError(t, err)
	_ = f.Close()
}

// TestFailedMediaNearUploadGuard: under twice --min-free-bytes free, failed
// jobs' media is freed first, even below the disk ceiling.
func TestFailedMediaNearUploadGuard(t *testing.T) {
	t.Parallel()
	h := newHarness(t, maxAttempts(1), func(c *Config, _ *store.Config) { c.MinFreeBytes = 1000 })
	owner := h.client(submitOn("public"))
	old := h.failedJob(owner, 1)
	h.clk.Advance(time.Minute)
	newer := h.failedJob(owner, 1)
	h.total.Store(2000) // 25% used: under the ceiling
	h.free.Store(1500)
	h.srv.reapOnce(h.ctx)
	assert.True(t, h.job(old.ID).Media.AudioPurged)
	assert.False(t, h.job(newer.ID).Media.AudioPurged)
}

// TestFailedMediaMetrics: the failed-job storage and retryable gauges.
func TestFailedMediaMetrics(t *testing.T) {
	t.Parallel()
	h := newHarness(t, maxAttempts(1))
	owner := h.client(submitOn("public"))
	h.failedJob(owner, 1)
	h.srv.refreshStats(h.ctx)
	out := h.scrape()
	assert.Contains(t, out, "whisper_failed_jobs_retryable 1")
	assert.Contains(t, out, `whisper_storage_bytes{kind="failed"} `)
	assert.NotContains(t, out, `whisper_storage_bytes{kind="failed"} 0`)
}

// TestPartialTranscript: ?partial=1 assembles an unfinished job's done
// tasks with gaps at the start, middle and end marked, flags it in the
// provenance header, and records the download; without the flag it is 409
// as before; access is as for the full transcript (#66).
func TestPartialTranscript(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	owner := h.client(submitOn("public"))
	other := h.client(submitOn("public"))
	viewer := h.client(onQueue(api.RoleViewAll, "public"))
	reader := h.client(onQueue(api.RoleReadAll, "public"))
	wid, wtok := h.activeWorker(api.WorkerFacts{}, "public")
	job := h.submit(owner, "public", api.JobParams{Model: "small.en"}, canonicalWAV(speech(16, media.SampleRate)))
	n := job.Tasks.Total
	require.GreaterOrEqual(t, n, 5)
	leases := map[int]*api.Lease{}
	for range n {
		l := h.claim(wid, wtok, "", nil)
		leases[l.Seq] = l
	}
	h.complete(leases[2], wtok)
	h.complete(leases[4], wtok)

	resp, b := h.call("GET", "/job/"+job.ID+"/transcript", owner, nil)
	h.expectProblem(resp, b, http.StatusConflict, api.ProblemNotDone)
	resp, b = h.call("GET", "/job/"+job.ID+"/result", owner, nil)
	h.expectProblem(resp, b, http.StatusConflict, api.ProblemNotDone)

	txt := string(h.expect("GET", "/job/"+job.ID+"/transcript?partial=1", owner, nil, http.StatusOK, nil))
	lines := strings.Split(strings.TrimSuffix(txt, "\n"), "\n")
	assert.True(t, strings.HasSuffix(lines[0], fmt.Sprintf("; partial=true; missing=%d/%d", n-2, n)), lines[0])
	var res api.JobResult
	h.expect("GET", "/job/"+job.ID+"/result?partial=1", reader, nil, http.StatusOK, &res)
	assert.True(t, res.Partial)
	require.Len(t, res.Missing, n-2)
	assert.Equal(t, []int{1, 3, 5}, []int{res.Missing[0].Task, res.Missing[1].Task, res.Missing[2].Task})
	assert.Equal(t, int64(0), res.Missing[0].StartMS)
	require.Len(t, res.Segments, 2)

	marker := func(g api.Gap) string { return gapMarker(g) }
	want := []string{marker(res.Missing[0]), "[" + formatTimestamp(res.Segments[0].StartMS) + "] seg2",
		marker(res.Missing[1]), "[" + formatTimestamp(res.Segments[1].StartMS) + "] seg4"}
	for _, g := range res.Missing[2:] {
		want = append(want, marker(g))
	}
	assert.Equal(t, want, lines[1:])
	assert.Regexp(t, `^\[\.\.\. missing 0:00:00-0:00:\d\d \(task 1\) \.\.\.\]$`, lines[1])

	assert.Equal(t, 2, h.job(job.ID).Downloads.Count, "both partial downloads count")

	resp, b = h.call("GET", "/job/"+job.ID+"/transcript?partial=1", other, nil)
	h.expectProblem(resp, b, http.StatusNotFound, api.ProblemNotFound)
	resp, b = h.call("GET", "/job/"+job.ID+"/transcript?partial=1", viewer, nil)
	h.expectProblem(resp, b, http.StatusForbidden, api.ProblemForbidden)

	// A done job's partial request is its full transcript.
	for seq, l := range leases {
		if seq != 2 && seq != 4 {
			h.complete(l, wtok)
		}
	}
	require.Equal(t, api.JobDone, h.job(job.ID).State)
	full := string(h.expect("GET", "/job/"+job.ID+"/transcript?partial=1", owner, nil, http.StatusOK, nil))
	assert.NotContains(t, full, "partial=true")
}

// TestPartialTranscriptPreparing: with nothing transcribed yet there is no
// partial transcript.
func TestPartialTranscriptPreparing(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	owner := h.client(submitOn("public"))
	job := h.submit(owner, "public", api.JobParams{Model: "small.en"}, []byte("opaque video"))
	resp, b := h.call("GET", "/job/"+job.ID+"/transcript?partial=1", owner, nil)
	h.expectProblem(resp, b, http.StatusConflict, api.ProblemNotDone)
}

// TestListJobsSortAPI: GET /queue/{q} takes sort and order; the default is
// unchanged (creation, oldest first).
func TestListJobsSortAPI(t *testing.T) {
	t.Parallel()
	h := newHarness(t, maxAttempts(1))
	owner := h.client(submitOn("public"))
	failed := h.failedJob(owner, 1)
	h.clk.Advance(time.Second)
	queued := h.submit(owner, "public", api.JobParams{Model: "small.en"}, canonicalWAV(speech(1, media.SampleRate)))
	ids := func(q string) []string {
		var l api.JobList
		h.expect("GET", "/queue/public"+q, owner, nil, http.StatusOK, &l)
		var out []string
		for _, j := range l.Jobs {
			out = append(out, j.ID)
		}
		return out
	}
	assert.Equal(t, []string{failed.ID, queued.ID}, ids(""))
	assert.Equal(t, []string{failed.ID, queued.ID}, ids("?sort=created&order=asc"))
	assert.Equal(t, []string{queued.ID, failed.ID}, ids("?sort=created&order=desc"))
	assert.Equal(t, []string{queued.ID, failed.ID}, ids("?sort=state"))
	assert.Equal(t, []string{failed.ID}, ids("?sort=state&state=failed"))
	for _, bad := range []string{"?sort=size", "?order=up", "?sort=state&order=desc"} {
		resp, b := h.call("GET", "/queue/public"+bad, owner, nil)
		h.expectProblem(resp, b, http.StatusBadRequest, api.ProblemBadRequest)
	}
}

func bulk(h *harness, token string, req api.BulkRequest) api.BulkResult {
	h.t.Helper()
	var res api.BulkResult
	h.expect("POST", "/queue/public/bulk", token, req, http.StatusOK, &res)
	return res
}

func outcomes(res api.BulkResult) map[string]string {
	out := map[string]string{}
	for _, j := range res.Jobs {
		out[j.JobID] = j.Outcome
		if j.Reason != "" {
			out[j.JobID] += ": " + j.Reason
		}
	}
	return out
}

// TestBulkRetryMixed: per-job outcomes for a mixed selection, one audit
// entry per changed job plus the summary, and a dry run that changes
// nothing (#68).
func TestBulkRetryMixed(t *testing.T) {
	t.Parallel()
	h := newHarness(t, maxAttempts(1))
	caller := h.client(submitOn("public"), onQueue(api.RoleViewAll, "public"))
	stranger := h.client(submitOn("public"))
	retryable := h.failedJob(caller, 1)
	purged := h.failedJob(caller, 1)
	require.NoError(t, h.srv.purgeCanonical(h.ctx, purged.ID))
	wid, wtok := h.activeWorker(api.WorkerFacts{}, "public")
	done := h.submit(caller, "public", api.JobParams{Model: "small.en"}, canonicalWAV(speech(1, media.SampleRate)))
	h.complete(h.claim(wid, wtok, "", nil), wtok)
	require.Equal(t, api.JobDone, h.job(done.ID).State)
	theirs := h.failedJob(stranger, 1)
	ids := []string{retryable.ID, purged.ID, done.ID, theirs.ID, "job_nope"}

	mark := h.auditMark()
	dry := bulk(h, caller, api.BulkRequest{Action: api.BulkRetry, JobIDs: ids, DryRun: true})
	assert.True(t, dry.DryRun)
	assert.Equal(t, map[string]string{retryable.ID: "would_retry", purged.ID: "skipped: media purged",
		done.ID: "skipped: not failed (done)", theirs.ID: "forbidden", "job_nope": "not_found"}, outcomes(dry))
	assert.Equal(t, mark, h.auditMark(), "a dry run audits nothing")
	assert.Equal(t, api.JobFailed, h.job(retryable.ID).State)

	res := bulk(h, caller, api.BulkRequest{Action: api.BulkRetry, JobIDs: ids})
	assert.Equal(t, map[string]string{retryable.ID: "retried", purged.ID: "skipped: media purged",
		done.ID: "skipped: not failed", theirs.ID: "forbidden", "job_nope": "not_found"}, outcomes(res))
	assert.Equal(t, map[string]int{"retried": 1, "skipped": 2, "forbidden": 1, "not_found": 1}, res.Counts)
	assert.Equal(t, api.JobQueued, h.job(retryable.ID).State)
	assert.Equal(t, api.JobFailed, h.job(theirs.ID).State)

	es, err := h.st.QueryAudit(h.ctx, store.AuditFilter{Limit: 100})
	require.NoError(t, err)
	var actions []string
	for _, e := range es {
		if e.ID > mark {
			actions = append(actions, e.Action+" "+e.Target)
		}
	}
	assert.ElementsMatch(t, []string{"job.retry job:" + retryable.ID, "job.bulk_retry queue:public"}, actions)
}

// TestBulkDeleteAndFilter: delete by filter, the per-request cap with its
// cursor, and since.
func TestBulkDeleteAndFilter(t *testing.T) {
	t.Parallel()
	h := newHarness(t, maxAttempts(1))
	h.srv.bulkLimit = 2
	owner := h.client(submitOn("public"))
	stranger := h.client(submitOn("public"))
	viewer := h.client(onQueue(api.RoleViewAll, "public"))
	var mine []string
	for range 3 {
		mine = append(mine, h.failedJob(owner, 1).ID)
	}
	h.clk.Advance(time.Hour)
	cut := h.clk.Now()
	h.clk.Advance(time.Second)
	newest := h.failedJob(owner, 1)
	theirs := h.failedJob(stranger, 1)

	resp, b := h.call("POST", "/queue/public/bulk", owner, api.BulkRequest{Action: api.BulkRetry,
		JobIDs: []string{"a", "b", "c"}})
	h.expectProblem(resp, b, http.StatusBadRequest, api.ProblemBadRequest)
	resp, b = h.call("POST", "/queue/public/bulk", owner, api.BulkRequest{Action: api.BulkRetry})
	h.expectProblem(resp, b, http.StatusBadRequest, api.ProblemBadRequest)
	resp, b = h.call("POST", "/queue/public/bulk", owner, api.BulkRequest{Action: "nuke",
		Filter: &api.BulkFilter{State: api.JobFailed}})
	h.expectProblem(resp, b, http.StatusBadRequest, api.ProblemBadRequest)

	since := bulk(h, owner, api.BulkRequest{Action: api.BulkRetry, DryRun: true,
		Filter: &api.BulkFilter{State: api.JobFailed, Since: &cut}})
	assert.Equal(t, map[string]string{newest.ID: "would_retry"}, outcomes(since), "only its own, failed since")

	// A viewer sees every failed job but may delete none.
	v := bulk(h, viewer, api.BulkRequest{Action: api.BulkDelete, Filter: &api.BulkFilter{State: api.JobFailed}})
	assert.Equal(t, map[string]int{"forbidden": 2}, v.Counts)
	assert.NotEmpty(t, v.NextCursor)

	first := bulk(h, owner, api.BulkRequest{Action: api.BulkDelete, Filter: &api.BulkFilter{State: api.JobFailed}})
	assert.Equal(t, map[string]string{mine[0]: "deleted", mine[1]: "deleted"}, outcomes(first))
	require.NotEmpty(t, first.NextCursor)
	second := bulk(h, owner, api.BulkRequest{Action: api.BulkDelete,
		Filter: &api.BulkFilter{State: api.JobFailed, Cursor: first.NextCursor}})
	assert.Equal(t, map[string]string{mine[2]: "deleted", newest.ID: "deleted"}, outcomes(second))
	assert.Empty(t, second.NextCursor)
	resp, _ = h.call("GET", "/job/"+mine[0], h.admin, nil)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, api.JobFailed, h.job(theirs.ID).State, "another client's job is untouched")
	es, err := h.st.QueryAudit(h.ctx, store.AuditFilter{ActionPrefix: "job.bulk_delete"})
	require.NoError(t, err)
	assert.Len(t, es, 3)
}

// failedURLJobWithBlobs inserts a failed URL job (through a second
// connection) with blob files for its media and a transcript; original says
// whether its original is still recorded as present.
func (h *harness) failedURLJobWithBlobs(id string, original bool) {
	h.t.Helper()
	h.fetchingJob(id)
	db, err := sql.Open("sqlite", "file:"+filepath.Join(h.dir, "wn.db")+"?_pragma=busy_timeout(5000)")
	require.NoError(h.t, err)
	defer db.Close()
	bytes := 0
	if original {
		bytes = 5
	}
	_, err = db.ExecContext(h.ctx, `UPDATE jobs SET state = 'failed', error = 'boom', completed_at = 2,
		original_bytes = ? WHERE id = ?`, bytes, id)
	require.NoError(h.t, err)
	_, err = db.ExecContext(h.ctx, `UPDATE tasks SET state = 'failed' WHERE job_id = ?`, id)
	require.NoError(h.t, err)
	for _, name := range []string{blobs.Original, blobs.Canonical, blobs.Transcript} {
		require.NoError(h.t, h.blobs.WriteFile(id, name, []byte("x")))
	}
}

// TestRetryRefetchRemovesBlobs: retrying a URL job whose media is gone
// removes its stale blobs and sends it back to fetching; one whose original
// is still present keeps them and is prepared again.
func TestRetryRefetchRemovesBlobs(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	h.failedURLJobWithBlobs("jgone", false)
	assert.True(t, h.job("jgone").Retryable)
	var got api.Job
	h.expect("POST", "/job/jgone/retry", h.admin, nil, http.StatusOK, &got)
	assert.Equal(t, api.JobFetching, got.State)
	_, err := h.blobs.ReadFile("jgone", blobs.Canonical)
	assert.Error(t, err, "stale canonical audio removed")
	_, err = h.blobs.ReadFile("jgone", blobs.Transcript)
	assert.Error(t, err, "stale transcript removed")
	assert.Equal(t, api.JobFetching, h.job("jgone").State)

	h.failedURLJobWithBlobs("jkeep", true)
	h.expect("POST", "/job/jkeep/retry", h.admin, nil, http.StatusOK, &got)
	assert.Equal(t, api.JobPreparing, got.State)
	b, err := h.blobs.ReadFile("jkeep", blobs.Original)
	require.NoError(t, err)
	assert.Equal(t, "x", string(b))
}

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/blobs"
	"github.com/deweysasser/whisper-network/internal/fetchpolicy"
	"github.com/deweysasser/whisper-network/internal/media"
	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/store"
	"github.com/deweysasser/whisper-network/internal/testscale"
)

const (
	ytURL  = "https://www.youtube.com/watch?v=abc123"
	exURL  = "https://media.example.com/talk.wav"
	exSite = "example.com"
)

func fetchKinds() *api.ClaimRequest { return &api.ClaimRequest{Kinds: []api.TaskKind{api.KindFetch}} }

func TestFetchSubmitRequiresSubmitURL(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.downloader("", "", "public")
	plain := h.client(submitOn("public"))
	resp, b := h.fetchCall(plain, "public", api.FetchRequest{JobParams: api.JobParams{Model: "small.en"}, URL: exURL}, "")
	h.expectProblem(resp, b, http.StatusForbidden, api.ProblemForbidden)

	h.expect("POST", "/admin/queues", h.admin, api.Queue{Name: "other"}, http.StatusCreated, nil)
	other := h.client(submitURLOn("other"))
	resp, b = h.fetchCall(other, "public", api.FetchRequest{JobParams: api.JobParams{Model: "small.en"}, URL: exURL}, "")
	h.expectProblem(resp, b, http.StatusForbidden, api.ProblemForbidden)

	ok := h.client(submitURLOn("public"))
	j := h.fetchSubmit(ok, "public", exURL)
	assert.Equal(t, api.JobFetching, j.State)
	assert.Equal(t, exURL, j.Source, "source defaults to the URL")
	assert.Len(t, h.logs.events(t, obs.EventFetchSubmitted), 1)
}

func TestFetchSubmitNoDownloader409(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	client := h.client(submitURLOn("public"))
	// A processing worker and a pending downloader do not count.
	h.activeWorker(api.WorkerFacts{}, "public")
	h.worker(api.WorkerFacts{Kind: api.WorkerDownloader, Kinds: []api.TaskKind{api.KindFetch}})
	resp, b := h.fetchCall(client, "public", api.FetchRequest{JobParams: api.JobParams{Model: "small.en"}, URL: exURL}, "")
	h.expectProblem(resp, b, http.StatusConflict, api.ProblemNoDownloader)

	// A downloader for another queue does not either.
	h.expect("POST", "/admin/queues", h.admin, api.Queue{Name: "other"}, http.StatusCreated, nil)
	h.downloader("", "", "other")
	resp, b = h.fetchCall(client, "public", api.FetchRequest{JobParams: api.JobParams{Model: "small.en"}, URL: exURL}, "")
	h.expectProblem(resp, b, http.StatusConflict, api.ProblemNoDownloader)

	// A paused one does.
	id, _ := h.downloader("", "", "public")
	h.expect("POST", "/admin/workers/"+id+"/pause", h.admin, nil, http.StatusNoContent, nil)
	h.fetchSubmit(client, "public", exURL)
}

func TestFetchSubmitDownloaderBelowFloor409(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	client := h.client(submitURLOn("public"))
	h.downloader("", api.SandboxFSOnly, "public")
	resp, b := h.fetchCall(client, "public", api.FetchRequest{JobParams: api.JobParams{Model: "small.en"}, URL: exURL}, "")
	h.expectProblem(resp, b, http.StatusConflict, api.ProblemNoDownloader)

	// With the floor lowered (--fetch-min-sandbox fs-only) it is eligible.
	h2 := newHarness(t, func(_ *Config, sc *store.Config) { sc.FetchMinSandbox = api.SandboxFSOnly })
	client2 := h2.client(submitURLOn("public"))
	h2.downloader("", api.SandboxFSOnly, "public")
	h2.fetchSubmit(client2, "public", exURL)
}

func TestFetchSubmitValidation(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	client := h.client(submitURLOn("public"))
	h.downloader("", "", "public")
	p := api.JobParams{Model: "small.en"}
	for name, u := range map[string]string{
		"scheme":   "ftp://media.example.com/a.wav",
		"userinfo": "https://user:pw@media.example.com/a.wav",
		"no host":  "https:///a.wav",
		"empty":    "",
		"too long": "https://media.example.com/" + strings.Repeat("a", api.MaxSourceLen),
		"no site":  "https://com/a.wav",
	} {
		resp, b := h.fetchCall(client, "public", api.FetchRequest{JobParams: p, URL: u}, "")
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, "%s: %s", name, b)
	}
	resp, b := h.fetchCall(client, "public", api.FetchRequest{JobParams: api.JobParams{Model: "nope"}, URL: exURL}, "")
	h.expectProblem(resp, b, http.StatusBadRequest, api.ProblemBadRequest)
	resp = h.do(req{method: "POST", path: "/queue/public/fetch", token: client,
		body: strings.NewReader(`{"model":"small.en","url":"` + exURL + `","bogus":1}`)})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "unknown fields are refused")

	// An explicit source is kept; idempotency replays the job.
	fr := api.FetchRequest{JobParams: api.JobParams{Model: "small.en", Source: "my talk"}, URL: exURL}
	resp, b = h.fetchCall(client, "public", fr, "k1")
	require.Equal(t, http.StatusCreated, resp.StatusCode, string(b))
	var a api.Job
	require.NoError(t, json.Unmarshal(b, &a))
	assert.Equal(t, "my talk", a.Source)
	resp, b = h.fetchCall(client, "public", fr, "k1")
	require.Equal(t, http.StatusOK, resp.StatusCode, string(b))
	var again api.Job
	require.NoError(t, json.Unmarshal(b, &again))
	assert.Equal(t, a.ID, again.ID)
}

func TestFetchFlowToTranscript(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	client := h.client(submitURLOn("public"))
	did, dtok := h.downloader("home", "", "public")
	wid, wtok := h.activeWorker(api.WorkerFacts{}, "public")
	job := h.fetchSubmit(client, "public", exURL)

	assert.Nil(t, h.claim(wid, wtok, "", nil), "a processing worker gets no fetch task")
	l := h.claim(did, dtok, "", fetchKinds())
	require.NotNil(t, l)
	assert.Equal(t, api.KindFetch, l.Kind)
	assert.Equal(t, exURL, l.URL)
	assert.Equal(t, exSite, l.Site)
	assert.Equal(t, int64(2<<30), l.MaxOutputBytes, "the server's --max-upload-bytes")

	// A fetch lease has no input.
	resp, b := h.call("GET", l.InputURL, dtok, nil)
	h.expectProblem(resp, b, http.StatusNotFound, api.ProblemNotFound)

	orig := stereoWAV(1)
	resp, b = h.putFetch(l, dtok, orig)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(b))
	j := h.job(job.ID)
	assert.Equal(t, api.JobPreparing, j.State)
	assert.Equal(t, int64(len(orig)), j.Media.OriginalBytes)
	// The result is idempotent; another body conflicts.
	resp, b = h.putFetch(l, dtok, orig)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(b))
	resp, b = h.putFetch(l, dtok, stereoWAV(2))
	h.expectProblem(resp, b, http.StatusConflict, api.ProblemConflict)

	pl := h.claim(wid, wtok, "", nil)
	require.NotNil(t, pl)
	require.Equal(t, api.KindPrepare, pl.Kind)
	resp, in := h.call("GET", pl.InputURL, wtok, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, orig, in)
	resp, b = h.putFLAC(pl, wtok, canonicalFLAC(t, speech(4, media.SampleRate)))
	require.Equal(t, http.StatusOK, resp.StatusCode, string(b))
	for {
		tl := h.claim(wid, wtok, "", nil)
		if tl == nil {
			break
		}
		resp, b := h.putResult(tl, wtok, result(tl, "words"))
		require.Equal(t, http.StatusOK, resp.StatusCode, string(b))
	}
	assert.Equal(t, api.JobDone, h.job(job.ID).State)

	done := h.logs.events(t, obs.EventFetchDone)
	require.Len(t, done, 1)
	assert.Equal(t, exSite, done[0][obs.FieldSite])
	assert.Equal(t, "label:home", done[0][obs.FieldEgressKey])
	assert.Equal(t, "yt-dlp", done[0][obs.FieldFetchMethod])
	assert.EqualValues(t, len(orig), done[0][obs.FieldBytes])
	m := h.metricsText()
	assert.Contains(t, m, `whisper_fetch_total{site="other",outcome="done"} 1`)
	assert.Contains(t, m, `whisper_fetch_bytes_total{site="other"} `+itoa(len(orig)))
}

func TestFetchFastPathFLAC(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	client := h.client(submitURLOn("public"))
	did, dtok := h.downloader("", "", "public")
	job := h.fetchSubmit(client, "public", ytURL)
	l := h.claim(did, dtok, "", nil)
	require.NotNil(t, l)
	assert.Equal(t, "youtube.com", l.Site)
	flac := canonicalFLAC(t, speech(7, media.SampleRate))
	resp, b := h.putFetch(l, dtok, flac)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(b))
	j := h.job(job.ID)
	assert.Equal(t, api.JobQueued, j.State)
	assert.True(t, j.Media.OriginalPurged)
	assert.Greater(t, j.Tasks.Total, 2)
	_, err := os.Stat(blobPath(t, h, job.ID, blobs.Original))
	assert.ErrorIs(t, err, os.ErrNotExist)
	stored, err := os.ReadFile(blobPath(t, h, job.ID, blobs.Canonical))
	require.NoError(t, err)
	assert.Equal(t, flac, stored)
	assert.Contains(t, h.metricsText(), `whisper_fetch_total{site="youtube.com",outcome="done"} 1`)
}

func TestFetchResultTooLarge413(t *testing.T) {
	t.Parallel()
	h := newHarness(t, func(c *Config, sc *store.Config) { sc.FetchMaxBytes = 4000 })
	client := h.client(submitURLOn("public"))
	did, dtok := h.downloader("", "", "public")
	job := h.fetchSubmit(client, "public", exURL)
	l := h.claim(did, dtok, "", nil)
	require.NotNil(t, l)
	assert.Equal(t, int64(4000), l.MaxOutputBytes)
	resp, b := h.putFetch(l, dtok, stereoWAV(1))
	h.expectProblem(resp, b, http.StatusRequestEntityTooLarge, api.ProblemTooLarge)
	j := h.job(job.ID)
	assert.Equal(t, api.JobFailed, j.State, "too large is not retryable")
	assert.Equal(t, 1, j.Tasks.Failed)
	assertNoOriginal(t, h, job.ID)
}

func TestFetchResultNotMedia422(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	client := h.client(submitURLOn("public"))
	did, dtok := h.downloader("", "", "public")
	d2, d2tok := h.downloader("", "", "public")
	for _, body := range []string{
		"<!DOCTYPE html><html><head><title>Sign in</title></head><body>no</body></html>",
		`{"error": "not found"}`,
	} {
		job := h.fetchSubmit(client, "public", exURL)
		l := h.claim(did, dtok, "", nil)
		require.NotNil(t, l)
		resp, b := h.putFetch(l, dtok, []byte(body))
		h.expectProblem(resp, b, http.StatusUnprocessableEntity, api.ProblemNotMedia)
		j := h.job(job.ID)
		assert.Equal(t, api.JobFailed, j.State, "not media fails the job at once")
		require.NotNil(t, j.Error)
		assert.Contains(t, *j.Error, "invalid result: not media")
		assertNoOriginal(t, h, job.ID)
		assert.Nil(t, h.claim(d2, d2tok, "", nil), "not requeued")
	}
	assert.Contains(t, h.metricsText(), `whisper_fetch_total{site="other",outcome="not_media"} 2`)
}

func assertNoOriginal(t *testing.T, h *harness, jobID string) {
	t.Helper()
	ents, err := os.ReadDir(filepath.Join(h.blobs.Root(), jobID))
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	require.NoError(t, err)
	for _, e := range ents {
		assert.NotContains(t, e.Name(), blobs.Original, "no original or part file left")
	}
}

// abortingReader yields its bytes, then fails, so the client abandons the
// chunked body mid-stream.
type abortingReader struct{ r io.Reader }

func (a *abortingReader) Read(p []byte) (int, error) {
	n, err := a.r.Read(p)
	if errors.Is(err, io.EOF) {
		return n, errors.New("child crashed")
	}
	return n, err
}

func TestFetchResultTruncatedNotCommitted(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	client := h.client(submitURLOn("public"))
	did, dtok := h.downloader("", "", "public")
	job := h.fetchSubmit(client, "public", exURL)
	l := h.claim(did, dtok, "", nil)
	require.NotNil(t, l)

	orig := stereoWAV(1)
	hr, err := http.NewRequestWithContext(h.ctx, "PUT", h.ts.URL+"/lease/"+l.ID+"/result",
		&abortingReader{r: bytes.NewReader(orig[:len(orig)/2])})
	require.NoError(t, err)
	hr.Header.Set("Authorization", "Bearer "+dtok)
	_, err = h.ts.Client().Do(hr)
	require.Error(t, err, "the client aborted the body")

	// Wait until the server has finished with the request.
	require.Eventually(t, func() bool {
		for _, e := range h.logs.events(t, obs.EventHTTPRequest) {
			if e[obs.FieldRoute] == "/lease/{id}/result" {
				return true
			}
		}
		return false
	}, testscale.D(5*time.Second), 10*time.Millisecond)
	assertNoOriginal(t, h, job.ID)
	j := h.job(job.ID)
	assert.Equal(t, api.JobFetching, j.State)
	h.expect("POST", "/lease/"+l.ID+"/heartbeat", dtok, nil, http.StatusOK, nil)

	resp, b := h.putFetch(l, dtok, orig)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(b))
	assert.Equal(t, api.JobPreparing, h.job(job.ID).State)
}

func TestFetchRateLimitedCooldownBlocksGroup(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	client := h.client(submitURLOn("public"))
	aID, aTok := h.downloader("a", "", "public")
	job := h.fetchSubmit(client, "public", ytURL)
	l := h.claim(aID, aTok, "", nil)
	require.NotNil(t, l)
	h.expect("POST", "/lease/"+l.ID+"/failure", aTok, api.FailureReport{Reason: "HTTP Error 429: Too Many Requests",
		Retryable: false, Stage: api.StageRateLimited}, http.StatusNoContent, nil)
	j := h.job(job.ID)
	assert.Equal(t, api.JobFetching, j.State, "requeued, not failed")
	assert.Equal(t, 1, j.Tasks.Pending)

	rl := h.logs.events(t, obs.EventFetchRateLimited)
	require.Len(t, rl, 1)
	assert.Equal(t, "youtube.com", rl[0][obs.FieldSite])
	assert.Equal(t, "label:a", rl[0][obs.FieldEgressKey])
	cd := h.logs.events(t, obs.EventFetchCooldown)
	require.Len(t, cd, 1)
	assert.Equal(t, "youtube.com", cd[0][obs.FieldSite])
	assert.Equal(t, "label:a", cd[0][obs.FieldEgressKey])

	h.srv.refreshStats(h.ctx)
	m := h.metricsText()
	assert.Contains(t, m, `whisper_fetch_total{site="youtube.com",outcome="rate_limited"} 1`)
	assert.Contains(t, m, `whisper_fetch_cooldowns{site="youtube.com"} 1`)
	assert.Contains(t, m, `whisper_fetch_blocked_tasks{site="youtube.com"} 1`)

	// The whole group is cooled down, even after the rate window passes.
	h.clk.Advance(10 * time.Minute)
	assert.Nil(t, h.claim(aID, aTok, "", nil))
	a2ID, a2Tok := h.downloader("a", "", "public")
	assert.Nil(t, h.claim(a2ID, a2Tok, "", nil), "same egress group")
	bID, bTok := h.downloader("b", "", "public")
	l2 := h.claim(bID, bTok, "", nil)
	require.NotNil(t, l2, "another group takes it")
	assert.Equal(t, job.ID, l2.Job)
}

func TestFetchPacingWakesWaitingClaim(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	client := h.client(submitURLOn("public"))
	did, dtok := h.downloader("a", "", "public")
	h.fetchSubmit(client, "public", ytURL)
	l := h.claim(did, dtok, "", nil)
	require.NotNil(t, l)
	resp, b := h.putFetch(l, dtok, canonicalFLAC(t, speech(1, media.SampleRate)))
	require.Equal(t, http.StatusOK, resp.StatusCode, string(b))

	// youtube.com allows 1 per 3m per group: the next one must wait.
	job2 := h.fetchSubmit(client, "public", ytURL+"x")
	assert.Nil(t, h.claim(did, dtok, "", nil))

	wait := testscale.D(8 * time.Second)
	got := make(chan *api.Lease, 1)
	go func() { got <- h.claim(did, dtok, wait.String(), nil) }()
	require.Eventually(t, func() bool { return len(h.srv.waiters.keys()) > 0 },
		testscale.D(5*time.Second), 5*time.Millisecond, "the claim is waiting")
	start := time.Now()
	h.clk.Advance(3*time.Minute + time.Second)
	select {
	case l2 := <-got:
		require.NotNil(t, l2, "woken by the pacing timer, not the timeout")
		assert.Equal(t, job2.ID, l2.Job)
		assert.Less(t, time.Since(start), wait)
	case <-time.After(wait + testscale.D(2*time.Second)):
		t.Fatal("claim was not woken when the rate window freed")
	}
}

func TestDownloaderCannotRequestPrepare(t *testing.T) {
	t.Parallel()
	h := newHarness(t, func(c *Config, _ *store.Config) { c.RegisterBurst = 100 })
	did, dtok := h.downloader("", "", "public")
	wid, wtok := h.activeWorker(api.WorkerFacts{}, "public")

	resp, b := h.call("POST", "/worker/"+did+"/leases", dtok,
		api.ClaimRequest{Kinds: []api.TaskKind{api.KindPrepare}})
	h.expectProblem(resp, b, http.StatusBadRequest, api.ProblemBadRequest)
	resp, b = h.call("POST", "/worker/"+did+"/leases", dtok,
		api.ClaimRequest{Kinds: []api.TaskKind{api.KindFetch, api.KindTranscribe}})
	h.expectProblem(resp, b, http.StatusBadRequest, api.ProblemBadRequest)
	resp, b = h.call("POST", "/worker/"+wid+"/leases", wtok, fetchKinds())
	h.expectProblem(resp, b, http.StatusBadRequest, api.ProblemBadRequest)
	assert.Nil(t, h.claim(did, dtok, "", fetchKinds()))
	var doc api.Worker
	h.expect("GET", "/worker/"+did, dtok, nil, http.StatusOK, &doc)
	assert.Equal(t, []string{"public"}, doc.Queues, "a downloader's queues are its fetch grants")

	// Registration: kinds must match the worker kind.
	for name, f := range map[string]api.WorkerFacts{
		"processing lists fetch":    {Hostname: "h", SandboxLevel: api.SandboxFull, Kinds: []api.TaskKind{api.KindFetch}},
		"downloader lists prepare":  {Hostname: "h", SandboxLevel: api.SandboxFull, Kind: api.WorkerDownloader, Kinds: []api.TaskKind{api.KindPrepare}},
		"unknown kind":              {Hostname: "h", SandboxLevel: api.SandboxFull, Kind: "uploader"},
		"bad egress ip":             {Hostname: "h", SandboxLevel: api.SandboxFull, Kind: api.WorkerDownloader, Kinds: []api.TaskKind{api.KindFetch}, EgressIP: "nope"},
		"bad egress group":          {Hostname: "h", SandboxLevel: api.SandboxFull, Kind: api.WorkerDownloader, Kinds: []api.TaskKind{api.KindFetch}, EgressGroup: "a@b"},
		"processing reports egress": {Hostname: "h", SandboxLevel: api.SandboxFull, EgressGroup: "a"},
	} {
		resp, b := h.call("POST", "/worker", "", f)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, "%s: %s", name, b)
	}

	// The kind is fixed at registration.
	resp, b = h.call("PATCH", "/worker/"+did, dtok, api.WorkerFacts{Hostname: "h", SandboxLevel: api.SandboxFull,
		Kinds: []api.TaskKind{api.KindPrepare}})
	h.expectProblem(resp, b, http.StatusConflict, api.ProblemConflict)
	resp, b = h.call("PATCH", "/worker/"+wid+"", wtok, api.WorkerFacts{Hostname: "h", SandboxLevel: api.SandboxFull,
		Kind: api.WorkerDownloader, Kinds: []api.TaskKind{api.KindFetch}})
	h.expectProblem(resp, b, http.StatusConflict, api.ProblemConflict)
}

func TestEgressKeyResolution(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		facts  api.WorkerFacts
		remote string
		want   string
	}{
		{"label wins", api.WorkerFacts{EgressGroup: "home", EgressIP: "93.184.216.34"}, "8.8.8.8", "label:home"},
		{"probe ip", api.WorkerFacts{EgressIP: "93.184.216.34"}, "8.8.8.8", "ip:93.184.216.34"},
		{"private probe ip ignored", api.WorkerFacts{EgressIP: "10.0.0.1"}, "8.8.8.8", "ip:8.8.8.8"},
		{"public remote", api.WorkerFacts{}, "8.8.8.8", "ip:8.8.8.8"},
		{"public remote v6", api.WorkerFacts{}, "2606:4700:4700::1111", "ip:2606:4700:4700::1111"},
		{"private remote", api.WorkerFacts{}, "192.168.1.5", "unknown"},
		{"loopback remote", api.WorkerFacts{}, "127.0.0.1", "unknown"},
		{"garbage remote", api.WorkerFacts{}, "", "unknown"},
	} {
		assert.Equal(t, tc.want, egressKeyFor(tc.facts, tc.remote), tc.name)
	}

	// Through the claim: the key is stored on the worker before claiming.
	h := newHarness(t, func(c *Config, _ *store.Config) { c.TrustProxy = true })
	client := h.client(submitURLOn("public"))
	did, dtok := h.downloader("", "", "public")
	h.fetchSubmit(client, "public", exURL)
	key := func() string {
		wk, err := h.st.WorkerByID(h.ctx, did)
		require.NoError(t, err)
		return wk.EgressKey
	}
	resp := h.do(req{method: "POST", path: "/worker/" + did + "/leases", token: dtok,
		header: map[string]string{"X-Forwarded-For": "93.184.216.34"}})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.Equal(t, "ip:93.184.216.34", key())

	gid, gtok := h.downloader("home", "", "public")
	assert.Nil(t, h.claim(gid, gtok, "", nil))
	wk, err := h.st.WorkerByID(h.ctx, gid)
	require.NoError(t, err)
	assert.Equal(t, "label:home", wk.EgressKey)
}

func TestFetchSubmitURLNotLoggedOrAudited(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	client := h.client(submitURLOn("public"))
	did, dtok := h.downloader("", "", "public")
	// The bare token, the URL, and a percent-decoded form of it.
	const token = "secret"
	secretURL := "https://media.example.com/my%20talk.wav?token=" + token + "%2Fx"
	decoded := "https://media.example.com/my talk.wav?token=" + token + "/x"
	resp, b := h.fetchCall(client, "public", api.FetchRequest{
		JobParams: api.JobParams{Model: "small.en", Source: "my talk"}, URL: secretURL}, "")
	require.Equal(t, http.StatusCreated, resp.StatusCode, string(b))
	var job api.Job
	require.NoError(t, json.Unmarshal(b, &job))
	// Bad submissions must not echo it into logs either.
	h.fetchCall(client, "public", api.FetchRequest{JobParams: api.JobParams{Model: "nope"}, URL: secretURL}, "")
	h.fetchCall(client, "public", api.FetchRequest{JobParams: api.JobParams{Model: "small.en"},
		URL: "ftp://media.example.com/a.wav?token=" + token}, "")
	l := h.claim(did, dtok, "", nil)
	require.NotNil(t, l)
	assert.Equal(t, secretURL, l.URL, "the downloader gets the URL")
	h.expect("POST", "/lease/"+l.ID+"/failure", dtok, api.FailureReport{
		Reason:     "ERROR: [generic] " + secretURL + ": HTTP Error 404 for " + decoded + " (token " + token + ")",
		StderrTail: "fetching " + decoded + "\nmedia.example.com/my talk.wav?token=" + token + "\nERROR 404",
		Retryable:  false, Stage: api.StageInput},
		http.StatusNoContent, nil)
	_, jb := h.call("GET", "/job/"+job.ID, client, nil)
	var j api.Job
	require.NoError(t, json.Unmarshal(jb, &j))
	require.Equal(t, api.JobFailed, j.State)
	require.NotNil(t, j.Error)
	assert.Contains(t, *j.Error, "permanent failure", "a classified reason, not the worker's text")
	assert.NotContains(t, string(jb), token, "the job view")
	assert.NotContains(t, string(jb), "media.example.com")
	h.srv.refreshStats(h.ctx)

	logs := string(h.logs.Bytes())
	for _, leak := range []string{token, "media.example.com", "my talk.wav", "my%20talk"} {
		assert.NotContains(t, logs, leak)
		assert.NotContains(t, h.metricsText(), leak)
	}
	failed := h.logs.events(t, obs.EventTaskFailed)
	require.Len(t, failed, 1)
	assert.Equal(t, "permanent failure", failed[0][obs.FieldError])
	assert.Empty(t, failed[0][obs.FieldStderrTail])
	es, err := h.st.QueryAudit(h.ctx, store.AuditFilter{Limit: 1000})
	require.NoError(t, err)
	var found bool
	for _, e := range es {
		assert.NotContains(t, e.Detail, token, e.Action)
		assert.NotContains(t, e.Detail, "media.example.com", e.Action)
		if e.Action == "job.create" {
			found = true
			assert.Contains(t, e.Detail, `"site":"example.com"`)
		}
	}
	assert.True(t, found)
}

func TestFetchFailureReasons(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		stage     api.FailureStage
		retryable bool
		want      string
	}{
		{api.StageRateLimited, false, "rate limited"},
		{api.StagePreWork, true, "pre-work failure"},
		{api.StageInput, false, "permanent failure"},
		{api.StageInput, true, "transient failure"},
		{"", true, "transient failure"},
	} {
		assert.Equal(t, tc.want, fetchFailureReason(tc.stage, tc.retryable), "%s %v", tc.stage, tc.retryable)
	}
}

// TestFetchFailureKeepsProcessingReasons: other leases keep the worker's
// own failure text.
func TestFetchFailureKeepsProcessingReasons(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	client := h.client(submitOn("public"))
	wid, wtok := h.activeWorker(api.WorkerFacts{}, "public")
	h.submit(client, "public", api.JobParams{Model: "small.en"}, []byte("not audio at all"))
	l := h.claim(wid, wtok, "", nil)
	require.NotNil(t, l)
	h.expect("POST", "/lease/"+l.ID+"/failure", wtok, api.FailureReport{Reason: "ffmpeg exploded",
		Retryable: true}, http.StatusNoContent, nil)
	failed := h.logs.events(t, obs.EventTaskFailed)
	require.Len(t, failed, 1)
	assert.Equal(t, "ffmpeg exploded", failed[0][obs.FieldError])
}

func TestFetchLeaseEndOutcomesCounted(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	client := h.client(submitURLOn("public"))
	did, dtok := h.downloader("", "", "public")
	h.fetchSubmit(client, "public", exURL)
	l := h.claim(did, dtok, "", nil)
	require.NotNil(t, l)
	h.expect("DELETE", "/lease/"+l.ID, dtok, nil, http.StatusNoContent, nil)
	assert.Contains(t, h.metricsText(), `whisper_fetch_total{site="other",outcome="released"} 1`)

	l = h.claim(did, dtok, "", nil)
	require.NotNil(t, l)
	h.clk.Advance(time.Hour)
	h.srv.reapOnce(h.ctx)
	assert.Contains(t, h.metricsText(), `whisper_fetch_total{site="other",outcome="expired"} 1`)
}

func TestNotMediaAllowsKnownMedia(t *testing.T) {
	t.Parallel()
	pad := func(b []byte) []byte { return append(b, bytes.Repeat([]byte("plain text "), 40)...) }
	for name, head := range map[string][]byte{
		"mpeg layer I": pad([]byte{0xFF, 0xFE, 0x90, 0x00}), // FF FE also reads as a UTF-16 BOM
		"mp3 frame":    pad([]byte{0xFF, 0xFB, 0x90, 0x00}),
		"id3":          pad([]byte("ID3\x04\x00")),
		"ogg":          pad([]byte("OggS\x00")),
		"mp4":          pad([]byte("\x00\x00\x00\x20ftypisom")),
		"webm":         pad([]byte{0x1A, 0x45, 0xDF, 0xA3}),
		"wav":          canonicalWAV(speech(0.01, media.SampleRate)),
		"flac":         pad([]byte("fLaC")),
	} {
		_, bad := notMedia(head)
		assert.False(t, bad, name)
	}
	for name, head := range map[string][]byte{
		"html": []byte("<!doctype html><html></html>"),
		"json": []byte(`  {"a":1}`),
		"xml":  []byte(`<?xml version="1.0"?><a/>`),
		"text": []byte("Too many requests, slow down"),
	} {
		_, bad := notMedia(head)
		assert.True(t, bad, name)
	}
}

// TestPacingTimerGenerations: a timer that fires after it was replaced
// leaves its successor pending.
func TestPacingTimerGenerations(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	client := h.client(submitURLOn("public"))
	did, dtok := h.downloader("a", "", "public")
	h.fetchSubmit(client, "public", ytURL)
	l := h.claim(did, dtok, "", nil)
	require.NotNil(t, l)
	resp, b := h.putFetch(l, dtok, canonicalFLAC(t, speech(1, media.SampleRate)))
	require.Equal(t, http.StatusOK, resp.StatusCode, string(b))
	h.fetchSubmit(client, "public", ytURL+"x") // paced: a timer is set

	pt := &h.srv.pacing
	state := func() (bool, uint64, time.Time) {
		pt.mu.Lock()
		defer pt.mu.Unlock()
		return pt.stop != nil, pt.gen, pt.due
	}
	pending, gen, due := state()
	require.True(t, pending)
	h.srv.pacingFired(gen - 1) // a stale timer
	p2, g2, d2 := state()
	assert.True(t, p2, "the pending timer is kept")
	assert.Equal(t, gen, g2)
	assert.Equal(t, due, d2)

	h.srv.pacingFired(gen) // the current one: cleared, then rescheduled
	p3, g3, _ := state()
	assert.True(t, p3)
	assert.Equal(t, gen+1, g3)
}

// TestFetchWaitersLifecycle: a downloader's key is registered while its
// claim long-polls, and removed however the claim ends.
func TestFetchWaitersLifecycle(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	client := h.client(submitURLOn("public"))
	did, dtok := h.downloader("a", "", "public")
	waiting := func() bool { return slices.Contains(h.srv.waiters.keys(), "label:a") }
	gone := func() bool { return len(h.srv.waiters.keys()) == 0 }
	tick := 5 * time.Millisecond
	within := testscale.D(5 * time.Second)

	// Timeout.
	done := make(chan *api.Lease, 1)
	go func() { done <- h.claim(did, dtok, "500ms", nil) }()
	require.Eventually(t, waiting, within, tick)
	assert.Nil(t, <-done)
	require.Eventually(t, gone, within, tick)

	// A lease.
	go func() { done <- h.claim(did, dtok, testscale.D(8*time.Second).String(), nil) }()
	require.Eventually(t, waiting, within, tick)
	h.fetchSubmit(client, "public", exURL)
	assert.NotNil(t, <-done)
	require.Eventually(t, gone, within, tick)

	// The worker goes away.
	ctx, cancel := context.WithCancel(h.ctx)
	hr, err := http.NewRequestWithContext(ctx, "POST", h.ts.URL+"/worker/"+did+"/leases?wait=8s", nil)
	require.NoError(t, err)
	hr.Header.Set("Authorization", "Bearer "+dtok)
	errc := make(chan error, 1)
	go func() {
		resp, err := h.ts.Client().Do(hr)
		if err == nil {
			_ = resp.Body.Close()
		}
		errc <- err
	}()
	require.Eventually(t, waiting, within, tick)
	cancel()
	assert.Error(t, <-errc)
	require.Eventually(t, gone, within, tick)
}

// TestFetchSiteLabel: a site gets its own metric label only when a fetch
// policy names it.
func TestFetchSiteLabel(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	require.NoError(t, h.st.InTx(h.ctx, func(tx *store.Tx) error {
		_, err := tx.PutFetchPolicy(h.ctx, fetchpolicy.Policy{Key: exSite, Site: exSite, Concurrency: 1,
			BackoffInitial: time.Minute, BackoffCap: time.Hour}, "test")
		return err
	}))
	h.srv.FetchPacingChanged()
	assert.Equal(t, exSite, h.srv.siteLabel(h.ctx, exSite))
	assert.Equal(t, "youtube.com", h.srv.siteLabel(h.ctx, "youtube.com"))
	assert.Equal(t, "other", h.srv.siteLabel(h.ctx, "vimeo.com"))
}

// TestRotationBlockedClaimRewoken: a claim that rotation holds back for a
// less recent group is woken when that group's claim takes a fetch, which
// makes the held-back group the least recent: it must not sleep out its
// long poll.
func TestRotationBlockedClaimRewoken(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	client := h.client(submitURLOn("public"))
	aid, atok := h.downloader("a", "", "public")
	bid, btok := h.downloader("b", "", "public")
	// a fetches the site once; b never has.
	h.fetchSubmit(client, "public", exURL)
	l := h.claim(aid, atok, "", nil)
	require.NotNil(t, l)
	resp, body := h.putFetch(l, atok, canonicalFLAC(t, speech(1, media.SampleRate)))
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	h.clk.Advance(time.Hour)

	// b has a claim waiting (registered directly: only its presence
	// matters), so a's claims are held back for b.
	defer h.srv.waiters.add(store.FetchWaiter{WorkerID: bid, EgressKey: "label:b", SandboxLevel: api.SandboxFull})()
	h.fetchSubmit(client, "public", exURL+"?1")
	h.fetchSubmit(client, "public", exURL+"?2")
	wait := testscale.D(8 * time.Second)
	got := make(chan *api.Lease, 1)
	go func() { got <- h.claim(aid, atok, wait.String(), nil) }()
	require.Eventually(t, func() bool { return slices.Contains(h.srv.waiters.keys(), "label:a") },
		testscale.D(5*time.Second), 5*time.Millisecond, "a's claim is waiting")
	assert.Nil(t, h.claim(aid, atok, "", nil), "rotation holds a back while b waits")

	lb := h.claim(bid, btok, "", nil)
	require.NotNil(t, lb, "b, the least recent group, takes one")
	start := time.Now()
	select {
	case la := <-got:
		require.NotNil(t, la, "a's waiting claim was woken and took the other job")
		assert.Less(t, time.Since(start), wait)
	case <-time.After(wait + testscale.D(2*time.Second)):
		t.Fatal("a's claim never returned")
	}
}

package e2e

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/client"
	"github.com/deweysasser/whisper-network/internal/e2e/e2etest"
	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/worker"
	"github.com/deweysasser/whisper-network/internal/worker/workertest"
)

// fetchEnv is a server with one active processing worker on "public".
func fetchEnv(t *testing.T) *e2etest.Env {
	e := e2etest.New(t)
	p := e.StartWorker("proc")
	e.Approve(p.ID(), "public")
	return e
}

func startDownloader(e *e2etest.Env, desc string, mods ...func(*worker.Config)) *e2etest.Worker {
	d := e.StartDownloader(desc, "home", mods...)
	// Like requirePrepareSandbox: below full, the downloader is correctly
	// never offered fetch tasks, so the test would only wait.
	if lvl := e.WorkerByDescription(desc).Facts.SandboxLevel; lvl != api.SandboxFull {
		e.T.Skipf("this platform's downloader sandbox level is %s, below the default fetch policy", lvl)
	}
	e.Approve(d.ID(), "public")
	return d
}

func urlSubmitter(e *e2etest.Env) *client.Client {
	return e.NewClient(api.Grant{Role: api.RoleSubmitURL, Scope: "queue:public"})
}

func submitURL(t *testing.T, c *client.Client, url string) *api.Job {
	t.Helper()
	j, err := c.SubmitURL(context.Background(), "public",
		api.FetchRequest{JobParams: api.JobParams{Model: e2etest.Model.Name}, URL: url}, "")
	require.NoError(t, err)
	return j
}

func fetchLeases(t *testing.T, e *e2etest.Env, jobID string) []map[string]any {
	var out []map[string]any
	for _, ev := range leasedFor(t, e, jobID) {
		if ev[obs.FieldTaskKind] == string(api.KindFetch) {
			out = append(out, ev)
		}
	}
	return out
}

func TestE2EFetchViaYtDlp(t *testing.T) {
	t.Parallel()
	e := fetchEnv(t)
	startDownloader(e, "dl", e2etest.ChildEnv(workertest.YtDlpBody(e2etest.SpeechWAV(2))))
	sub := urlSubmitter(e)
	const url = "http://127.0.0.1:1/watch?v=abc"
	job := submitURL(t, sub, url)
	assert.Equal(t, api.JobFetching, job.State)
	done := e.WaitJob(job.ID, api.JobDone)
	assert.Equal(t, url, done.Source)
	assert.NotEmpty(t, e.Transcript(sub, job.ID))
	assert.Len(t, fetchLeases(t, e, job.ID), 1)
}

func TestE2EFetchFallbackGET(t *testing.T) {
	t.Parallel()
	e := fetchEnv(t)
	wav := e2etest.SpeechWAV(2)
	var mu sync.Mutex
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		w.Header().Set("Content-Type", "audio/wav")
		_, _ = w.Write(wav)
	}))
	defer srv.Close()
	port := uint16(srv.Listener.Addr().(*net.TCPAddr).Port)
	startDownloader(e, "dl", e2etest.ChildEnv(workertest.YtDlpMode(workertest.YtDlpUnsupported)),
		func(c *worker.Config) { c.EgressPorts = []uint16{port} })
	sub := urlSubmitter(e)
	job := submitURL(t, sub, srv.URL+"/a.wav")
	done := e.WaitJob(job.ID, api.JobDone)
	assert.Equal(t, srv.URL+"/a.wav", done.Source)
	assert.NotEmpty(t, e.Transcript(sub, job.ID))
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, hits)
}

func TestE2ENoDownloader409(t *testing.T) {
	t.Parallel()
	e := fetchEnv(t)
	sub := urlSubmitter(e)
	_, err := sub.SubmitURL(context.Background(), "public",
		api.FetchRequest{JobParams: api.JobParams{Model: e2etest.Model.Name}, URL: "http://127.0.0.1:1/x"}, "")
	var ce *client.Error
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, api.ProblemNoDownloader, ce.Problem.Type)
	assert.Equal(t, http.StatusConflict, ce.Problem.Status)
}

// TestE2EPacingTwoJobsOneGroup: with a 1/3s rule on the site, two jobs
// through one egress group start fetching at least 3 s apart (server-side
// lease creation times).
func TestE2EPacingTwoJobsOneGroup(t *testing.T) {
	t.Parallel()
	e := fetchEnv(t)
	e.AdminJSON(http.MethodPut, "/admin/fetch-policies/127.0.0.1", api.FetchPolicy{
		Rules: []string{"1/3s"}, Concurrency: 1, BackoffInitial: "1m", BackoffCap: "1h"},
		http.StatusCreated, nil)
	startDownloader(e, "dl", e2etest.ChildEnv(workertest.YtDlpBody(e2etest.SpeechWAV(2))))
	sub := urlSubmitter(e)
	j1 := submitURL(t, sub, "http://127.0.0.1:1/one")
	j2 := submitURL(t, sub, "http://127.0.0.1:1/two")

	// The server's own lease creation times (millisecond resolution), not
	// when the test happened to see the leases.
	var created []time.Time
	e.Eventually(func() bool {
		return len(fetchLeases(t, e, j1.ID)) > 0 && len(fetchLeases(t, e, j2.ID)) > 0
	}, "both fetches never leased")
	for _, j := range []*api.Job{j1, j2} {
		ev := fetchLeases(t, e, j.ID)[0]
		l, err := e.Server().Store().Lease(context.Background(), ev[obs.FieldLeaseID].(string), ev[obs.FieldWorkerID].(string))
		require.NoError(t, err)
		created = append(created, l.CreatedAt)
	}
	gap := created[1].Sub(created[0]).Abs()
	assert.GreaterOrEqual(t, gap, 3*time.Second, "the 1/3s rule spaces the two fetch starts")
	e.WaitJob(j1.ID, api.JobDone)
	e.WaitJob(j2.ID, api.JobDone)
}

// TestE2ERetryRefetch: a job whose fetch failed permanently is retried and
// fetches again, on a downloader that works.
func TestE2ERetryRefetch(t *testing.T) {
	t.Parallel()
	e := fetchEnv(t)
	bad := startDownloader(e, "bad", e2etest.ChildEnv(workertest.YtDlpMode(workertest.YtDlpPrivate)))
	sub := urlSubmitter(e)
	job := submitURL(t, sub, "http://127.0.0.1:1/private")
	failed := e.WaitJob(job.ID, api.JobFailed)
	require.NotNil(t, failed.Error)
	assert.Contains(t, *failed.Error, "after 1 attempt(s)", "classified permanent, not a retryable error")
	assert.Contains(t, *failed.Error, "ERROR: [youtube] secretVideoId: Private video", "yt-dlp's own words reach the job")
	require.True(t, failed.Retryable, "retryable: %v", *failed.Error)
	require.NoError(t, bad.Stop())

	startDownloader(e, "good", e2etest.ChildEnv(workertest.YtDlpBody(e2etest.SpeechWAV(2))))
	_, err := sub.RetryJob(context.Background(), job.ID)
	require.NoError(t, err)
	done := e.WaitJob(job.ID, api.JobDone)
	assert.Equal(t, 1, done.Retries)
	assert.GreaterOrEqual(t, len(fetchLeases(t, e, job.ID)), 2)
}

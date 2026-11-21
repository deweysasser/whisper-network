package obs

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
)

func newTestMetrics() *Metrics { return NewMetrics(metrics.NewSet()) }

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "text/plain")
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(b)
}

func TestEventMetricsExposed(t *testing.T) {
	m := newTestMetrics()
	m.HTTPRequest("/job/{id}", "GET", 404, 20*time.Millisecond)
	m.JobCreated("main", api.WeightHigh, true)
	m.JobFinished("main", api.JobDone, time.Minute, time.Hour)
	m.ServedCost("main", api.WeightHigh, 12.5)
	m.TaskCompute(api.KindTranscribe, "small.en", 90)
	m.TaskRealtimeFactor("small.en", "w1", 0.2)
	m.LeaseEnded(api.OutcomeExpired, api.KindPrepare)
	m.ClaimWait(time.Second)
	m.WorkerAudio("w1", "small.en", 30)
	m.Upload("main", 1000)
	m.InputServed(api.KindTranscribe, 2000)
	m.DBTx("claim", time.Millisecond)

	out := scrape(t, m)
	for _, want := range []string{
		`whisper_http_requests_total{route="/job/{id}",method="GET",code="404"} 1`,
		`whisper_http_request_duration_seconds_count{route="/job/{id}"} 1`,
		`whisper_jobs_created_total{queue="main",weight="high",fast_path="true"} 1`,
		`whisper_jobs_finished_total{queue="main",outcome="done"} 1`,
		`whisper_job_wait_seconds_count{queue="main"} 1`,
		`whisper_job_elapsed_seconds_count{queue="main"} 1`,
		`whisper_sched_served_cost_total{queue="main",class="high"} 12.5`,
		`whisper_task_compute_seconds_count{kind="transcribe",model="small.en"} 1`,
		`whisper_task_realtime_factor_count{model="small.en",worker="w1"} 1`,
		`whisper_leases_ended_total{outcome="expired",kind="prepare"} 1`,
		`whisper_claim_wait_seconds_count 1`,
		`whisper_worker_audio_seconds_total{worker="w1",model="small.en"} 30`,
		`whisper_upload_bytes_total{queue="main"} 1000`,
		`whisper_input_served_bytes_total{kind="transcribe"} 2000`,
		`whisper_db_tx_seconds_count{op="claim"} 1`,
		"go_goroutines", "process_",
	} {
		assert.Contains(t, out, want)
	}
}

func TestHistogramsHaveLeBuckets(t *testing.T) {
	m := newTestMetrics()
	m.HTTPRequest("/x", "GET", 200, 7*time.Millisecond)
	m.TaskRealtimeFactor("small.en", "w1", 0.2)
	out := scrape(t, m)

	assert.Contains(t, out, `whisper_http_request_duration_seconds_bucket{route="/x",le="0.005"} 0`)
	assert.Contains(t, out, `whisper_http_request_duration_seconds_bucket{route="/x",le="0.01"} 1`)
	assert.Contains(t, out, `whisper_http_request_duration_seconds_bucket{route="/x",le="+Inf"} 1`)
	assert.Contains(t, out, `whisper_http_request_duration_seconds_sum{route="/x"}`)
	assert.Contains(t, out, `whisper_task_realtime_factor_bucket{model="small.en",worker="w1",le="0.2"} 1`)
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "whisper_") {
			assert.NotContains(t, line, "quantile=", "no summaries: histogram_quantile must work server-side")
		}
	}
}

func TestLabelEscaping(t *testing.T) {
	assert.Equal(t, `a\\b\"c\nd`, EscapeLabelValue("a\\b\"c\nd"))
	assert.Equal(t, "plain", EscapeLabelValue("plain"))

	m := newTestMetrics()
	m.HTTPRequest("/a\"b\\c\nd", "GET", 200, time.Millisecond)
	out := scrape(t, m)
	assert.Contains(t, out, `whisper_http_requests_total{route="/a\"b\\c\nd",method="GET",code="200"} 1`)
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "whisper_") {
			assert.NotContains(t, line, "\r", "no raw control characters in a series line")
		}
	}
}

func fixtureSnapshot() Snapshot {
	return Snapshot{
		Jobs:  []JobCount{{"main", "queued", 3}, {"main", "done", 9}},
		Tasks: []TaskCount{{"main", "transcribe", "pending", 7}},
		Queues: []QueueStat{{Queue: "main", BacklogCost: 123.5, OldestPending: 90 * time.Second,
			Weight: 2, VTime: 41.5, EligibleWorkers: 2}},
		Workers: []WorkerCount{{"active", 2}, {"pending", 1}},
		WorkerInfo: []WorkerStat{{
			ID: "w1", Hostname: `host"1`, Description: "desk", Version: "v0.5.0", EngineVersion: "1.9.5",
			FFmpegVersion: "7.1", SandboxLevel: "full", Arch: "amd64",
			LastSeen: time.Unix(1700000000, 0), Slots: 4, SlotsBusy: 3, ChildRSSBytes: 1 << 30,
			Load1: 1.5, CPUThrottled: 2 * time.Second,
			ModelsBytes: ptr(int64(3 << 30)), WorkBytes: ptr(int64(0)), StateFreeBytes: ptr(int64(50 << 30)),
			HostLoad: ptr(72.5), ClaimsDeferred: ptr(int64(12)),
		}},
		Storage:       StorageBytes{Original: 10, Canonical: 20, Transcript: 30},
		DataFreeBytes: 999, RetryableFailedJobs: 4,
	}
}

func TestSnapshotGauges(t *testing.T) {
	m := newTestMetrics()
	assert.NotContains(t, scrape(t, m), "whisper_jobs", "no snapshot yet, no state gauges")

	m.SetSnapshot(fixtureSnapshot())
	out := scrape(t, m)
	for _, want := range []string{
		`whisper_jobs{queue="main",state="done"} 9`,
		`whisper_jobs{queue="main",state="queued"} 3`,
		`whisper_tasks{queue="main",kind="transcribe",state="pending"} 7`,
		`whisper_queue_backlog_cost{queue="main"} 123.5`,
		`whisper_queue_oldest_pending_seconds{queue="main"} 90`,
		`whisper_queue_weight{queue="main"} 2`,
		`whisper_sched_vtime{queue="main"} 41.5`,
		`whisper_queue_eligible_workers{queue="main"} 2`,
		`whisper_workers{state="active"} 2`,
		`whisper_worker_info{worker="w1",hostname="host\"1",description="desk",version="v0.5.0",engine_version="1.9.5",ffmpeg_version="7.1",sandbox_level="full",arch="amd64"} 1`,
		`whisper_worker_last_seen_timestamp_seconds{worker="w1"} 1.7e+09`,
		`whisper_worker_slots{worker="w1"} 4`,
		`whisper_worker_slots_busy{worker="w1"} 3`,
		`whisper_worker_child_rss_bytes{worker="w1"} 1.073741824e+09`,
		`whisper_worker_models_bytes{worker="w1"} 3221225472`,
		`whisper_worker_work_bytes{worker="w1"} 0`,
		`whisper_worker_state_free_bytes{worker="w1"} 53687091200`,
		`whisper_worker_host_load{worker="w1"} 72.5`,
		`whisper_worker_claims_deferred_total{worker="w1"} 12`,
		`whisper_storage_bytes{kind="original"} 10`,
		`whisper_storage_bytes{kind="transcript"} 30`,
		`whisper_data_free_bytes 999`,
		`whisper_failed_jobs_retryable 4`,
	} {
		assert.Contains(t, out, want)
	}
}

// A worker that reported no disk readings (an older worker) has no disk
// series, rather than a zero that would trip the low-disk alert.
func TestWorkerDiskSeriesOnlyWhenReported(t *testing.T) {
	m := newTestMetrics()
	m.SetSnapshot(Snapshot{WorkerInfo: []WorkerStat{{ID: "old"}, {ID: "full", StateFreeBytes: ptr(int64(0))}}})
	out := scrape(t, m)
	assert.Contains(t, out, `whisper_worker_slots{worker="old"} 0`)
	assert.NotContains(t, out, `whisper_worker_models_bytes{worker="old"}`)
	assert.NotContains(t, out, `whisper_worker_state_free_bytes{worker="old"}`)
	assert.Contains(t, out, `whisper_worker_state_free_bytes{worker="full"} 0`)
	assert.NotContains(t, out, `whisper_worker_work_bytes{worker="full"}`)
	assert.NotContains(t, out, `whisper_worker_host_load{worker="old"}`)
	assert.NotContains(t, out, `whisper_worker_claims_deferred_total{worker="old"}`)
}

func ptr[T any](v T) *T { return &v }

func TestSnapshotReplacementDropsRetiredSeries(t *testing.T) {
	m := newTestMetrics()
	m.SetSnapshot(fixtureSnapshot())
	m.SetSnapshot(Snapshot{DataFreeBytes: 1})
	out := scrape(t, m)
	assert.NotContains(t, out, `queue="main"`)
	assert.NotContains(t, out, `worker="w1"`)
	assert.Contains(t, out, "whisper_data_free_bytes 1")
}

func TestNoJobLeaseOrClientLabels(t *testing.T) {
	m := newTestMetrics()
	m.SetSnapshot(fixtureSnapshot())
	m.HTTPRequest("/job/{id}", "GET", 200, time.Millisecond)
	m.LeaseEnded(api.OutcomeDone, api.KindTranscribe)
	out := scrape(t, m)
	for _, bad := range []string{"job_id=", "lease_id=", "client_id=", "job=", "lease=", "client="} {
		assert.NotContains(t, out, bad)
	}
}

func TestHandlerRejectsPost(t *testing.T) {
	rec := httptest.NewRecorder()
	newTestMetrics().Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/metrics", nil))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestInstancesAreIndependent(t *testing.T) {
	a, b := newTestMetrics(), newTestMetrics()
	a.Upload("q", 5)
	assert.NotContains(t, scrape(t, b), "whisper_upload_bytes_total")
}

func TestConcurrentUse(t *testing.T) {
	m := newTestMetrics()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				m.HTTPRequest("/x", "GET", 200, time.Millisecond)
				m.SetSnapshot(fixtureSnapshot())
				m.WritePrometheus(io.Discard)
			}
		}()
	}
	wg.Wait()
	assert.Contains(t, scrape(t, m), `whisper_http_requests_total{route="/x",method="GET",code="200"} 800`)
}

func TestFormatAndParseLabels(t *testing.T) {
	assert.Equal(t, `dc="home",env="pr\"od"`, FormatLabels(map[string]string{"env": `pr"od`, "dc": "home"}))
	assert.Equal(t, "", FormatLabels(nil))

	got, err := ParseLabels(`env=prod, dc="home"`)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod", "dc": "home"}, got)

	empty, err := ParseLabels("")
	require.NoError(t, err)
	assert.Empty(t, empty)

	for _, bad := range []string{"novalue", "=x", "9a=b", "a-b=c"} {
		_, err := ParseLabels(bad)
		assert.Error(t, err, bad)
	}
}

func TestPushSendsMetricsWithExtraLabels(t *testing.T) {
	got := make(chan string, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var body bytes.Buffer
		if r.Header.Get("Content-Encoding") == "gzip" {
			zr, err := gunzip(r.Body)
			if err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			_, _ = io.Copy(&body, zr)
		} else {
			_, _ = io.Copy(&body, r.Body)
		}
		select {
		case got <- r.Method + "\n" + r.Header.Get("X-Test") + "\n" + body.String():
		default:
		}
	}))
	defer srv.Close()

	m := newTestMetrics()
	m.Upload("main", 42)
	ctx, cancel := context.WithCancel(context.Background())
	done, err := m.StartPush(ctx, PushConfig{
		URL: srv.URL + "/api/v1/import/prometheus", Interval: 20 * time.Millisecond,
		ExtraLabels: map[string]string{"instance": "srv-1", "env": `a"b`},
		Headers:     []string{"X-Test: yes"}, Logger: zerolog.Nop(),
	})
	require.NoError(t, err)

	select {
	case msg := <-got:
		lines := strings.SplitN(msg, "\n", 3)
		assert.Equal(t, "POST", lines[0])
		assert.Equal(t, "yes", lines[1])
		assert.Contains(t, lines[2], `whisper_upload_bytes_total{env="a\"b",instance="srv-1",queue="main"} 42`)
		assert.Contains(t, lines[2], `go_goroutines{env="a\"b",instance="srv-1"}`)
	case <-time.After(5 * time.Second):
		t.Fatal("no push received")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("push loop did not stop after cancel")
	}
}

func TestStartPushValidatesConfig(t *testing.T) {
	m := newTestMetrics()
	for name, cfg := range map[string]PushConfig{
		"bad scheme":  {URL: "ftp://x/y", Interval: time.Second},
		"no interval": {URL: "http://x/y"},
		"bad header":  {URL: "http://x/y", Interval: time.Second, Headers: []string{"nocolon"}},
		"bad url":     {URL: "::", Interval: time.Second},
	} {
		_, err := m.StartPush(context.Background(), cfg)
		assert.Error(t, err, name)
	}
}

func TestServerInfo(t *testing.T) {
	m := newTestMetrics()
	assert.NotContains(t, scrape(t, m), "whisper_server_info", "no version set yet")
	m.SetServerVersion("v0.5.0-dev+7.g420d428")
	assert.Contains(t, scrape(t, m), `whisper_server_info{version="v0.5.0-dev+7.g420d428"} 1`,
		"present before the first snapshot")
	m.SetSnapshot(fixtureSnapshot())
	assert.Contains(t, scrape(t, m), `whisper_server_info{version="v0.5.0-dev+7.g420d428"} 1`)
}

func TestFetchMetrics(t *testing.T) {
	m := newTestMetrics()
	assert.NotContains(t, scrape(t, m), "whisper_fetch_invalid_policies", "not until set")
	m.FetchResult("youtube.com", "done")
	m.FetchResult("other", "rate_limited")
	m.FetchBytes("youtube.com", 4096)
	m.SetFetchInvalidPolicies(2)
	m.SetSnapshot(Snapshot{FetchCooldowns: []SiteCount{{Site: "youtube.com", N: 2}, {Site: "other", N: 1}},
		FetchBlocked: []SiteCount{{Site: "youtube.com", N: 5}}})
	out := scrape(t, m)
	for _, want := range []string{
		`whisper_fetch_total{site="youtube.com",outcome="done"} 1`,
		`whisper_fetch_total{site="other",outcome="rate_limited"} 1`,
		`whisper_fetch_bytes_total{site="youtube.com"} 4096`,
		`whisper_fetch_invalid_policies 2`,
		`whisper_fetch_cooldowns{site="other"} 1`,
		`whisper_fetch_cooldowns{site="youtube.com"} 2`,
		`whisper_fetch_blocked_tasks{site="youtube.com"} 5`,
	} {
		assert.Contains(t, out, want)
	}
}

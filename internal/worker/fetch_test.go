package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/egress"
	"github.com/deweysasser/whisper-network/internal/sandbox"
	"github.com/deweysasser/whisper-network/internal/testscale"
	"github.com/deweysasser/whisper-network/internal/worker/workertest"
)

// fakeMedia is a body that sniffs as media (FLAC) and is longer than the
// media guard's sniff window.
var fakeMedia = append([]byte("fLaC\x00\x00\x00\x22"), bytes.Repeat([]byte{1, 2, 3, 0xfe}, 1024)...)

const fetchURL = "https://www.youtube.com/watch?v=secretVideoId"

// fetchHarness is an active downloader whose fake yt-dlp runs in mode,
// printing body in the modes that print one.
func fetchHarness(t *testing.T, mode string, body []byte) *harness {
	t.Helper()
	h := downloaderHarness(t)
	h.cfg.ChildEnv = append(h.cfg.ChildEnv, workertest.YtDlpMode(mode), workertest.YtDlpBody(body))
	return h
}

// startFetch runs h's worker and queues one fetch task.
func startFetch(t *testing.T, h *harness, task workertest.Task) string {
	t.Helper()
	h.run()
	h.activate()
	task.Kind = api.KindFetch
	if task.URL == "" {
		task.URL = fetchURL
	}
	if task.Site == "" {
		task.Site = "youtube.com"
	}
	return h.srv.AddTask(task)
}

// failure waits for the task's first failure report.
func (h *harness) failure(task string) api.FailureReport {
	h.t.Helper()
	h.eventually(func() bool { return len(h.srv.Task(task).Failures) > 0 }, "no failure reported")
	return h.srv.Task(task).Failures[0]
}

// assertNoURL checks a successful fetch never logged its URL.
func (h *harness) assertNoURL(urls ...string) {
	h.t.Helper()
	logs := string(h.logs.Bytes())
	for _, u := range urls {
		assert.NotContains(h.t, logs, u)
	}
	assert.NotContains(h.t, logs, "secretVideoId")
}

// mediaServer serves body with ctype and counts requests.
func mediaServer(t *testing.T, status int, ctype string, body []byte) (*httptest.Server, *atomic.Int32) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", ctype)
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func TestFetchEndToEnd(t *testing.T) {
	h := fetchHarness(t, workertest.YtDlpOK, fakeMedia)
	task := startFetch(t, h, workertest.Task{})
	h.eventually(h.taskState(task, api.TaskDone), "fetch never completed")
	got := h.srv.Task(task)
	assert.Equal(t, fakeMedia, got.Fetched)
	assert.Equal(t, api.FetchMethodYtDlp, got.FetchMethod)
	assert.Zero(t, got.AbortedResults)
	h.waitEvent("task.done")
	assert.Contains(t, string(h.logs.Bytes()), `"site":"youtube.com"`)
	h.assertNoURL(fetchURL)
}

func TestFetchCopiesCookies(t *testing.T) {
	h := fetchHarness(t, workertest.YtDlpOK, fakeMedia)
	cookies := filepath.Join(t.TempDir(), "cookies.txt")
	require.NoError(t, os.WriteFile(cookies, []byte(workertest.CookiesHeader+"\n.youtube.com\tTRUE\t/\tTRUE\t0\tSID\tx\n"), 0o600))
	h.cfg.YtDlpCookies = cookies
	task := startFetch(t, h, workertest.Task{})
	h.eventually(h.taskState(task, api.TaskDone), "fetch with cookies never completed")
	assert.Equal(t, fakeMedia, h.srv.Task(task).Fetched)
}

func TestFetchFallsBackToGETOnUnsupported(t *testing.T) {
	srv, hits := mediaServer(t, http.StatusOK, "audio/flac", fakeMedia)
	h := fetchHarness(t, workertest.YtDlpUnsupported, nil)
	h.cfg.AllowPrivateEgress = true
	h.cfg.EgressPorts = []uint16{serverPort(srv.URL)}
	u := srv.URL + "/podcast/secretVideoId.flac"
	task := startFetch(t, h, workertest.Task{URL: u, Site: "127.0.0.1"})
	h.eventually(h.taskState(task, api.TaskDone), "GET fallback never completed")
	got := h.srv.Task(task)
	assert.Equal(t, fakeMedia, got.Fetched)
	assert.Equal(t, api.FetchMethodHTTP, got.FetchMethod)
	assert.Equal(t, int32(1), hits.Load())
	h.assertNoURL(u)
}

func TestFetchNoFallbackOnRateLimit(t *testing.T) {
	srv, hits := mediaServer(t, http.StatusOK, "audio/flac", fakeMedia)
	h := fetchHarness(t, workertest.YtDlpRateLimited, nil)
	h.cfg.AllowPrivateEgress = true
	task := startFetch(t, h, workertest.Task{URL: srv.URL + "/a.flac"})
	f := h.failure(task)
	assert.Equal(t, api.StageRateLimited, f.Stage)
	assert.True(t, f.Retryable)
	assert.Contains(t, f.Reason, "ERROR: [youtube] secretVideoId: Sign in to confirm")
	assert.Contains(t, f.StderrTail, srv.URL, "the full stderr reaches the server")
	assert.Zero(t, hits.Load(), "no GET after a rate limit")
}

func TestFetchPrivateVideoNotRetryable(t *testing.T) {
	h := fetchHarness(t, workertest.YtDlpPrivate, nil)
	task := startFetch(t, h, workertest.Task{})
	f := h.failure(task)
	assert.False(t, f.Retryable)
	assert.Equal(t, api.StageInput, f.Stage)
	assert.Contains(t, f.Reason, "Private video")
	assert.Contains(t, f.StderrTail, "ERROR: [youtube] secretVideoId: Private video")
}

func TestFetchHTMLRefused(t *testing.T) {
	h := fetchHarness(t, workertest.YtDlpHTML, nil)
	task := startFetch(t, h, workertest.Task{})
	f := h.failure(task)
	assert.False(t, f.Retryable)
	assert.Equal(t, api.StageInput, f.Stage)
	assert.Contains(t, f.Reason, "not media")
	assert.Nil(t, h.srv.Task(task).Fetched)
}

func TestFetchChildCrashAbortsUpload(t *testing.T) {
	h := fetchHarness(t, workertest.YtDlpCrashAfterBytes, fakeMedia)
	task := startFetch(t, h, workertest.Task{})
	f := h.failure(task)
	assert.True(t, f.Retryable)
	assert.Equal(t, api.StageInput, f.Stage)
	// The failure report can overtake the server's view of the aborted
	// PUT: wait for it.
	h.eventually(func() bool { return h.srv.Task(task).AbortedResults == 1 },
		"the PUT was aborted, not ended cleanly")
	assert.Nil(t, h.srv.Task(task).Fetched, "the server never saw a complete body")
}

func TestFetchTooLarge(t *testing.T) {
	h := fetchHarness(t, workertest.YtDlpOK, fakeMedia)
	task := startFetch(t, h, workertest.Task{MaxOutputBytes: 1000})
	f := h.failure(task)
	assert.False(t, f.Retryable)
	assert.Equal(t, api.StageInput, f.Stage)
	assert.Contains(t, f.Reason, "too large")
	assert.Nil(t, h.srv.Task(task).Fetched)
}

func TestFetchGETRefusesPrivateByDefault(t *testing.T) {
	srv, hits := mediaServer(t, http.StatusOK, "audio/flac", fakeMedia)
	h := fetchHarness(t, workertest.YtDlpUnsupported, nil)
	task := startFetch(t, h, workertest.Task{URL: srv.URL + "/a.flac", Site: "127.0.0.1"})
	f := h.failure(task)
	assert.False(t, f.Retryable, "a URL on a refused address is the input's fault")
	assert.Equal(t, api.StageInput, f.Stage)
	assert.Zero(t, hits.Load())
	assert.Contains(t, f.Reason, "refused")
}

func TestFetchGETOutcomes(t *testing.T) {
	for name, c := range map[string]struct {
		status    int
		ctype     string
		body      []byte
		stage     api.FailureStage
		retryable bool
	}{
		"html":        {200, "text/html; charset=utf-8", fakeMedia, api.StageInput, false},
		"json":        {200, "application/json", fakeMedia, api.StageInput, false},
		"xml":         {200, "text/xml", fakeMedia, api.StageInput, false},
		"sniffed":     {200, "application/octet-stream", []byte("<html><body>x</body></html>"), api.StageInput, false},
		"429":         {429, "text/plain", nil, api.StageRateLimited, true},
		"404":         {404, "text/plain", nil, api.StageInput, false},
		"410":         {410, "text/plain", nil, api.StageInput, false},
		"503":         {503, "text/plain", nil, api.StageInput, true},
		"forbidden":   {403, "text/plain", nil, api.StageInput, false},
		"partial 206": {206, "audio/flac", fakeMedia, api.StageInput, true},
	} {
		t.Run(name, func(t *testing.T) {
			srv, _ := mediaServer(t, c.status, c.ctype, c.body)
			h := fetchHarness(t, workertest.YtDlpUnsupported, nil)
			h.cfg.AllowPrivateEgress = true
			h.cfg.EgressPorts = []uint16{serverPort(srv.URL)}
			task := startFetch(t, h, workertest.Task{URL: srv.URL + "/a.flac"})
			f := h.failure(task)
			assert.Equal(t, c.stage, f.Stage)
			assert.Equal(t, c.retryable, f.Retryable)
			assert.Nil(t, h.srv.Task(task).Fetched)
		})
	}
}

func TestFetchGETTooLarge(t *testing.T) {
	srv, _ := mediaServer(t, http.StatusOK, "audio/flac", fakeMedia)
	h := fetchHarness(t, workertest.YtDlpUnsupported, nil)
	h.cfg.AllowPrivateEgress = true
	h.cfg.EgressPorts = []uint16{serverPort(srv.URL)}
	task := startFetch(t, h, workertest.Task{URL: srv.URL + "/a.flac", MaxOutputBytes: 1000})
	f := h.failure(task)
	assert.False(t, f.Retryable)
	assert.Contains(t, f.Reason, "too large")
}

func TestFetchYtDlpGonePreWork(t *testing.T) {
	h := fetchHarness(t, workertest.YtDlpOK, fakeMedia)
	h.run()
	h.activate()
	h.eventually(func() bool { return len(h.srv.Claims()) > 0 }, "worker never claimed")
	require.NoError(t, os.Remove(h.fakes.YtDlp))
	task := h.srv.AddTask(workertest.Task{Kind: api.KindFetch, URL: fetchURL, Site: "youtube.com"})
	f := h.failure(task)
	assert.Equal(t, api.StagePreWork, f.Stage)
	assert.Contains(t, f.Reason, "yt-dlp")
}

func TestFetchNetworkErrorTriggersEgressProbe(t *testing.T) {
	h := fetchHarness(t, workertest.YtDlpUnsupported, nil)
	h.cfg.AllowPrivateEgress = true
	// A port nothing listens on.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	dead := "http://" + ln.Addr().String() + "/a.flac"
	require.NoError(t, ln.Close())
	h.cfg.EgressPorts = []uint16{serverPort(dead)}
	var probes atomic.Int32
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		probes.Add(1)
		_, _ = w.Write([]byte("203.0.113.7"))
	}))
	defer probe.Close()
	h.cfg.EgressProbe = probe.URL
	h.cfg.EgressProbeInterval = 1 << 40
	task := startFetch(t, h, workertest.Task{URL: dead})
	f := h.failure(task)
	assert.True(t, f.Retryable)
	assert.Equal(t, api.StageInput, f.Stage)
	h.eventually(func() bool { return probes.Load() >= 2 }, "no egress probe after the network error")
}

func TestErrClassFixed(t *testing.T) {
	for want, err := range map[string]error{
		"timeout": context.DeadlineExceeded,
		"refused": errors.Join(egress.ErrRefused, errors.New("https://secret.example/x")),
		"dns":     &net.DNSError{Err: "no such host", Name: "secret.example"},
		"status":  errProbeStatus,
		"body":    errProbeBody,
		"other":   errors.New("Get \"https://secret.example/x\": EOF"),
	} {
		assert.Equal(t, want, errClass(err))
	}
}

func TestEgressProbeRejectsNonGlobalIP(t *testing.T) {
	for _, body := range []string{"10.1.2.3", "127.0.0.1", "192.168.1.1", "100.64.0.1", "fd00::1"} {
		srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
			_, _ = rw.Write([]byte(body))
		}))
		h := downloaderHarness(t)
		h.cfg.EgressProbe = srv.URL
		w, err := New(h.cfg)
		require.NoError(t, err)
		w.egressClient = srv.Client() // reach the loopback probe; the answer is still checked
		_, err = w.probeEgressIP(t.Context())
		assert.ErrorIs(t, err, errProbeBody, body)

		w.cfg.AllowPrivateEgress = true
		ip, err := w.probeEgressIP(t.Context())
		require.NoError(t, err, body)
		assert.Equal(t, body, ip)
		srv.Close()
	}
}

func TestFetchSandboxSetupFailurePreWork(t *testing.T) {
	var probes atomic.Int32
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		probes.Add(1)
		_, _ = w.Write([]byte("203.0.113.7"))
	}))
	defer probe.Close()
	h := fetchHarness(t, workertest.YtDlpOK, fakeMedia)
	// sandbox-exec fails as it does when Landlock or seccomp cannot be set up.
	self := filepath.Join(t.TempDir(), "self")
	require.NoError(t, os.WriteFile(self, []byte("#!/bin/sh\necho 'sandbox-exec: landlock: setup failed' >&2\nexit 126\n"), 0o755))
	h.cfg.Self = self
	h.cfg.AllowPrivateEgress = true
	h.cfg.EgressProbe = probe.URL
	h.cfg.EgressProbeInterval = 1 << 40
	task := startFetch(t, h, workertest.Task{})
	f := h.failure(task)
	assert.Equal(t, api.StagePreWork, f.Stage)
	time.Sleep(testscale.D(200 * time.Millisecond))
	assert.Equal(t, int32(1), probes.Load(), "a sandbox failure says nothing about the network")
}

func TestYtDlpRunError(t *testing.T) {
	exit := func(code int) error {
		err := exec.Command("/bin/sh", "-c", fmt.Sprintf("exit %d", code)).Run()
		require.Error(t, err)
		return err
	}
	var te *taskError
	assert.NoError(t, ytdlpRunError(nil))
	assert.NoError(t, ytdlpRunError(exit(1)), "an exit status is the classifier's")
	require.ErrorAs(t, ytdlpRunError(exit(sandbox.SetupFailureStatus)), &te)
	assert.Equal(t, api.StagePreWork, te.stage)
	require.ErrorAs(t, ytdlpRunError(fmt.Errorf("%w: starting yt-dlp: %w", errChildStart, os.ErrNotExist)), &te)
	assert.Equal(t, api.StagePreWork, te.stage)
	require.ErrorAs(t, ytdlpRunError(exec.ErrWaitDelay), &te)
	assert.Equal(t, api.StageInput, te.stage)
	assert.True(t, te.retryable)
}

func TestFetchExitZeroWithBytesIsOK(t *testing.T) {
	h := fetchHarness(t, workertest.YtDlpOKFiltered, fakeMedia)
	task := startFetch(t, h, workertest.Task{})
	h.eventually(h.taskState(task, api.TaskDone), "a completed upload must stand")
	h.waitEvent("task.done")
	assert.NotContains(t, string(h.logs.Bytes()), `"event":"task.failed"`)
}

func TestFetchStalledUploadEndsAtTimeout(t *testing.T) {
	const limit = 2 * time.Second
	t.Run("yt-dlp", func(t *testing.T) {
		h := fetchHarness(t, workertest.YtDlpEndless, fakeMedia)
		h.cfg.Limits.FetchTimeout = limit
		h.srv.StallResults(t)
		start := time.Now()
		task := startFetch(t, h, workertest.Task{})
		f := h.failure(task)
		assert.True(t, f.Retryable)
		assert.Less(t, time.Since(start), limit+testscale.D(2500*time.Millisecond))
	})
	t.Run("GET", func(t *testing.T) {
		media := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "audio/flac")
			_, _ = w.Write(fakeMedia)
			buf := make([]byte, 64<<10)
			for r.Context().Err() == nil {
				if _, err := w.Write(buf); err != nil {
					return
				}
			}
		}))
		defer media.Close()
		h := fetchHarness(t, workertest.YtDlpUnsupported, nil)
		h.cfg.AllowPrivateEgress = true
		h.cfg.EgressPorts = []uint16{serverPort(media.URL)}
		h.cfg.Limits.FetchTimeout = limit
		h.srv.StallResults(t)
		start := time.Now()
		task := startFetch(t, h, workertest.Task{URL: media.URL + "/a.flac"})
		f := h.failure(task)
		assert.True(t, f.Retryable)
		assert.Less(t, time.Since(start), limit+testscale.D(2500*time.Millisecond))
	})
}

func TestFetchResultFailsMidStream(t *testing.T) {
	t.Run("503", func(t *testing.T) {
		h := fetchHarness(t, workertest.YtDlpTrickle, fakeMedia)
		h.srv.FailResultsEarly(1, http.StatusServiceUnavailable)
		task := startFetch(t, h, workertest.Task{})
		f := h.failure(task) // the endless producer was stopped, or this never comes
		assert.True(t, f.Retryable)
		assert.Equal(t, api.StageInput, f.Stage)
		assert.Nil(t, h.srv.Task(task).Fetched)
	})
	t.Run("410", func(t *testing.T) {
		h := fetchHarness(t, workertest.YtDlpTrickle, fakeMedia)
		h.srv.FailResultsEarly(1, http.StatusGone)
		task := startFetch(t, h, workertest.Task{})
		h.eventually(func() bool {
			return bytes.Contains(h.logs.Bytes(), []byte("lease ended by the server; task abandoned"))
		}, "the fetch never ended after the 410")
		assert.Empty(t, h.srv.Task(task).Failures, "a gone lease is not reported")
	})
}

func TestFetchHeartbeat410KillsFetch(t *testing.T) {
	h := fetchHarness(t, workertest.YtDlpTrickle, fakeMedia)
	task := startFetch(t, h, workertest.Task{})
	l := h.liveLease(task)
	// The trickle's PUT has reached the server.
	h.eventually(func() bool { return h.srv.Task(task).ResultsStarted > 0 }, "the result PUT never started")
	h.srv.CancelLease(l.ID)
	h.eventually(func() bool {
		return bytes.Contains(h.logs.Bytes(), []byte("server ended the lease; killing the task"))
	}, "heartbeat 410 did not kill the fetch")
	h.eventually(func() bool { return h.srv.Task(task).AbortedResults == 1 }, "the PUT was not aborted")
	assert.Nil(t, h.srv.Task(task).Fetched)
}

// TestFetchChildSpecAddressSpace: deno (which yt-dlp runs for YouTube)
// reserves far more than 4 GiB of virtual address space at startup, so the
// fetch child gets no RLIMIT_AS unless the operator sets one.
func TestFetchChildSpecAddressSpace(t *testing.T) {
	h := downloaderHarness(t)
	h.cfg.Limits = Limits{} // the harness disables RLIMIT_AS; use the real defaults
	w, err := New(h.cfg)
	require.NoError(t, err)
	dir := t.TempDir()
	spec := w.fetchChildSpec(dir, 4242)
	assert.Zero(t, spec.AddressSpace, "no RLIMIT_AS by default")
	assert.True(t, spec.DenyNet)
	assert.Equal(t, []uint16{4242}, spec.ConnectPorts)
	assert.Equal(t, []string{dir}, spec.ReadWrite)
	assert.Equal(t, uint64(fetchFileSize), spec.FileSize)

	h.cfg.Limits = Limits{FetchAddressSpace: 64 << 30}
	w, err = New(h.cfg)
	require.NoError(t, err)
	assert.Equal(t, uint64(64<<30), w.fetchChildSpec(dir, 4242).AddressSpace, "operator's --fetch-address-space")
}

// TestFetchRefusedWithoutProxyConnect: below Landlock ABI 4 the fetch child
// gets no sockets at all, so a downloader there (the server's floor lowered
// to let it fetch) refuses fetch leases as pre-work instead of running
// yt-dlp into a transient failure on every attempt.
func TestFetchRefusedWithoutProxyConnect(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("only Linux applies a seccomp/Landlock network policy to the fetch child")
	}
	h := fetchHarness(t, workertest.YtDlpOK, fakeMedia)
	h.cfg.ProbeDownloaderSandbox = func(context.Context, uint16) (sandbox.ProbeResult, error) {
		return sandbox.ProbeResult{Level: api.SandboxFSOnly, LandlockABI: 3}, nil
	}
	task := startFetch(t, h, workertest.Task{})
	f := h.failure(task)
	assert.Equal(t, api.StagePreWork, f.Stage)
	assert.Contains(t, f.Reason, "proxy connects")
	assert.Nil(t, h.srv.Task(task).Fetched)
	assert.Equal(t, 1, strings.Count(string(h.logs.Bytes()), "every fetch lease is refused"), "warned once, at startup")
}

// TestFetchGETRedirectToOtherPortRefused: the plain GET's redirects are held
// to the same port allowlist as the proxy.
func TestFetchGETRedirectToOtherPortRefused(t *testing.T) {
	target, hits := mediaServer(t, http.StatusOK, "audio/flac", fakeMedia)
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/secretVideoId.flac", http.StatusFound)
	}))
	defer src.Close()
	h := fetchHarness(t, workertest.YtDlpUnsupported, nil)
	h.cfg.AllowPrivateEgress = true
	h.cfg.EgressPorts = []uint16{serverPort(src.URL)}
	task := startFetch(t, h, workertest.Task{URL: src.URL + "/a.flac"})
	f := h.failure(task)
	assert.False(t, f.Retryable, "a refused port is the URL's fault")
	assert.Equal(t, api.StageInput, f.Stage)
	assert.Zero(t, hits.Load(), "the redirect target was reached")
	assert.Contains(t, f.Reason, "refused")
}

// TestDrainKeepsEgressProxyForRunningFetch: with --drain, a fetch running
// at the shutdown signal can still reach the egress proxy and finishes.
func TestDrainKeepsEgressProxyForRunningFetch(t *testing.T) {
	h := fetchHarness(t, workertest.YtDlpProxyLate, fakeMedia)
	h.cfg.Drain = true
	cancel, done := h.run()
	h.activate()
	task := h.srv.AddTask(workertest.Task{Kind: api.KindFetch, URL: fetchURL, Site: "youtube.com"})
	h.liveLease(task)
	h.eventually(func() bool { return h.srv.LeasesFor(task)[0].Heartbeats >= 1 }, "worker never held the lease")
	cancel()
	require.NoError(t, waitDone(t, done))
	l := h.srv.LeasesFor(task)
	require.Len(t, l, 1)
	assert.Equal(t, api.OutcomeDone, l[0].Outcome, "failures: %v", h.srv.Task(task).Failures)
	assert.True(t, bytes.Equal(fakeMedia, h.srv.Task(task).Fetched), "the fetched media reached the server")
}

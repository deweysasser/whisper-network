package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/models"
	"github.com/deweysasser/whisper-network/internal/sandbox"
)

// fakeYtDlp writes an executable that prints a version.
func fakeYtDlp(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "yt-dlp")
	require.NoError(t, os.WriteFile(p, []byte("#!/bin/sh\necho 2025.01.15\n"), 0o755))
	return p
}

// downloaderHarness configures h as a downloader whose ffmpeg and
// whisper-cli do not exist.
func downloaderHarness(t *testing.T) *harness {
	h := newHarness(t)
	h.cfg.Downloader = true
	h.cfg.YtDlp = fakeYtDlp(t)
	h.cfg.WhisperCLI = "/nonexistent/whisper-cli"
	h.cfg.FFmpeg = "/nonexistent/ffmpeg"
	h.cfg.Slots = 0
	h.cfg.EgressGroup = "home"
	h.cfg.ProbeDownloaderSandbox = func(context.Context, uint16) (sandbox.ProbeResult, error) {
		return sandbox.ProbeResult{Level: api.SandboxFull, LandlockABI: 6}, nil
	}
	return h
}

func TestDownloaderFactsAndKinds(t *testing.T) {
	h := downloaderHarness(t)
	var port uint16
	h.cfg.ProbeDownloaderSandbox = func(_ context.Context, p uint16) (sandbox.ProbeResult, error) {
		port = p
		return sandbox.ProbeResult{Level: api.SandboxFull, LandlockABI: 6}, nil
	}
	h.run()
	f := h.worker().Facts
	assert.Equal(t, api.WorkerDownloader, f.Kind)
	assert.Equal(t, []api.TaskKind{api.KindFetch}, f.Kinds)
	assert.Equal(t, "2025.01.15", f.YtDlpVersion)
	assert.Equal(t, "home", f.EgressGroup)
	assert.Equal(t, api.SandboxFull, f.SandboxLevel)
	assert.Equal(t, 2, f.Slots, "downloader default")
	assert.Empty(t, f.EngineVersion)
	assert.Empty(t, f.Models)
	assert.NotZero(t, port, "the sandbox probe got the started proxy's port")
}

func TestDownloaderWarnsBelowFullSandbox(t *testing.T) {
	h := downloaderHarness(t)
	h.cfg.ProbeDownloaderSandbox = func(context.Context, uint16) (sandbox.ProbeResult, error) {
		return sandbox.ProbeResult{Level: api.SandboxFSOnly, LandlockABI: 3}, nil
	}
	h.run()
	h.worker()
	assert.Contains(t, string(h.logs.Bytes()), "will not offer fetch leases")
}

func TestDownloaderRequiresYtDlp(t *testing.T) {
	h := downloaderHarness(t)
	h.cfg.YtDlp = "/nonexistent/yt-dlp"
	w, err := New(h.cfg)
	require.NoError(t, err)
	err = w.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "yt-dlp")
	_, ok := h.srv.OnlyWorker()
	assert.False(t, ok, "must not register")
}

func TestDownloaderOfferNotEmpty(t *testing.T) {
	assert.False(t, offer{kinds: []api.TaskKind{api.KindFetch}}.empty())
	assert.True(t, offer{}.empty())

	b := newMemBudget(2 * gib)
	o, r, err := b.reserve(context.Background(), []api.TaskKind{api.KindFetch}, []models.Model(nil), 512*mib, gib)
	require.NoError(t, err)
	assert.Equal(t, []api.TaskKind{api.KindFetch}, o.kinds)
	assert.Equal(t, gib, b.inUse())
	r.release()

	w := &Worker{cfg: Config{Limits: Limits{FetchMemory: gib}}}
	assert.Equal(t, gib, w.leaseMemory(&api.Lease{Kind: api.KindFetch}))
	assert.Equal(t, diskSlack, w.leaseDisk(&api.Lease{Kind: api.KindFetch}))
}

func TestEgressProbeUpdatesFacts(t *testing.T) {
	var mu sync.Mutex
	ip := "203.0.113.7"
	probe := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		_, _ = rw.Write([]byte(ip + "\n"))
	}))
	defer probe.Close()

	h := downloaderHarness(t)
	h.cfg.EgressProbe = probe.URL
	h.cfg.AllowPrivateEgress = true // the probe server is on loopback
	h.cfg.EgressProbeInterval = time.Hour
	w, _ := h.start(t.Context())
	h.activate()
	assert.Equal(t, "203.0.113.7", h.worker().Facts.EgressIP, "registered with the first probe")

	h.eventually(func() bool { return len(h.srv.Claims()) > 0 }, "worker never became active")
	before := h.worker().Patches
	mu.Lock()
	ip = "203.0.113.8"
	mu.Unlock()
	w.requestEgressProbe()
	h.eventually(func() bool {
		wk := h.worker()
		return wk.Patches > before && wk.Facts.EgressIP == "203.0.113.8"
	}, "egress IP never patched")
	assert.NotContains(t, string(h.logs.Bytes()), probe.URL)
}

func TestEgressProbeRejectsBadBodies(t *testing.T) {
	for name, body := range map[string]string{
		"not an ip": "hello",
		"too large": string(make([]byte, 200)),
		"html":      "<html>203.0.113.7</html>",
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
				_, _ = rw.Write([]byte(body))
			}))
			defer srv.Close()
			h := downloaderHarness(t)
			h.cfg.EgressProbe = srv.URL
			h.cfg.AllowPrivateEgress = true
			w, err := New(h.cfg)
			require.NoError(t, err)
			stop, err := w.startEgress(t.Context())
			require.NoError(t, err)
			defer stop()
			_, err = w.probeEgressIP(t.Context())
			require.Error(t, err)
			assert.NotContains(t, err.Error(), srv.URL)
		})
	}
}

func TestEgressProbeFilteredByDefault(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		_, _ = rw.Write([]byte("203.0.113.7"))
	}))
	defer srv.Close()
	h := downloaderHarness(t)
	h.cfg.EgressProbe = srv.URL
	w, err := New(h.cfg)
	require.NoError(t, err)
	stop, err := w.startEgress(t.Context())
	require.NoError(t, err)
	defer stop()
	_, err = w.probeEgressIP(t.Context())
	require.Error(t, err, "loopback probe URL is refused without --allow-private-egress")
	assert.NotContains(t, err.Error(), srv.URL)
}

func TestFactsEqualIncludesEgress(t *testing.T) {
	base := api.WorkerFacts{Kind: api.WorkerDownloader, YtDlpVersion: "1", EgressGroup: "a", EgressIP: "203.0.113.7"}
	assert.True(t, factsEqual(base, base))
	for name, mut := range map[string]func(*api.WorkerFacts){
		"kind":    func(f *api.WorkerFacts) { f.Kind = "" },
		"version": func(f *api.WorkerFacts) { f.YtDlpVersion = "2" },
		"group":   func(f *api.WorkerFacts) { f.EgressGroup = "b" },
		"ip":      func(f *api.WorkerFacts) { f.EgressIP = "203.0.113.8" },
	} {
		o := base
		mut(&o)
		assert.False(t, factsEqual(base, o), name)
	}
}

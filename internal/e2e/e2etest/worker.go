package e2etest

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/backoff"
	"github.com/deweysasser/whisper-network/internal/client"
	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/worker"
	"github.com/deweysasser/whisper-network/internal/worker/workertest"
)

// CutTransport is an http.RoundTripper that can be switched off, so a
// worker can be made to vanish without a graceful goodbye (no release, no
// failure report), as if its machine crashed.
type CutTransport struct {
	cut  atomic.Bool
	next http.RoundTripper

	mu    sync.Mutex
	holds map[string]chan struct{} // path prefix → closed on release
	cuts  map[string]int64         // path suffix → cut the next body after n bytes
	held  map[string]chan struct{} // path suffix → responses held until closed
}

// HoldResponses delivers responses to requests whose path ends with suffix
// only once release is called: the server has processed the request, but
// the worker has not heard back yet.
func (c *CutTransport) HoldResponses(suffix string) (release func()) {
	ch := make(chan struct{})
	c.mu.Lock()
	if c.held == nil {
		c.held = map[string]chan struct{}{}
	}
	c.held[suffix] = ch
	c.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			delete(c.held, suffix)
			c.mu.Unlock()
			close(ch)
		})
	}
}

// CutBodyOnce makes the next response whose path ends with suffix fail
// with a connection reset after n body bytes.
func (c *CutTransport) CutBodyOnce(suffix string, n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cuts == nil {
		c.cuts = map[string]int64{}
	}
	c.cuts[suffix] = n
}

type cutBody struct {
	io.ReadCloser
	left int64
}

func (b *cutBody) Read(p []byte) (int, error) {
	if b.left <= 0 {
		return 0, errors.New("read: connection reset by peer (simulated)")
	}
	if int64(len(p)) > b.left {
		p = p[:b.left]
	}
	n, err := b.ReadCloser.Read(p)
	b.left -= int64(n)
	return n, err
}

// RoundTrip implements http.RoundTripper.
func (c *CutTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.mu.Lock()
	var gate chan struct{}
	for prefix, ch := range c.holds {
		if strings.HasPrefix(r.URL.Path, prefix) {
			gate = ch
		}
	}
	c.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
	}
	if c.cut.Load() {
		return nil, ErrSimulatedCrash
	}
	resp, err := c.next.RoundTrip(r)
	if err != nil {
		return resp, err
	}
	c.mu.Lock()
	var held chan struct{}
	for suffix, ch := range c.held {
		if strings.HasSuffix(r.URL.Path, suffix) {
			held = ch
		}
	}
	c.mu.Unlock()
	if held != nil {
		select {
		case <-held:
		case <-r.Context().Done(): // as a real in-flight response would be abandoned
			resp.Body.Close()
			return nil, r.Context().Err()
		}
	}
	c.mu.Lock()
	for suffix, n := range c.cuts {
		if strings.HasSuffix(r.URL.Path, suffix) {
			resp.Body = &cutBody{ReadCloser: resp.Body, left: n}
			delete(c.cuts, suffix)
			break
		}
	}
	c.mu.Unlock()
	return resp, nil
}

// Hold blocks requests whose path starts with prefix until release is
// called, so a test can act at a precise point of the worker's protocol.
func (c *CutTransport) Hold(prefix string) (release func()) {
	ch := make(chan struct{})
	c.mu.Lock()
	if c.holds == nil {
		c.holds = map[string]chan struct{}{}
	}
	c.holds[prefix] = ch
	c.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			delete(c.holds, prefix)
			c.mu.Unlock()
			close(ch)
		})
	}
}

// Cut makes every later request fail.
func (c *CutTransport) Cut() { c.cut.Store(true) }

// Worker is one running worker.
type Worker struct {
	Desc string
	Logs *SyncBuffer
	Net  *CutTransport
	Cfg  worker.Config

	env    *Env
	cancel context.CancelFunc
	done   chan error
	result error
	ended  bool
}

// StartWorker runs a real worker against the server with fast timings, one
// slot, the fake tools and the test catalog. desc identifies it in the
// admin API. mods adjust its configuration before it starts.
func (e *Env) StartWorker(desc string, mods ...func(*worker.Config)) *Worker {
	e.T.Helper()
	logs := &SyncBuffer{}
	tr := &CutTransport{next: http.DefaultTransport.(*http.Transport).Clone()}
	c, err := client.New(e.URL, client.WithHTTPClient(&http.Client{Transport: tr}))
	require.NoError(e.T, err)
	cfg := worker.Config{
		Client:         c,
		StateDir:       e.T.TempDir(),
		Description:    desc,
		WhisperCLI:     e.Fakes.WhisperCLI,
		FFmpeg:         e.Fakes.FFmpeg,
		Memory:         4 << 30,
		Slots:          1,
		Catalog:        Catalog(),
		Version:        "e2e",
		Logger:         obs.NewLogger(zerolog.New(logs).With().Timestamp().Logger(), obs.ModeWorker),
		ClaimWait:      time.Second,
		PendingBackoff: backoff.Policy{Initial: 50 * time.Millisecond, Max: 200 * time.Millisecond},
		RetryBackoff:   backoff.Policy{Initial: 50 * time.Millisecond, Max: 300 * time.Millisecond},
		PausedPoll:     200 * time.Millisecond,
		// Race-instrumented test binaries reserve terabytes of address space.
		Limits:   worker.Limits{NoAddressSpaceLimit: true},
		ChildEnv: []string{workertest.FastExit},
	}
	for _, m := range mods {
		m(&cfg)
	}
	w, err := worker.New(cfg)
	require.NoError(e.T, err)
	ctx, cancel := context.WithCancel(context.Background())
	wk := &Worker{Desc: desc, Logs: logs, Net: tr, Cfg: cfg, env: e, cancel: cancel, done: make(chan error, 1)}
	go func() { wk.done <- w.Run(ctx) }()
	e.T.Cleanup(func() {
		wk.cancel()
		wk.Wait()
	})
	return wk
}

// Wait waits for Run to return and returns its result.
func (w *Worker) Wait() error {
	w.env.T.Helper()
	if w.ended {
		return w.result
	}
	select {
	case w.result = <-w.done:
		w.ended = true
	case <-time.After(Timeout):
		w.env.T.Fatalf("worker %s did not stop", w.Desc)
	}
	return w.result
}

// Stop cancels the worker (graceful shutdown) and returns Run's result.
func (w *Worker) Stop() error {
	w.cancel()
	return w.Wait()
}

// Kill cuts the worker's network and then stops it, so it cannot release
// its leases: they must expire on the server.
func (w *Worker) Kill() {
	w.Net.Cut()
	w.cancel()
	_ = w.Wait()
}

// ID waits for the worker to register and returns its server-assigned ID.
func (w *Worker) ID() string {
	return w.env.WorkerByDescription(w.Desc).ID
}

// Principal is the worker's principal name in server logs.
func (w *Worker) Principal() string { return "worker:" + w.ID() }

// WaitLog waits for a worker log event matching match.
func (w *Worker) WaitLog(ev obs.Event, match func(map[string]any) bool) map[string]any {
	w.env.T.Helper()
	deadline := time.Now().Add(Timeout)
	for {
		for _, m := range w.Logs.Events(w.env.T, ev) {
			if match == nil || match(m) {
				return m
			}
		}
		if time.Now().After(deadline) {
			lines := strings.Split(strings.TrimSpace(string(w.Logs.Bytes())), "\n")
			w.env.T.Fatalf("timed out: worker %s never logged %q; its last log lines:\n%s", w.Desc, ev,
				strings.Join(lines[max(0, len(lines)-30):], "\n"))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// ChildEnv sets the fake tools' behaviour for a worker.
func ChildEnv(env ...string) func(*worker.Config) {
	return func(c *worker.Config) { c.ChildEnv = append(c.ChildEnv, env...) }
}

// Kinds restricts the task kinds a worker offers.
func Kinds(k ...api.TaskKind) func(*worker.Config) {
	return func(c *worker.Config) { c.Kinds = k }
}

// WaitClaimed waits until the worker itself has received the lease (the
// server logs task.leased before its response reaches the worker; a worker
// cancelled in between never learns of the lease).
func (w *Worker) WaitClaimed(leaseID any) {
	w.env.T.Helper()
	w.WaitLog("", func(m map[string]any) bool {
		return m["message"] == "lease claimed" && m[obs.FieldLeaseID] == leaseID
	})
}

// StartDownloader runs a real downloader worker with the fake yt-dlp, which
// may reach loopback (AllowPrivateEgress) so tests can serve media from
// local HTTP servers. group is its egress group. Approve it with
// Env.Approve like any worker (the grant is fetch).
func (e *Env) StartDownloader(desc, group string, mods ...func(*worker.Config)) *Worker {
	e.T.Helper()
	all := append([]func(*worker.Config){func(c *worker.Config) {
		c.Downloader = true
		c.YtDlp = e.Fakes.YtDlp
		c.EgressGroup = group
		c.AllowPrivateEgress = true
		c.Slots = 0
	}}, mods...)
	return e.StartWorker(desc, all...)
}

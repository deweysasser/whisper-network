package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/auth"
	"github.com/deweysasser/whisper-network/internal/blobs"
	"github.com/deweysasser/whisper-network/internal/media"
	"github.com/deweysasser/whisper-network/internal/models"
	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/store"
	"github.com/deweysasser/whisper-network/internal/testscale"
)

// fakeClock is a settable clock shared by the store and the server. Its
// AfterFunc timers fire (each in its own goroutine) when Advance reaches
// them.
type fakeClock struct {
	mu     sync.Mutex
	t      time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	due  time.Time
	f    func()
	done bool // fired or stopped
}

// AfterFunc is time.AfterFunc on the fake clock; it returns the stop func.
func (c *fakeClock) AfterFunc(d time.Duration, f func()) func() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{due: c.t.Add(d), f: f}
	c.timers = append(c.timers, t)
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		if t.done {
			return false
		}
		t.done = true
		return true
	}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	var due []*fakeTimer
	keep := c.timers[:0]
	for _, t := range c.timers {
		switch {
		case t.done:
		case !t.due.After(c.t):
			t.done = true
			due = append(due, t)
		default:
			keep = append(keep, t)
		}
	}
	c.timers = keep
	c.mu.Unlock()
	for _, t := range due {
		go t.f()
	}
}

// syncBuffer is a goroutine-safe log sink (handlers log concurrently).
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buf.Bytes()...)
}

// events returns the decoded log lines whose event field is ev.
func (b *syncBuffer) events(t *testing.T, ev obs.Event) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range bytes.Split(b.Bytes(), []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var m map[string]any
		require.NoError(t, json.Unmarshal(line, &m))
		if m[obs.FieldEvent] == string(ev) {
			out = append(out, m)
		}
	}
	return out
}

// Fake models for the /models tests: small files with known hashes.
var (
	fakeModelBytes = bytes.Repeat([]byte("ggml-fake-model "), 4096)
	fakeModel      = models.Model{Name: "fake.en", Kind: models.KindWhisper, File: "ggml-fake.en.bin",
		SHA256: sha256hex(fakeModelBytes), Size: int64(len(fakeModelBytes)), CostFactor: 1}
	badModel = models.Model{Name: "bad.en", Kind: models.KindWhisper, File: "ggml-bad.en.bin",
		SHA256: strings.Repeat("0", 64), Size: int64(len(fakeModelBytes)), CostFactor: 1}
)

func sha256hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func lookupTestModel(name string) (models.Model, error) {
	switch name {
	case fakeModel.Name:
		return fakeModel, nil
	case badModel.Name:
		return badModel, nil
	}
	return models.Lookup(name)
}

// harness is a server on a temp data dir behind httptest, with a fake
// clock, captured logs, private metrics and an adjustable free-space probe.
type harness struct {
	t       *testing.T
	ctx     context.Context
	clk     *fakeClock
	st      *store.Store
	blobs   *blobs.Store
	srv     *Server
	ts      *httptest.Server
	logs    *syncBuffer
	metrics *obs.Metrics
	free    atomic.Int64
	total   atomic.Int64 // the data filesystem's size
	dir     string
	admin   string // client token with admin on *
}

type harnessOpt func(*Config, *store.Config)

func newHarness(t *testing.T, opts ...harnessOpt) *harness {
	t.Helper()
	h := &harness{t: t, ctx: t.Context(), clk: &fakeClock{t: time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)},
		logs: &syncBuffer{}, dir: t.TempDir()}
	h.free.Store(100 << 30)
	h.total.Store(200 << 30)
	cfg := DefaultConfig()
	cfg.DataDir = h.dir
	cfg.TaskTarget = 3 * time.Second
	cfg.SplitWindow = 500 * time.Millisecond
	cfg.MaxClaimWait = testscale.D(10 * time.Second)
	cfg.Models = []string{"small.en", fakeModel.Name, badModel.Name}
	cfg.PasswordParams = auth.PasswordParams{MemoryKiB: 64, Time: 1, Threads: 1}
	scfg := store.Config{Path: filepath.Join(h.dir, "wn.db"), Now: h.clk.Now}
	for _, o := range opts {
		o(&cfg, &scfg)
	}
	var err error
	h.st, err = store.Open(h.ctx, scfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.st.Close() })
	_, err = Bootstrap(h.ctx, h.st, "admin-password", cfg.PasswordParams, io.Discard)
	require.NoError(t, err)
	h.blobs, err = blobs.New(BlobsPath(h.dir))
	require.NoError(t, err)
	h.metrics = obs.NewMetrics(metrics.NewSet())
	logger := obs.NewLogger(zerolog.New(h.logs).With().Timestamp().Logger(), obs.ModeServer)
	h.srv, err = New(cfg, Deps{Store: h.st, Blobs: h.blobs, Logger: logger, Metrics: h.metrics, Now: h.clk.Now,
		FreeBytes:  func(string) (int64, error) { return h.free.Load(), nil },
		TotalBytes: func(string) (int64, error) { return h.total.Load(), nil }, LookupModel: lookupTestModel,
		AfterFunc: h.clk.AfterFunc})
	require.NoError(t, err)
	h.ts = httptest.NewServer(h.srv.Handler())
	t.Cleanup(func() {
		h.srv.Close()
		h.ts.Close()
	})
	h.admin = h.client(api.Grant{Role: api.RoleAdmin, Scope: "*"})
	return h
}

// client creates an API client with grants and returns its token.
func (h *harness) client(grants ...api.Grant) string {
	h.t.Helper()
	c, err := h.srv.Ops().CreateClient(h.ctx, CLIActor(), "test", grants)
	require.NoError(h.t, err)
	return c.Token
}

func submitOn(q string) api.Grant { return api.Grant{Role: api.RoleSubmit, Scope: "queue:" + q} }

func submitURLOn(q string) api.Grant { return api.Grant{Role: api.RoleSubmitURL, Scope: "queue:" + q} }

// downloader registers a downloader worker in egress group (none if empty)
// at sandbox level lvl (full if empty) and approves it for queues.
func (h *harness) downloader(group string, lvl api.SandboxLevel, queues ...string) (string, string) {
	h.t.Helper()
	return h.activeWorker(api.WorkerFacts{Kind: api.WorkerDownloader, Kinds: []api.TaskKind{api.KindFetch},
		Models: []string{}, EgressGroup: group, SandboxLevel: lvl}, queues...)
}

// fetchCall posts a URL job and returns the response and body.
func (h *harness) fetchCall(token, queue string, fr api.FetchRequest, key string) (*http.Response, []byte) {
	h.t.Helper()
	b, err := json.Marshal(fr)
	require.NoError(h.t, err)
	hdr := map[string]string{"Content-Type": "application/json"}
	if key != "" {
		hdr["Idempotency-Key"] = key
	}
	resp := h.do(req{method: "POST", path: "/queue/" + queue + "/fetch", token: token, body: bytes.NewReader(b), header: hdr})
	body, err := io.ReadAll(resp.Body)
	require.NoError(h.t, err)
	return resp, body
}

// fetchSubmit posts a URL job and requires 201, returning the job.
func (h *harness) fetchSubmit(token, queue, url string) api.Job {
	h.t.Helper()
	resp, b := h.fetchCall(token, queue, api.FetchRequest{JobParams: api.JobParams{Model: "small.en"}, URL: url}, "")
	require.Equal(h.t, http.StatusCreated, resp.StatusCode, string(b))
	var j api.Job
	require.NoError(h.t, json.Unmarshal(b, &j))
	assert.Equal(h.t, "/job/"+j.ID, resp.Header.Get("Location"))
	return j
}

// putFetch PUTs a fetch result with a chunked body (no Content-Length).
func (h *harness) putFetch(l *api.Lease, token string, body []byte) (*http.Response, []byte) {
	h.t.Helper()
	resp := h.do(req{method: "PUT", path: "/lease/" + l.ID + "/result", token: token,
		body:   io.MultiReader(bytes.NewReader(body)),
		header: map[string]string{"Content-Type": "application/octet-stream", api.HeaderFetchMethod: "yt-dlp"}})
	b, err := io.ReadAll(resp.Body)
	require.NoError(h.t, err)
	return resp, b
}

// metricsText renders the server's metrics.
func (h *harness) metricsText() string {
	h.t.Helper()
	var b bytes.Buffer
	h.metrics.Set().WritePrometheus(&b)
	return b.String()
}

// req is one HTTP request in a test.
type req struct {
	method, path, token string
	body                io.Reader
	header              map[string]string
}

func (h *harness) do(r req) *http.Response {
	h.t.Helper()
	hr, err := http.NewRequestWithContext(h.ctx, r.method, h.ts.URL+r.path, r.body)
	require.NoError(h.t, err)
	if r.token != "" {
		hr.Header.Set("Authorization", "Bearer "+r.token)
	}
	for k, v := range r.header {
		hr.Header.Set(k, v)
	}
	resp, err := h.ts.Client().Do(hr)
	require.NoError(h.t, err)
	h.t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// call sends a JSON request (body marshalled unless nil) and returns the
// response with its body read.
func (h *harness) call(method, path, token string, body any) (*http.Response, []byte) {
	h.t.Helper()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(h.t, err)
		rd = bytes.NewReader(b)
	}
	resp := h.do(req{method: method, path: path, token: token, body: rd})
	b, err := io.ReadAll(resp.Body)
	require.NoError(h.t, err)
	return resp, b
}

// expect asserts a status and decodes the body into out (if non-nil).
func (h *harness) expect(method, path, token string, body any, status int, out any) []byte {
	h.t.Helper()
	resp, b := h.call(method, path, token, body)
	require.Equal(h.t, status, resp.StatusCode, "%s %s: %s", method, path, b)
	if out != nil {
		require.NoError(h.t, json.Unmarshal(b, out), string(b))
	}
	return b
}

// expectProblem asserts a problem response with the given status and slug.
func (h *harness) expectProblem(resp *http.Response, body []byte, status int, slug string) api.Problem {
	h.t.Helper()
	require.Equal(h.t, status, resp.StatusCode, string(body))
	assert.Equal(h.t, api.ProblemContentType, resp.Header.Get("Content-Type"))
	var p api.Problem
	require.NoError(h.t, json.Unmarshal(body, &p), string(body))
	assert.Equal(h.t, slug, p.Type)
	assert.Equal(h.t, status, p.Status)
	return p
}

// worker registers a worker and returns its id and token.
func (h *harness) worker(facts api.WorkerFacts) (string, string) {
	h.t.Helper()
	if facts.Hostname == "" {
		facts.Hostname = "test-host"
	}
	if facts.SandboxLevel == "" {
		facts.SandboxLevel = api.SandboxFull
	}
	if facts.Kinds == nil {
		facts.Kinds = []api.TaskKind{api.KindPrepare, api.KindTranscribe}
	}
	if facts.Models == nil {
		facts.Models = []string{"small.en"}
	}
	var reg api.RegisterResponse
	h.expect("POST", "/worker", "", facts, http.StatusCreated, &reg)
	return reg.ID, reg.Token
}

// activeWorker registers a worker and approves it for queues.
func (h *harness) activeWorker(facts api.WorkerFacts, queues ...string) (string, string) {
	h.t.Helper()
	id, tok := h.worker(facts)
	h.expect("POST", "/admin/workers/"+id+"/approve", h.admin, api.ApproveWorkerRequest{Queues: queues},
		http.StatusNoContent, nil)
	return id, tok
}

// upload posts a multipart job and returns the response and body.
func (h *harness) upload(token, queue string, params api.JobParams, mediaBytes []byte, key string) (*http.Response, []byte) {
	h.t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	ph := textproto.MIMEHeader{}
	ph.Set("Content-Disposition", `form-data; name="params"`)
	ph.Set("Content-Type", "application/json")
	pw, err := mw.CreatePart(ph)
	require.NoError(h.t, err)
	require.NoError(h.t, json.NewEncoder(pw).Encode(params))
	fw, err := mw.CreateFormFile("media", "input.bin")
	require.NoError(h.t, err)
	_, err = fw.Write(mediaBytes)
	require.NoError(h.t, err)
	require.NoError(h.t, mw.Close())
	hdr := map[string]string{"Content-Type": mw.FormDataContentType()}
	if key != "" {
		hdr["Idempotency-Key"] = key
	}
	resp := h.do(req{method: "POST", path: "/queue/" + queue, token: token, body: &buf, header: hdr})
	b, err := io.ReadAll(resp.Body)
	require.NoError(h.t, err)
	return resp, b
}

// submit uploads and requires 201, returning the job.
func (h *harness) submit(token, queue string, params api.JobParams, mediaBytes []byte) api.Job {
	h.t.Helper()
	resp, b := h.upload(token, queue, params, mediaBytes, "")
	require.Equal(h.t, http.StatusCreated, resp.StatusCode, string(b))
	var j api.Job
	require.NoError(h.t, json.Unmarshal(b, &j))
	assert.Equal(h.t, "/job/"+j.ID, resp.Header.Get("Location"))
	return j
}

// claim posts a claim and returns the lease, or nil on 204.
func (h *harness) claim(workerID, token, wait string, body *api.ClaimRequest) *api.Lease {
	h.t.Helper()
	path := "/worker/" + workerID + "/leases"
	if wait != "" {
		path += "?wait=" + wait
	}
	var b any
	if body != nil {
		b = body
	}
	resp, data := h.call("POST", path, token, b)
	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil
	case http.StatusCreated:
		var l api.Lease
		require.NoError(h.t, json.Unmarshal(data, &l))
		return &l
	}
	h.t.Fatalf("claim: %d %s", resp.StatusCode, data)
	return nil
}

func (h *harness) job(id string) api.Job {
	h.t.Helper()
	var j api.Job
	h.expect("GET", "/job/"+id, h.admin, nil, http.StatusOK, &j)
	return j
}

// putResult PUTs a transcribe result for a lease.
func (h *harness) putResult(l *api.Lease, token string, res api.TranscribeResult) (*http.Response, []byte) {
	h.t.Helper()
	return h.call("PUT", "/lease/"+l.ID+"/result", token, res)
}

// result builds a valid transcribe result with one segment.
func result(l *api.Lease, text string) api.TranscribeResult {
	return api.TranscribeResult{EngineVersion: "whisper.cpp 1.9.5", Model: l.Model, ModelSHA256: l.ModelSHA256,
		VAD: l.VADModel, Language: "en", ComputeMS: 1500,
		Segments: []api.Segment{{StartMS: 500, EndMS: 900, Text: " " + text + " "}}}
}

// putFLAC PUTs a prepare result.
func (h *harness) putFLAC(l *api.Lease, token string, body []byte) (*http.Response, []byte) {
	h.t.Helper()
	resp := h.do(req{method: "PUT", path: "/lease/" + l.ID + "/result", token: token, body: bytes.NewReader(body),
		header: map[string]string{"Content-Type": "audio/flac", api.HeaderComputeMS: "4200", api.HeaderFFmpegVersion: "7.1.1"}})
	b, err := io.ReadAll(resp.Body)
	require.NoError(h.t, err)
	return resp, b
}

// --- audio fixtures ---

// speech returns seconds of a tone with a 150 ms near-silent gap every
// second, so split points have somewhere quiet to land.
func speech(seconds float64, rate int) []int16 {
	n := int(seconds * float64(rate))
	out := make([]int16, n)
	for i := range out {
		if i%rate < rate*85/100 {
			out[i] = int16(8000 * math.Sin(float64(i)*2*math.Pi*220/float64(rate)))
		} else {
			out[i] = int16(i % 3)
		}
	}
	return out
}

func pcmBytes(s []int16) []byte {
	b := make([]byte, 2*len(s))
	for i, v := range s {
		b[2*i], b[2*i+1] = byte(v), byte(uint16(v)>>8)
	}
	return b
}

// canonicalWAV is a 16 kHz mono 16-bit WAV.
func canonicalWAV(s []int16) []byte {
	hdr := media.WAVHeader(int64(len(s)))
	return append(hdr[:], pcmBytes(s)...)
}

// stereoWAV is a well-formed 44.1 kHz stereo WAV: not canonical.
func stereoWAV(seconds float64) []byte {
	s := speech(seconds, 44100)
	var b bytes.Buffer
	data := 4 * len(s)
	le := func(v any) { _ = writeLE(&b, v) }
	b.WriteString("RIFF")
	le(uint32(36 + data))
	b.WriteString("WAVEfmt ")
	le(uint32(16))
	le(uint16(1))
	le(uint16(2))
	le(uint32(44100))
	le(uint32(44100 * 4))
	le(uint16(4))
	le(uint16(16))
	b.WriteString("data")
	le(uint32(data))
	for _, v := range s {
		le(v)
		le(v)
	}
	return b.Bytes()
}

func writeLE(w io.Writer, v any) error {
	switch x := v.(type) {
	case uint16:
		_, err := w.Write([]byte{byte(x), byte(x >> 8)})
		return err
	case int16:
		return writeLE(w, uint16(x))
	case uint32:
		_, err := w.Write([]byte{byte(x), byte(x >> 8), byte(x >> 16), byte(x >> 24)})
		return err
	}
	panic(fmt.Sprintf("writeLE %T", v))
}

// memFile is an in-memory io.WriteSeeker for EncodeFLAC.
type memFile struct {
	buf []byte
	pos int64
}

func (m *memFile) Write(p []byte) (int, error) {
	if end := m.pos + int64(len(p)); end > int64(len(m.buf)) {
		m.buf = append(m.buf, make([]byte, end-int64(len(m.buf)))...)
	}
	copy(m.buf[m.pos:], p)
	m.pos += int64(len(p))
	return len(p), nil
}

func (m *memFile) Seek(off int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		m.pos = off
	case io.SeekCurrent:
		m.pos += off
	case io.SeekEnd:
		m.pos = int64(len(m.buf)) + off
	}
	return m.pos, nil
}

// canonicalFLAC encodes samples as canonical FLAC.
func canonicalFLAC(t *testing.T, s []int16) []byte {
	t.Helper()
	var m memFile
	require.NoError(t, media.EncodeFLAC(&m, bytes.NewReader(pcmBytes(s)), int64(len(s))))
	return m.buf
}

// wavSamples decodes a canonical WAV served by /lease/{id}/input.
func wavSamples(t *testing.T, b []byte) []int16 {
	t.Helper()
	require.GreaterOrEqual(t, len(b), 44)
	w, err := media.OpenWAV(bytes.NewReader(b), 0)
	require.NoError(t, err)
	raw, err := io.ReadAll(w.PCM)
	require.NoError(t, err)
	out := make([]int16, len(raw)/2)
	for i := range out {
		out[i] = int16(uint16(raw[2*i]) | uint16(raw[2*i+1])<<8)
	}
	return out
}

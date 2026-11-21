package ui

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/auth"
	"github.com/deweysasser/whisper-network/internal/blobs"
	"github.com/deweysasser/whisper-network/internal/media"
	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/server"
	"github.com/deweysasser/whisper-network/internal/store"
)

// bootstrapPassword is the first-run admin password in tests.
const bootstrapPassword = "bootstrap-password"

// fakeClock is a settable clock shared by the store and the server.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// syncBuffer is a goroutine-safe log sink.
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

// events returns the decoded log lines with event ev.
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

// env is a real server with the UI on a temp data dir behind httptest,
// with a fake clock and captured logs.
type env struct {
	t      *testing.T
	ctx    context.Context
	clk    *fakeClock
	st     *store.Store
	srv    *server.Server
	ts     *httptest.Server
	logs   *syncBuffer
	admin  string // API client token with admin on *
	dbPath string
}

func newEnv(t *testing.T, opts ...func(*server.Config)) *env {
	t.Helper()
	return newEnvStore(t, store.Config{}, opts...)
}

// newEnvStore is newEnv with store settings (Path and Now are filled in).
func newEnvStore(t *testing.T, sc store.Config, opts ...func(*server.Config)) *env {
	t.Helper()
	e := &env{t: t, ctx: t.Context(), clk: &fakeClock{t: time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)},
		logs: &syncBuffer{}}
	dir := t.TempDir()
	cfg := server.DefaultConfig()
	cfg.DataDir = dir
	cfg.Models = []string{"small.en"}
	cfg.PasswordParams = auth.PasswordParams{MemoryKiB: 64, Time: 1, Threads: 1}
	for _, o := range opts {
		o(&cfg)
	}
	var err error
	e.dbPath = filepath.Join(dir, "wn.db")
	sc.Path, sc.Now = e.dbPath, e.clk.Now
	e.st, err = store.Open(e.ctx, sc)
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.st.Close() })
	_, err = server.Bootstrap(e.ctx, e.st, bootstrapPassword, cfg.PasswordParams, io.Discard)
	require.NoError(t, err)
	bs, err := blobs.New(server.BlobsPath(dir))
	require.NoError(t, err)
	logger := obs.NewLogger(zerolog.New(e.logs).With().Timestamp().Logger(), obs.ModeServer)
	e.srv, err = server.New(cfg, server.Deps{Store: e.st, Blobs: bs, Logger: logger,
		Metrics: obs.NewMetrics(metrics.NewSet()), Now: e.clk.Now,
		FreeBytes: func(string) (int64, error) { return 100 << 30, nil }, UI: New})
	require.NoError(t, err)
	e.ts = httptest.NewServer(e.srv.Handler())
	t.Cleanup(func() {
		e.srv.Close()
		e.ts.Close()
	})
	e.admin = e.client(api.Grant{Role: api.RoleAdmin, Scope: "*"})
	return e
}

// client creates an API client and returns its token.
func (e *env) client(grants ...api.Grant) string {
	e.t.Helper()
	c, err := e.srv.Ops().CreateClient(e.ctx, server.CLIActor(), "test", grants)
	require.NoError(e.t, err)
	return c.Token
}

// user creates a UI user with a known password (no forced change).
func (e *env) user(name string, grants ...api.Grant) {
	e.t.Helper()
	_, err := e.srv.Ops().CreateUser(e.ctx, server.CLIActor(),
		api.CreateUserRequest{Username: name, Password: name + "-password", Grants: grants})
	require.NoError(e.t, err)
}

func (e *env) queue(name string) {
	e.t.Helper()
	_, err := e.srv.Ops().CreateQueue(e.ctx, server.CLIActor(), api.Queue{Name: name})
	require.NoError(e.t, err)
}

// api sends a JSON API request with a bearer token and returns status and body.
func (e *env) api(method, path, token string, body any) (int, []byte) {
	e.t.Helper()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(e.t, err)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(e.ctx, method, e.ts.URL+path, rd)
	require.NoError(e.t, err)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := e.ts.Client().Do(req)
	require.NoError(e.t, err)
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	require.NoError(e.t, err)
	return resp.StatusCode, b
}

// worker registers a worker and returns its id and token.
func (e *env) worker(hostname, description string) (string, string) {
	e.t.Helper()
	status, b := e.api("POST", "/worker", "", api.WorkerFacts{Hostname: hostname, Description: description,
		OS: "linux", Arch: "amd64", Cores: 4, SandboxLevel: api.SandboxFull,
		Kinds: []api.TaskKind{api.KindTranscribe}, Slots: 1, Models: []string{"small.en"}})
	require.Equal(e.t, http.StatusCreated, status, string(b))
	var reg api.RegisterResponse
	require.NoError(e.t, json.Unmarshal(b, &reg))
	return reg.ID, reg.Token
}

// canonicalWAV is seconds of a 220 Hz tone as 16 kHz mono 16-bit WAV.
func canonicalWAV(seconds float64) []byte {
	n := int(seconds * media.SampleRate)
	hdr := media.WAVHeader(int64(n))
	out := append([]byte(nil), hdr[:]...)
	for i := range n {
		v := int16(8000 * math.Sin(float64(i)*2*math.Pi*220/media.SampleRate))
		out = append(out, byte(v), byte(uint16(v)>>8))
	}
	return out
}

// submit uploads a 2-second canonical job to queue with the client token.
func (e *env) submit(token, queue, label string) api.Job {
	e.t.Helper()
	return e.submitAudio(token, queue, label, 2)
}

// submitAudio uploads seconds of canonical audio as a job.
func (e *env) submitAudio(token, queue, label string, seconds float64) api.Job {
	e.t.Helper()
	return e.submitParams(token, queue, api.JobParams{Model: "small.en", Label: label}, seconds)
}

// submitParams uploads seconds of canonical audio as a job with params.
func (e *env) submitParams(token, queue string, params api.JobParams, seconds float64) api.Job {
	e.t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	ph := textproto.MIMEHeader{}
	ph.Set("Content-Disposition", `form-data; name="params"`)
	ph.Set("Content-Type", "application/json")
	pw, err := mw.CreatePart(ph)
	require.NoError(e.t, err)
	require.NoError(e.t, json.NewEncoder(pw).Encode(params))
	fw, err := mw.CreateFormFile("media", "in.wav")
	require.NoError(e.t, err)
	_, err = fw.Write(canonicalWAV(seconds))
	require.NoError(e.t, err)
	require.NoError(e.t, mw.Close())
	req, err := http.NewRequestWithContext(e.ctx, "POST", e.ts.URL+"/queue/"+queue, &buf)
	require.NoError(e.t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := e.ts.Client().Do(req)
	require.NoError(e.t, err)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	require.Equal(e.t, http.StatusCreated, resp.StatusCode, string(b))
	var j api.Job
	require.NoError(e.t, json.Unmarshal(b, &j))
	return j
}

// runJob makes an approved worker claim and finish every task of the
// queue's work, so jobs reach done.
func (e *env) runJob(workerID, workerToken string) {
	e.t.Helper()
	for {
		status, b := e.api("POST", "/worker/"+workerID+"/leases", workerToken,
			api.ClaimRequest{Kinds: []api.TaskKind{api.KindTranscribe}, Models: []string{"small.en"}})
		if status == http.StatusNoContent {
			return
		}
		require.Equal(e.t, http.StatusCreated, status, string(b))
		var l api.Lease
		require.NoError(e.t, json.Unmarshal(b, &l))
		status, b = e.api("PUT", "/lease/"+l.ID+"/result", workerToken, api.TranscribeResult{
			EngineVersion: "whisper.cpp 1.9.5", Model: l.Model, ModelSHA256: l.ModelSHA256, VAD: l.VADModel,
			Language: "en", ComputeMS: 1000, Segments: []api.Segment{{StartMS: 0, EndMS: 900, Text: "hello"}}})
		require.Less(e.t, status, 300, string(b))
	}
}

// browser is a cookie-carrying UI client that does not follow redirects.
type browser struct {
	e    *env
	c    *http.Client
	csrf string
}

func (e *env) browser() *browser {
	jar := &fakeJar{now: e.clk.Now, c: map[string]*http.Cookie{}}
	return &browser{e: e, c: &http.Client{Jar: jar,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

// login signs in (no forced change expected) and picks up the CSRF token.
func (e *env) login(name string) *browser {
	e.t.Helper()
	b := e.browser()
	resp, _ := b.submitLogin(name, name+"-password", "")
	require.Equal(e.t, http.StatusSeeOther, resp.StatusCode)
	b.refreshCSRF()
	return b
}

func (b *browser) submitLogin(user, pass, next string) (*http.Response, string) {
	b.e.t.Helper()
	return b.do("POST", "/ui/login", url.Values{"username": {user}, "password": {pass}, "next": {next}}, nil)
}

var csrfRE = regexp.MustCompile(`name="csrf" value="([^"]+)"`)

// refreshCSRF reads the session's CSRF token from a page.
func (b *browser) refreshCSRF() {
	b.e.t.Helper()
	resp, body := b.get("/ui/queues")
	require.Equal(b.e.t, http.StatusOK, resp.StatusCode, body)
	m := csrfRE.FindStringSubmatch(body)
	require.NotNil(b.e.t, m, "no CSRF token on the page")
	b.csrf = m[1]
}

func (b *browser) get(path string) (*http.Response, string) {
	b.e.t.Helper()
	return b.do("GET", path, nil, nil)
}

// post submits a form with the session's CSRF token.
func (b *browser) post(path string, form url.Values) (*http.Response, string) {
	b.e.t.Helper()
	if form == nil {
		form = url.Values{}
	}
	form.Set("csrf", b.csrf)
	return b.do("POST", path, form, nil)
}

// do sends a request; a non-nil form is sent url-encoded.
func (b *browser) do(method, path string, form url.Values, hdr map[string]string) (*http.Response, string) {
	b.e.t.Helper()
	var rd io.Reader
	if form != nil {
		rd = strings.NewReader(form.Encode())
	}
	req, err := http.NewRequestWithContext(b.e.ctx, method, b.e.ts.URL+path, rd)
	require.NoError(b.e.t, err)
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := b.c.Do(req)
	require.NoError(b.e.t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(b.e.t, err)
	return resp, string(body)
}

// fakeJar is a cookie jar for the one test server that judges expiry by the
// env's fake clock. net/http/cookiejar uses the wall clock, so a session
// cookie that expires hours after the fake clock's fixed date would vanish
// once the real date passes it.
type fakeJar struct {
	mu  sync.Mutex
	now func() time.Time
	c   map[string]*http.Cookie
}

func (j *fakeJar) SetCookies(_ *url.URL, cs []*http.Cookie) {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, c := range cs {
		if c.MaxAge < 0 || !c.Expires.IsZero() && !c.Expires.After(j.now()) {
			delete(j.c, c.Name)
			continue
		}
		j.c[c.Name] = &http.Cookie{Name: c.Name, Value: c.Value, Expires: c.Expires}
	}
}

func (j *fakeJar) Cookies(*url.URL) []*http.Cookie {
	j.mu.Lock()
	defer j.mu.Unlock()
	var out []*http.Cookie
	for name, c := range j.c {
		if !c.Expires.IsZero() && !c.Expires.After(j.now()) {
			delete(j.c, name)
			continue
		}
		out = append(out, &http.Cookie{Name: c.Name, Value: c.Value})
	}
	return out
}

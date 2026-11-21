package program

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
)

const cliToken = "wnc_clitesttoken123"

// cliServer emulates the client API endpoints the CLI uses.
func cliServer(t *testing.T) *httptest.Server {
	t.Helper()
	created := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /queue/{q}", func(w http.ResponseWriter, r *http.Request) {
		mr, err := r.MultipartReader()
		require.NoError(t, err)
		p, _ := mr.NextPart()
		var params api.JobParams
		require.NoError(t, json.NewDecoder(p).Decode(&params))
		m, _ := mr.NextPart()
		media, _ := io.ReadAll(m)
		job := api.Job{ID: "0193-new", Queue: r.PathValue("q"), State: api.JobQueued, Model: params.Model,
			Weight: params.Weight, Label: params.Label, Metadata: params.Metadata,
			Media: api.MediaInfo{OriginalBytes: int64(len(media))}, CreatedAt: created}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(job)
	})
	mux.HandleFunc("POST /queue/{q}/fetch", func(w http.ResponseWriter, r *http.Request) {
		var req api.FetchRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		require.NotEmpty(t, req.URL)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(api.Job{ID: "0193-url", Queue: r.PathValue("q"), State: api.JobFetching, CreatedAt: created})
	})
	mux.HandleFunc("GET /queue/{q}", func(w http.ResponseWriter, r *http.Request) {
		page := api.JobList{}
		if r.URL.Query().Get("cursor") == "" {
			page.Jobs = []api.Job{{ID: "j1", State: api.JobDone, Weight: api.WeightLow, Model: "small.en",
				Tasks: api.TaskCounts{Total: 2, Done: 2}, Media: api.MediaInfo{DurationS: 3725}, CreatedAt: created, Label: "council"}}
			page.NextCursor = "c2"
		} else {
			page.Jobs = []api.Job{{ID: "j2", State: api.JobState(r.URL.Query().Get("state")), Model: "large-v3-turbo", CreatedAt: created}}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(page)
	})
	mux.HandleFunc("GET /job/{id}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") != "j1" {
			w.Header().Set("Content-Type", api.ProblemContentType)
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(api.Problem{Type: api.ProblemNotFound, Title: "Not Found", Status: 404})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(api.Job{ID: "j1", State: api.JobDone})
	})
	mux.HandleFunc("GET /job/{id}/transcript", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") == "busy" {
			w.Header().Set("Content-Type", api.ProblemContentType)
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(api.Problem{Type: api.ProblemNotDone, Title: "Conflict", Status: 409})
			return
		}
		_, _ = io.WriteString(w, "# transcript-source: whisper\n[00:00.00] hello\n")
	})
	mux.HandleFunc("GET /job/{id}/result", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(api.JobResult{JobID: "j1", Segments: []api.Segment{{StartMS: 0, EndMS: 10, Text: "hello"}}})
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+cliToken {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// parseCLI parses args (with server and token flags) into Options.
func parseCLI(t *testing.T, srv *httptest.Server, args ...string) *Options {
	t.Helper()
	var opts Options
	args = append(args, "--server", srv.URL, "--token", cliToken)
	_, err := opts.Parse(args)
	require.NoError(t, err)
	return &opts
}

func TestSubmitPrintsID(t *testing.T) {
	srv := cliServer(t)
	media := filepath.Join(t.TempDir(), "meeting.mp4")
	require.NoError(t, os.WriteFile(media, []byte("fake video"), 0o600))

	opts := parseCLI(t, srv, "submit", "--weight", "low", "--label", "council", media)
	var out bytes.Buffer
	opts.Submit.out = &out
	require.NoError(t, opts.Submit.Run(context.Background()))
	assert.Equal(t, "0193-new\n", out.String())

	opts = parseCLI(t, srv, "submit", "--json", "--metadata", `{"requester":"x"}`, media)
	out.Reset()
	opts.Submit.out = &out
	require.NoError(t, opts.Submit.Run(context.Background()))
	var job api.Job
	require.NoError(t, json.Unmarshal(out.Bytes(), &job))
	assert.Equal(t, int64(10), job.Media.OriginalBytes)
	assert.JSONEq(t, `{"requester":"x"}`, string(job.Metadata))
}

func TestSubmitValidatesLocally(t *testing.T) {
	srv := cliServer(t)
	media := filepath.Join(t.TempDir(), "a.wav")
	require.NoError(t, os.WriteFile(media, []byte("x"), 0o600))
	opts := parseCLI(t, srv, "submit", "--metadata", `[1]`, media)
	assert.ErrorContains(t, opts.Submit.Run(context.Background()), "JSON object")
	opts = parseCLI(t, srv, "submit", "--prompt", strings.Repeat("x", 801), media)
	assert.ErrorContains(t, opts.Submit.Run(context.Background()), "limit is 800")

	var o Options
	_, err := o.Parse([]string{"submit", "--server", srv.URL, "--token", cliToken, "--weight", "urgent", media})
	assert.Error(t, err, "weight is an enum")
}

func TestStatusTableFollowsCursors(t *testing.T) {
	srv := cliServer(t)
	opts := parseCLI(t, srv, "status", "--state", "done")
	var out bytes.Buffer
	opts.Status.out = &out
	require.NoError(t, opts.Status.Run(context.Background()))
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	require.Len(t, lines, 3)
	assert.Regexp(t, `^ID\s+STATE\s+WEIGHT\s+MODEL\s+TASKS\s+AUDIO\s+CREATED\s+LABEL$`, lines[0])
	assert.Regexp(t, `^j1\s+done\s+low\s+small\.en\s+2/2\s+1h2m5s\s+.*council$`, lines[1])
	assert.Regexp(t, `^j2\s+done\s+`, lines[2])
}

func TestStatusOfOneJob(t *testing.T) {
	srv := cliServer(t)
	opts := parseCLI(t, srv, "status", "j1")
	var out bytes.Buffer
	opts.Status.out = &out
	require.NoError(t, opts.Status.Run(context.Background()))
	assert.Contains(t, out.String(), `"state": "done"`)

	opts = parseCLI(t, srv, "status", "nope")
	assert.ErrorContains(t, opts.Status.Run(context.Background()), "404")
}

func TestFetch(t *testing.T) {
	srv := cliServer(t)
	opts := parseCLI(t, srv, "fetch", "j1")
	var out bytes.Buffer
	opts.Fetch.out = &out
	require.NoError(t, opts.Fetch.Run(context.Background()))
	assert.Equal(t, "# transcript-source: whisper\n[00:00.00] hello\n", out.String())

	file := filepath.Join(t.TempDir(), "r.json")
	opts = parseCLI(t, srv, "fetch", "--json", "-o", file, "j1")
	require.NoError(t, opts.Fetch.Run(context.Background()))
	b, err := os.ReadFile(file)
	require.NoError(t, err)
	assert.Contains(t, string(b), `"text": "hello"`)
	if runtime.GOOS != "windows" {
		fi, err := os.Stat(file)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o600), fi.Mode().Perm(), "transcripts are private to the user")
	}

	opts = parseCLI(t, srv, "fetch", "busy")
	err = opts.Fetch.Run(context.Background())
	assert.ErrorContains(t, err, "not done yet")
}

func TestClientCommandsTakeEnv(t *testing.T) {
	srv := cliServer(t)
	t.Setenv("WHISPER_SERVER", srv.URL)
	t.Setenv("WHISPER_TOKEN", cliToken)
	var opts Options
	_, err := opts.Parse([]string{"status", "j1"})
	require.NoError(t, err)
	assert.Equal(t, cliToken, opts.Status.Token)
	var out bytes.Buffer
	opts.Status.out = &out
	require.NoError(t, opts.Status.Run(context.Background()))

}

func TestSubmitCmdRequiresFileXorURL(t *testing.T) {
	srv := cliServer(t)
	media := filepath.Join(t.TempDir(), "a.wav")
	require.NoError(t, os.WriteFile(media, []byte("x"), 0o600))
	base := []string{"--server", srv.URL, "--token", cliToken}
	var o Options
	_, err := o.Parse(append([]string{"submit"}, base...))
	assert.Error(t, err, "neither file nor url")
	o = Options{}
	_, err = o.Parse(append([]string{"submit", "--url", "https://example.com/a", media}, base...))
	assert.Error(t, err, "both file and url")

	opts := parseCLI(t, srv, "submit", "--url", "https://example.com/a", "--label", "l")
	var out bytes.Buffer
	opts.Submit.out = &out
	require.NoError(t, opts.Submit.Run(context.Background()))
	assert.Equal(t, "0193-url\n", out.String())
}

func TestListStateFetching(t *testing.T) {
	srv := cliServer(t)
	opts := parseCLI(t, srv, "status", "--state", "fetching")
	var out bytes.Buffer
	opts.Status.out = &out
	require.NoError(t, opts.Status.Run(context.Background()))
	assert.Contains(t, out.String(), "fetching")
	var o Options
	_, err := o.Parse([]string{"delete", "--state", "fetching", "--server", srv.URL, "--token", cliToken})
	assert.NoError(t, err)
}

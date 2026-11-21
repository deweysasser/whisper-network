package client

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
)

func TestNewRejectsBadURLs(t *testing.T) {
	for _, u := range []string{"", "ftp://x", "localhost:8080", "http://"} {
		_, err := New(u)
		assert.Error(t, err, u)
	}
	c, err := New("https://wn.example/prefix/")
	require.NoError(t, err)
	assert.Equal(t, "https://wn.example/prefix", c.BaseURL())
}

func TestEndpointKeepsPrefixAndEscapes(t *testing.T) {
	c, err := New("https://wn.example/wn")
	require.NoError(t, err)
	assert.Equal(t, "https://wn.example/wn/job/a%2Fb/result", c.endpoint("job", "a/b", "result").String())
}

func TestResolveRefusesOtherOrigins(t *testing.T) {
	c, err := New("https://wn.example/wn")
	require.NoError(t, err)
	u, err := c.resolve("/lease/lse_1/input")
	require.NoError(t, err)
	assert.Equal(t, "https://wn.example/wn/lease/lse_1/input", u.String())
	_, err = c.resolve("https://evil.example/lease/lse_1/input")
	assert.Error(t, err, "the token must never be sent to another host")
	u, err = c.resolve("https://wn.example/lease/x")
	require.NoError(t, err)
	assert.Equal(t, "https://wn.example/lease/x", u.String())
}

func TestErrorDecodingAndHelpers(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /job/{id}", func(w http.ResponseWriter, r *http.Request) {
		switch r.PathValue("id") {
		case "missing":
			problem(w, http.StatusNotFound, api.ProblemNotFound, "")
		case "plain":
			w.WriteHeader(http.StatusBadGateway)
			_, _ = io.WriteString(w, "<html>proxy says no</html>")
		default:
			writeJSON(w, http.StatusOK, api.Job{ID: r.PathValue("id"), State: api.JobDone})
		}
	})
	c, _ := newTestClient(t, requireAuth(mux.ServeHTTP))
	ctx := context.Background()

	job, err := c.Job(ctx, "j1")
	require.NoError(t, err)
	assert.Equal(t, api.JobDone, job.State)

	_, err = c.Job(ctx, "missing")
	require.Error(t, err)
	assert.True(t, IsNotFound(err))
	assert.False(t, IsTransient(err))
	e, ok := AsError(err)
	require.True(t, ok)
	assert.Equal(t, api.ProblemNotFound, e.Problem.Type)
	assert.Contains(t, err.Error(), "404")

	_, err = c.Job(ctx, "plain")
	assert.True(t, IsTransient(err), "a 5xx from a proxy is transient")
	e, _ = AsError(err)
	assert.Equal(t, "Bad Gateway", e.Problem.Title)
	assert.Contains(t, e.Problem.Detail, "proxy says no")

	_, err = c.WithToken("wrong").Job(ctx, "j1")
	assert.True(t, IsUnauthorized(err))
}

func TestIsTransientClassifiesNetworkAndContextErrors(t *testing.T) {
	c, err := New("http://127.0.0.1:1") // nothing listens on port 1
	require.NoError(t, err)
	_, err = c.Job(context.Background(), "x")
	assert.True(t, IsTransient(err), "connection refused: %v", err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = c.Job(ctx, "x")
	assert.False(t, IsTransient(err))
	assert.False(t, IsTransient(nil))
	assert.True(t, IsTransient(&Error{Status: 429}))
	assert.False(t, IsTransient(&Error{Status: 400}))
}

// The media part must be streamed: the server sees the params part before the
// client has produced any media byte.
func TestSubmitStreamsParamsBeforeMedia(t *testing.T) {
	paramsSeen := make(chan api.JobParams, 1)
	var gotMedia, gotKey, gotFilename string
	mux := http.NewServeMux()
	mux.HandleFunc("POST /queue/{q}", func(w http.ResponseWriter, r *http.Request) {
		mr, err := r.MultipartReader()
		require.NoError(t, err)
		p, err := mr.NextPart()
		require.NoError(t, err)
		require.Equal(t, "params", p.FormName())
		var params api.JobParams
		require.NoError(t, json.NewDecoder(p).Decode(&params))
		paramsSeen <- params
		p, err = mr.NextPart()
		require.NoError(t, err)
		require.Equal(t, "media", p.FormName())
		gotFilename = p.FileName()
		b, _ := io.ReadAll(p)
		gotMedia = string(b)
		gotKey = r.Header.Get("Idempotency-Key")
		status := http.StatusCreated
		if gotKey == "again" {
			status = http.StatusOK
		}
		w.Header().Set("Location", "/job/j1")
		writeJSON(w, status, api.Job{ID: "j1", Queue: r.PathValue("q"), State: api.JobQueued})
	})
	c, _ := newTestClient(t, requireAuth(mux.ServeHTTP))

	pr, pw := io.Pipe()
	go func() {
		select {
		case p := <-paramsSeen:
			assert.Equal(t, "small.en", p.Model)
			assert.Equal(t, api.WeightLow, p.Weight)
		case <-time.After(5 * time.Second):
			pw.CloseWithError(errors.New("params part never arrived before media"))
			return
		}
		_, _ = io.WriteString(pw, "RIFF....media bytes")
		pw.Close()
	}()
	job, created, err := c.Submit(context.Background(), "public",
		api.JobParams{Model: "small.en", Weight: api.WeightLow}, pr, SubmitOptions{IdempotencyKey: "k1", Filename: "a.wav"})
	require.NoError(t, err)
	assert.True(t, created)
	assert.Equal(t, "j1", job.ID)
	assert.Equal(t, "RIFF....media bytes", gotMedia)
	assert.Equal(t, "k1", gotKey)
	assert.Equal(t, "a.wav", gotFilename)

	go func() { <-paramsSeen }()
	_, created, err = c.Submit(context.Background(), "public", api.JobParams{Model: "small.en"},
		strings.NewReader("x"), SubmitOptions{IdempotencyKey: "again"})
	require.NoError(t, err)
	assert.False(t, created, "200 means the idempotency key matched an existing job")
}

func TestSubmitMediaReadErrorAbortsRequest(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /queue/{q}", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		writeJSON(w, http.StatusCreated, api.Job{ID: "j"})
	})
	c, _ := newTestClient(t, mux)
	_, _, err := c.Submit(context.Background(), "public", api.JobParams{Model: "small.en"},
		io.MultiReader(strings.NewReader("abc"), errReader{}), SubmitOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "disk on fire")
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("disk on fire") }

func TestListJobsQuery(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /queue/{q}", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		assert.Equal(t, "done", q.Get("state"))
		assert.Equal(t, "10", q.Get("limit"))
		assert.Equal(t, "c1", q.Get("cursor"))
		assert.Equal(t, "2026-11-15T00:00:00Z", q.Get("since"))
		writeJSON(w, http.StatusOK, api.JobList{Jobs: []api.Job{{ID: "a"}}, NextCursor: "c2"})
	})
	c, _ := newTestClient(t, mux)
	l, err := c.ListJobs(context.Background(), "public", ListOptions{
		State: api.JobDone, Limit: 10, Cursor: "c1", Since: time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)})
	require.NoError(t, err)
	assert.Equal(t, "c2", l.NextCursor)
	assert.Len(t, l.Jobs, 1)
}

func TestTranscriptStreamsAndReportsNotDone(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /job/{id}/transcript", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") == "busy" {
			problem(w, http.StatusConflict, api.ProblemNotDone, "")
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "# transcript-source: whisper\n[00:00.00] hi\n")
	})
	c, _ := newTestClient(t, mux)
	rc, err := c.Transcript(context.Background(), "j")
	require.NoError(t, err)
	b, _ := io.ReadAll(rc)
	rc.Close()
	assert.Contains(t, string(b), "[00:00.00] hi")

	_, err = c.Transcript(context.Background(), "busy")
	assert.True(t, IsNotDone(err))
}

func TestDeleteAndClients(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /job/{id}", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("POST /client", func(w http.ResponseWriter, r *http.Request) {
		var in api.CreateClientRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&in))
		writeJSON(w, http.StatusCreated, api.Client{ID: "c1", Name: in.Name, Grants: in.Grants, Token: "wnc_x"})
	})
	mux.HandleFunc("DELETE /client/{id}", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	c, _ := newTestClient(t, mux)
	ctx := context.Background()
	require.NoError(t, c.DeleteJob(ctx, "j"))
	cl, err := c.CreateClient(ctx, api.CreateClientRequest{Name: "h", Grants: []api.Grant{{Role: api.RoleSubmit, Scope: "queue:public"}}})
	require.NoError(t, err)
	assert.Equal(t, "c1", cl.ID)
	require.NoError(t, c.RevokeClient(ctx, "c1"))
}

// TestRedirectsStayOnOrigin: a redirect that changes scheme, host or port is
// refused before any request reaches the new location, so the bearer token
// cannot leak there (net/http forwards it on same-host redirects, including
// https to plain http). Same-origin redirects still work.
func TestRedirectsStayOnOrigin(t *testing.T) {
	var elsewhere atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		elsewhere.Add(1)
		if r.Header.Get("Authorization") != "" {
			t.Errorf("token reached another origin: %s", r.URL)
		}
	}))
	t.Cleanup(other.Close)
	plain := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		elsewhere.Add(1)
		if r.Header.Get("Authorization") != "" {
			t.Errorf("token sent over plain http: %s", r.URL)
		}
	}))

	var target string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/job/away":
			http.Redirect(w, r, target, http.StatusFound)
		case "/job/moved":
			http.Redirect(w, r, "/job/here", http.StatusFound)
		case "/job/here":
			requireAuth(func(w http.ResponseWriter, r *http.Request) {
				writeJSON(w, http.StatusOK, api.Job{ID: "here"})
			})(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	plain.Start()
	t.Cleanup(plain.Close)

	// A default http.Client: WithHTTPClient must not be needed for safety.
	tr := srv.Client().Transport
	c, err := New(srv.URL, WithToken(testToken), WithHTTPClient(&http.Client{Transport: tr}))
	require.NoError(t, err)

	j, err := c.Job(context.Background(), "moved")
	require.NoError(t, err, "same-origin redirects are followed")
	assert.Equal(t, "here", j.ID)

	tlsURL, _ := url.Parse(srv.URL)
	for name, to := range map[string]string{
		"https to http, same host and port": "http://" + tlsURL.Host + "/job/here",
		"other port":                        other.URL + "/job/here",
		"plain http elsewhere":              plain.URL + "/job/here",
	} {
		t.Run(name, func(t *testing.T) {
			target = to
			_, err := c.Job(context.Background(), "away")
			require.ErrorIs(t, err, ErrRedirectRefused)
			assert.False(t, IsTransient(err), "a refused redirect is not retried")
		})
	}
	assert.Zero(t, elsewhere.Load(), "no request left the origin")
}

func TestDefaultHTTPClientIsPrivate(t *testing.T) {
	c, err := New("https://wn.example")
	require.NoError(t, err)
	assert.NotSame(t, http.DefaultClient, c.hc)
	assert.NotNil(t, c.hc.CheckRedirect)
	hc := &http.Client{}
	c, err = New("https://wn.example", WithHTTPClient(hc))
	require.NoError(t, err)
	assert.Nil(t, hc.CheckRedirect, "the caller's client is not modified")
	assert.NotNil(t, c.hc.CheckRedirect)
}

func TestSubmitURL(t *testing.T) {
	var gotKey, gotCT string
	var gotBody api.FetchRequest
	mux := http.NewServeMux()
	mux.HandleFunc("POST /queue/{q}/fetch", func(w http.ResponseWriter, r *http.Request) {
		gotKey, gotCT = r.Header.Get("Idempotency-Key"), r.Header.Get("Content-Type")
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.Header().Set("Location", "/job/j9")
		writeJSON(w, http.StatusCreated, api.Job{ID: "j9", State: api.JobFetching})
	})
	mux.HandleFunc("POST /queue/nodl/fetch", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", api.ProblemContentType)
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(api.Problem{Type: "no-downloader", Title: "Conflict", Status: 409, Detail: "no downloader online"})
	})
	c, _ := newTestClient(t, mux)
	job, err := c.SubmitURL(context.Background(), "public",
		api.FetchRequest{JobParams: api.JobParams{Model: "small.en", Label: "x"}, URL: "https://example.com/a"}, "k2")
	require.NoError(t, err)
	assert.Equal(t, "j9", job.ID)
	assert.Equal(t, "k2", gotKey)
	assert.Equal(t, "application/json", gotCT)
	assert.Equal(t, "https://example.com/a", gotBody.URL)
	assert.Equal(t, "small.en", gotBody.Model)

	_, err = c.SubmitURL(context.Background(), "nodl", api.FetchRequest{URL: "https://example.com/a"}, "")
	var ce *Error
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, "no-downloader", ce.Problem.Type)
	assert.Contains(t, err.Error(), "no downloader online")
}

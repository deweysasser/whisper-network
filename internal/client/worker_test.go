package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/backoff"
)

func TestRegisterNeedsNoToken(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /worker", func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(t, r.Header.Get("Authorization"))
		var f api.WorkerFacts
		require.NoError(t, json.NewDecoder(r.Body).Decode(&f))
		assert.Equal(t, "nuc-01", f.Hostname)
		writeJSON(w, http.StatusCreated, api.RegisterResponse{ID: "w1", Token: "wnw_secret", State: api.WorkerPending})
	})
	_, srv := newTestClient(t, mux)
	c, err := New(srv.URL)
	require.NoError(t, err)
	reg, err := c.Register(context.Background(), api.WorkerFacts{Hostname: "nuc-01"})
	require.NoError(t, err)
	assert.Equal(t, "w1", reg.ID)
	assert.Equal(t, api.WorkerPending, reg.State)
}

func TestClaim(t *testing.T) {
	var calls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("POST /worker/{id}/leases", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "30s", r.URL.Query().Get("wait"))
		var req api.ClaimRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Equal(t, []string{"small.en"}, req.Models)
		switch calls.Add(1) {
		case 1:
			w.WriteHeader(http.StatusNoContent)
		case 2:
			writeJSON(w, http.StatusCreated, api.Lease{ID: "lse_1", Kind: api.KindTranscribe, HeartbeatIntervalS: 30})
		case 3:
			problem(w, http.StatusConflict, api.ProblemWorkerPaused, "paused")
		case 4:
			problem(w, http.StatusForbidden, api.ProblemWorkerPending, "pending")
		default:
			problem(w, http.StatusGone, api.ProblemWorkerRejected, "rejected")
		}
	})
	c, _ := newTestClient(t, requireAuth(mux.ServeHTTP))
	ctx := context.Background()
	req := api.ClaimRequest{Kinds: []api.TaskKind{api.KindTranscribe}, Models: []string{"small.en"}}

	l, err := c.Claim(ctx, "w1", req, 30*time.Second)
	assert.NoError(t, err)
	assert.Nil(t, l, "204 is (nil, nil)")

	l, err = c.Claim(ctx, "w1", req, 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, "lse_1", l.ID)

	_, err = c.Claim(ctx, "w1", req, 30*time.Second)
	assert.True(t, IsPaused(err))
	_, err = c.Claim(ctx, "w1", req, 30*time.Second)
	assert.True(t, IsPending(err))
	_, err = c.Claim(ctx, "w1", req, 30*time.Second)
	assert.True(t, IsRejected(err))
	assert.False(t, IsPaused(err))
}

func TestHeartbeatAndLeaseGone(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /lease/{id}/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		var hb api.HeartbeatRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&hb))
		if r.PathValue("id") == "gone" {
			problem(w, http.StatusGone, api.ProblemLeaseGone, "")
			return
		}
		require.NotNil(t, hb.Gauges)
		assert.Equal(t, 2, hb.Gauges.SlotsBusy)
		writeJSON(w, http.StatusOK, api.HeartbeatResponse{ExpiresAt: time.Unix(100, 0).UTC()})
	})
	c, _ := newTestClient(t, mux)
	resp, err := c.Heartbeat(context.Background(), "live", &api.Gauges{SlotsBusy: 2})
	require.NoError(t, err)
	assert.Equal(t, int64(100), resp.ExpiresAt.Unix())

	_, err = c.Heartbeat(context.Background(), "gone", nil)
	assert.True(t, IsLeaseGone(err))
	assert.False(t, IsRejected(err))
	assert.False(t, IsTransient(err))
}

func TestResultsFailureAndRelease(t *testing.T) {
	var got struct {
		flac, ct, computeMS, ffv string
		tr                       api.TranscribeResult
		fail                     api.FailureReport
		released                 string
	}
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /lease/{id}/result", func(w http.ResponseWriter, r *http.Request) {
		got.ct = r.Header.Get("Content-Type")
		if got.ct == "audio/flac" {
			b, _ := io.ReadAll(r.Body)
			got.flac = string(b)
			got.computeMS = r.Header.Get(api.HeaderComputeMS)
			got.ffv = r.Header.Get(api.HeaderFFmpegVersion)
			assert.Equal(t, int64(4), r.ContentLength)
		} else {
			require.NoError(t, json.NewDecoder(r.Body).Decode(&got.tr))
		}
		if r.PathValue("id") == "bad" {
			problem(w, http.StatusUnprocessableEntity, api.ProblemResultInvalid, "")
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /lease/{id}/failure", func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got.fail))
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("DELETE /lease/{id}", func(w http.ResponseWriter, r *http.Request) {
		got.released = r.PathValue("id")
		w.WriteHeader(http.StatusNoContent)
	})
	c, _ := newTestClient(t, mux)
	ctx := context.Background()

	require.NoError(t, c.PutPrepareResult(ctx, "l1", PrepareResult{Body: strings.NewReader("fLaC"), Size: 4, ComputeMS: 1234, FFmpegVersion: "7.1.1"}))
	assert.Equal(t, "fLaC", got.flac)
	assert.Equal(t, "1234", got.computeMS)
	assert.Equal(t, "7.1.1", got.ffv)

	res := &api.TranscribeResult{Model: "small.en", Segments: []api.Segment{{StartMS: 0, EndMS: 10, Text: "x"}}}
	require.NoError(t, c.PutTranscribeResult(ctx, "l2", res))
	assert.Equal(t, "application/json", got.ct)
	assert.Equal(t, *res, got.tr)
	err := c.PutTranscribeResult(ctx, "bad", res)
	assert.True(t, IsResultInvalid(err))

	require.NoError(t, c.ReportFailure(ctx, "l3", api.FailureReport{Reason: "boom", Retryable: true}))
	assert.Equal(t, "boom", got.fail.Reason)
	require.NoError(t, c.Release(ctx, "l4"))
	assert.Equal(t, "l4", got.released)
}

func TestModelDownloadCarriesETag(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /models/{name}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"abc123"`)
		w.Header().Set("Content-Length", "5")
		_, _ = io.WriteString(w, "ggml!")
	})
	c, _ := newTestClient(t, mux)
	d, err := c.Model(context.Background(), "small.en")
	require.NoError(t, err)
	defer d.Body.Close()
	assert.Equal(t, "abc123", d.ETag)
	assert.Equal(t, int64(5), d.Size)
}

// flakyInput serves content, cutting the connection after cut bytes on the
// first `failures` requests. With ranged=false it ignores Range headers, like
// the transcribe input endpoint.
type flakyInput struct {
	content  []byte
	cut      int
	failures int32
	ranged   bool
	requests atomic.Int32
	ranges   []string
}

func (f *flakyInput) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n := f.requests.Add(1)
	f.ranges = append(f.ranges, r.Header.Get("Range"))
	body, start := f.content, 0
	if rg := r.Header.Get("Range"); rg != "" && f.ranged {
		_, _ = fmt.Sscanf(rg, "bytes=%d-", &start)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(f.content)-1, len(f.content)))
		w.Header().Set("Content-Length", fmt.Sprint(len(f.content)-start))
		w.WriteHeader(http.StatusPartialContent)
		body = f.content[start:]
	} else {
		w.Header().Set("Content-Length", fmt.Sprint(len(f.content)))
	}
	if n <= f.failures {
		_, _ = w.Write(body[:f.cut])
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	}
	_, _ = w.Write(body)
}

func downloadTo(t *testing.T, c *Client, inputURL string) ([]byte, error) {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "in"))
	require.NoError(t, err)
	defer f.Close()
	var retries int
	_, err = c.DownloadInput(context.Background(), "l1", inputURL, f, ResumeOptions{
		Retry:   backoff.Policy{Initial: time.Millisecond, Max: time.Millisecond},
		OnRetry: func(int, int64, error) { retries++ },
	})
	if err != nil {
		return nil, err
	}
	return os.ReadFile(f.Name())
}

func TestDownloadInputResumesWithRange(t *testing.T) {
	content := bytes.Repeat([]byte("0123456789"), 10000)
	fi := &flakyInput{content: content, cut: 30000, failures: 2, ranged: true}
	c, _ := newTestClient(t, fi)
	got, err := downloadTo(t, c, "/lease/l1/input")
	require.NoError(t, err)
	assert.Equal(t, content, got)
	assert.Equal(t, []string{"", "bytes=30000-", "bytes=60000-"}, fi.ranges)
}

func TestDownloadInputRestartsWhenRangeIgnored(t *testing.T) {
	content := bytes.Repeat([]byte("abcdefgh"), 4096)
	fi := &flakyInput{content: content, cut: 1000, failures: 1, ranged: false}
	c, _ := newTestClient(t, fi)
	got, err := downloadTo(t, c, "")
	require.NoError(t, err)
	assert.Equal(t, content, got, "restarted download must not keep the partial prefix twice")
	assert.Equal(t, "bytes=1000-", fi.ranges[1])
}

func TestDownloadInputGivesUpWithoutProgress(t *testing.T) {
	fi := &flakyInput{content: []byte("hello world"), cut: 0, failures: 100, ranged: true}
	c, _ := newTestClient(t, fi)
	_, err := downloadTo(t, c, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "giving up after 5 attempts")
}

func TestDownloadInputStopsOnLeaseGone(t *testing.T) {
	var n atomic.Int32
	c, _ := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		problem(w, http.StatusGone, api.ProblemLeaseGone, "")
	}))
	_, err := downloadTo(t, c, "")
	assert.True(t, IsLeaseGone(err))
	assert.Equal(t, int32(1), n.Load(), "410 is not retried")
}

// failingWriter is a ResumableFile whose writes fail like a full disk.
type failingWriter struct{ f *os.File }

func (failingWriter) Write([]byte) (int, error)               { return 0, errors.New("no space left on device") }
func (w failingWriter) Seek(off int64, wh int) (int64, error) { return w.f.Seek(off, wh) }
func (w failingWriter) Truncate(n int64) error                { return w.f.Truncate(n) }

func TestDownloadInputWriteErrorIsNotRetried(t *testing.T) {
	fi := &flakyInput{content: []byte("hello world")}
	c, _ := newTestClient(t, fi)
	f, err := os.Create(filepath.Join(t.TempDir(), "in"))
	require.NoError(t, err)
	defer f.Close()
	_, err = c.DownloadInput(context.Background(), "l1", "", failingWriter{f}, ResumeOptions{})
	require.Error(t, err)
	assert.False(t, IsTransient(err))
	assert.Equal(t, int32(1), fi.requests.Load())
}

func TestParseContentRange(t *testing.T) {
	s, total, err := parseContentRange("bytes 10-19/20")
	require.NoError(t, err)
	assert.Equal(t, int64(10), s)
	assert.Equal(t, int64(20), total)
	_, total, err = parseContentRange("bytes 10-19/*")
	require.NoError(t, err)
	assert.Equal(t, int64(-1), total)
	for _, bad := range []string{"", "bytes 10-5/20", "bytes 0-20/20", "items 0-1/2"} {
		_, _, err := parseContentRange(bad)
		assert.Error(t, err, bad)
	}
}

func downloadCapped(t *testing.T, c *Client, max int64) (int64, error) {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "in"))
	require.NoError(t, err)
	defer f.Close()
	n, err := c.DownloadInput(context.Background(), "l1", "", f, ResumeOptions{
		Retry: backoff.Policy{Initial: time.Millisecond, Max: time.Millisecond}, MaxBytes: max,
	})
	fi, statErr := f.Stat()
	require.NoError(t, statErr)
	assert.LessOrEqual(t, fi.Size(), max+1, "never writes more than one byte past the cap")
	return n, err
}

func TestDownloadInputCap(t *testing.T) {
	content := bytes.Repeat([]byte("x"), 1000)
	t.Run("exactly the cap", func(t *testing.T) {
		c, _ := newTestClient(t, &flakyInput{content: content})
		n, err := downloadCapped(t, c, 1000)
		require.NoError(t, err)
		assert.EqualValues(t, 1000, n)
	})
	t.Run("advertised size over the cap", func(t *testing.T) {
		fi := &flakyInput{content: content}
		c, _ := newTestClient(t, fi)
		_, err := downloadCapped(t, c, 999)
		require.ErrorIs(t, err, ErrInputTooLarge)
		assert.False(t, IsTransient(err))
		assert.Equal(t, int32(1), fi.requests.Load(), "not retried")
	})
	t.Run("unannounced body over the cap", func(t *testing.T) {
		var n atomic.Int32
		c, _ := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n.Add(1)
			w.(http.Flusher).Flush() // chunked: no Content-Length
			for range 100 {
				_, _ = w.Write(content)
			}
		}))
		_, err := downloadCapped(t, c, 5000)
		require.ErrorIs(t, err, ErrInputTooLarge)
		assert.Equal(t, int32(1), n.Load(), "not retried")
	})
	t.Run("resumed download keeps counting", func(t *testing.T) {
		fi := &flakyInput{content: content, cut: 600, failures: 1, ranged: true}
		c, _ := newTestClient(t, fi)
		_, err := downloadCapped(t, c, 900)
		require.ErrorIs(t, err, ErrInputTooLarge)
	})
}

func TestPutFetchResult(t *testing.T) {
	var got struct {
		ct, method, body string
		te               []string
		cl               int64
	}
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /lease/{id}/result", requireAuth(func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") == "moved" {
			http.Redirect(w, r, "/lease/l1/result", http.StatusTemporaryRedirect)
			return
		}
		b, _ := io.ReadAll(r.Body)
		got.ct, got.method, got.body = r.Header.Get("Content-Type"), r.Header.Get(api.HeaderFetchMethod), string(b)
		got.te, got.cl = r.TransferEncoding, r.ContentLength
		w.WriteHeader(http.StatusOK)
	}))
	c, _ := newTestClient(t, mux)
	ctx := context.Background()

	// A reader of unknown length is sent chunked.
	require.NoError(t, c.PutFetchResult(ctx, "l1", api.FetchMethodYtDlp, io.MultiReader(strings.NewReader("ID3"), strings.NewReader("media"))))
	assert.Equal(t, "application/octet-stream", got.ct)
	assert.Equal(t, api.FetchMethodYtDlp, got.method)
	assert.Equal(t, "ID3media", got.body)
	assert.Equal(t, []string{"chunked"}, got.te)
	assert.Equal(t, int64(-1), got.cl)

	got.body = ""
	err := c.PutFetchResult(ctx, "moved", api.FetchMethodHTTP, strings.NewReader("x"))
	require.Error(t, err, "redirects are not followed")
	assert.Empty(t, got.body)
}

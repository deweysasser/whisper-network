package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
)

// Register registers a new worker. It needs no credentials; the response
// carries the worker's ID and token, shown only this once.
func (c *Client) Register(ctx context.Context, facts api.WorkerFacts) (*api.RegisterResponse, error) {
	var out api.RegisterResponse
	if _, err := c.doJSON(ctx, http.MethodPost, c.endpoint("worker"), facts, &out, http.StatusCreated); err != nil {
		return nil, err
	}
	return &out, nil
}

// Worker returns a worker's state, grants and the server's timing hints.
func (c *Client) Worker(ctx context.Context, id string) (*api.Worker, error) {
	var out api.Worker
	if _, err := c.doJSON(ctx, http.MethodGet, c.endpoint("worker", id), nil, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// PatchWorker replaces the worker's reported facts.
func (c *Client) PatchWorker(ctx context.Context, id string, facts api.WorkerFacts) error {
	_, err := c.doJSON(ctx, http.MethodPatch, c.endpoint("worker", id), facts, nil,
		http.StatusOK, http.StatusNoContent)
	return err
}

// Claim long-polls for one task for worker id, waiting up to wait on the
// server. It returns (nil, nil) when the server times out with 204. The
// caller's context should outlive wait by a margin.
func (c *Client) Claim(ctx context.Context, id string, req api.ClaimRequest, wait time.Duration) (*api.Lease, error) {
	u := c.endpoint("worker", id, "leases")
	if wait > 0 {
		u.RawQuery = url.Values{"wait": {wait.String()}}.Encode()
	}
	var out api.Lease
	status, err := c.doJSON(ctx, http.MethodPost, u, req, &out, http.StatusCreated, http.StatusNoContent)
	if err != nil || status == http.StatusNoContent {
		return nil, err
	}
	return &out, nil
}

// ReportGauges sends worker id's gauges without claiming: an idle worker
// that is not claiming (its host is busy) stays seen and current.
func (c *Client) ReportGauges(ctx context.Context, id string, g *api.Gauges) error {
	_, err := c.doJSON(ctx, http.MethodPost, c.endpoint("worker", id, "gauges"), g, nil, http.StatusNoContent)
	return err
}

// Heartbeat extends a lease, reporting gauges if non-nil. A lease that is no
// longer live yields an error for which IsLeaseGone is true.
func (c *Client) Heartbeat(ctx context.Context, leaseID string, g *api.Gauges) (*api.HeartbeatResponse, error) {
	var out api.HeartbeatResponse
	_, err := c.doJSON(ctx, http.MethodPost, c.endpoint("lease", leaseID, "heartbeat"),
		api.HeartbeatRequest{Gauges: g}, &out, http.StatusOK)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// PutTranscribeResult uploads a transcribe lease's segments. It is
// idempotent for the same lease and body, so it may be retried.
func (c *Client) PutTranscribeResult(ctx context.Context, leaseID string, res *api.TranscribeResult) error {
	_, err := c.doJSON(ctx, http.MethodPut, c.endpoint("lease", leaseID, "result"), res, nil,
		http.StatusOK, http.StatusCreated, http.StatusNoContent)
	return err
}

// PrepareResult is the canonical FLAC produced by a prepare lease.
type PrepareResult struct {
	Body          io.Reader
	Size          int64 // Content-Length; < 0 if unknown
	ComputeMS     int64
	FFmpegVersion string
}

// PutPrepareResult uploads a prepare lease's FLAC as audio/flac. To retry,
// call again with a fresh Body.
func (c *Client) PutPrepareResult(ctx context.Context, leaseID string, res PrepareResult) error {
	req, err := c.newRequest(ctx, http.MethodPut, c.endpoint("lease", leaseID, "result"), res.Body)
	if err != nil {
		return err
	}
	if res.Size >= 0 {
		req.ContentLength = res.Size
		if res.Size == 0 {
			req.Body = http.NoBody
		}
	}
	req.Header.Set("Content-Type", "audio/flac")
	req.Header.Set(api.HeaderComputeMS, strconv.FormatInt(res.ComputeMS, 10))
	if res.FFmpegVersion != "" {
		req.Header.Set(api.HeaderFFmpegVersion, res.FFmpegVersion)
	}
	resp, err := c.send(req, http.StatusOK, http.StatusCreated, http.StatusNoContent)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	return resp.Body.Close()
}

// PutFetchResult streams a fetch lease's media as application/octet-stream,
// chunked (its length is unknown until the end), with X-Fetch-Method set to
// method when non-empty. body is read once, so the upload is never retried
// and redirects are not followed. To abort the upload so the server sees a
// truncated body rather than a short one, cancel ctx or make body's Read
// fail: either way the chunked body is never terminated.
func (c *Client) PutFetchResult(ctx context.Context, leaseID, method string, body io.Reader) error {
	req, err := c.newRequest(ctx, http.MethodPut, c.endpoint("lease", leaseID, "result"), body)
	if err != nil {
		return err
	}
	req.ContentLength = -1
	req.Header.Set("Content-Type", "application/octet-stream")
	if method != "" {
		req.Header.Set(api.HeaderFetchMethod, method)
	}
	hc := *c.hc
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := c.sendWith(&hc, req, http.StatusOK, http.StatusCreated, http.StatusNoContent)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	return resp.Body.Close()
}

// ReportFailure ends a lease as failed. The stderr tail is truncated to
// api.MaxStderrTail by the caller's responsibility.
func (c *Client) ReportFailure(ctx context.Context, leaseID string, rep api.FailureReport) error {
	_, err := c.doJSON(ctx, http.MethodPost, c.endpoint("lease", leaseID, "failure"), rep, nil,
		http.StatusOK, http.StatusNoContent)
	return err
}

// Release voluntarily gives a lease back (graceful shutdown). It does not
// consume one of the task's attempts.
func (c *Client) Release(ctx context.Context, leaseID string) error {
	_, err := c.doJSON(ctx, http.MethodDelete, c.endpoint("lease", leaseID), nil, nil,
		http.StatusNoContent, http.StatusOK)
	return err
}

// Model streams a model file. Download.ETag carries the server's sha256; the
// caller must still verify the bytes against its pinned hash.
func (c *Client) Model(ctx context.Context, name string) (*Download, error) {
	return c.open(ctx, c.endpoint("models", name), 0)
}

// OpenInput streams a lease's input starting at byte offset. A server that
// ignores the range answers 200 from byte 0; Download.Offset says where the
// body really starts. inputURL is the lease's input_url (or "" to build it
// from leaseID).
func (c *Client) OpenInput(ctx context.Context, leaseID, inputURL string, offset int64) (*Download, error) {
	u, err := c.inputURL(leaseID, inputURL)
	if err != nil {
		return nil, err
	}
	return c.open(ctx, u, offset)
}

// CheckInputURL reports whether DownloadInput would fetch a lease's
// input_url: an absolute URL must be on the server's origin.
func (c *Client) CheckInputURL(leaseID, inputURL string) error {
	_, err := c.inputURL(leaseID, inputURL)
	return err
}

func (c *Client) inputURL(leaseID, ref string) (*url.URL, error) {
	if ref == "" {
		return c.endpoint("lease", leaseID, "input"), nil
	}
	return c.resolve(ref)
}

// Download is a streamed response body. The caller must close Body.
type Download struct {
	Body io.ReadCloser
	// Offset is the position of Body's first byte in the whole resource.
	Offset int64
	// Size is the whole resource's length, or -1 if the server did not say.
	Size int64
	// ETag is the response's entity tag, unquoted.
	ETag string
}

func (c *Client) open(ctx context.Context, u *url.URL, offset int64) (*Download, error) {
	req, err := c.newRequest(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	resp, err := c.send(req, http.StatusOK, http.StatusPartialContent)
	if err != nil {
		return nil, err
	}
	d := &Download{Body: resp.Body, Size: resp.ContentLength, ETag: unquote(resp.Header.Get("ETag"))}
	if resp.StatusCode == http.StatusPartialContent {
		start, total, err := parseContentRange(resp.Header.Get("Content-Range"))
		if err != nil {
			resp.Body.Close()
			return nil, err
		}
		d.Offset, d.Size = start, total
	}
	return d, nil
}

// parseContentRange parses "bytes start-end/total"; total may be "*" (-1).
func parseContentRange(h string) (start, total int64, err error) {
	var end int64
	var totalStr string
	if _, err := fmt.Sscanf(h, "bytes %d-%d/%s", &start, &end, &totalStr); err != nil {
		return 0, 0, fmt.Errorf("bad Content-Range %q", h)
	}
	if totalStr == "*" {
		return start, -1, nil
	}
	total, err = strconv.ParseInt(totalStr, 10, 64)
	if err != nil || end < start || total <= end {
		return 0, 0, fmt.Errorf("bad Content-Range %q", h)
	}
	return start, total, nil
}

func unquote(etag string) string {
	if len(etag) >= 2 && etag[0] == '"' && etag[len(etag)-1] == '"' {
		return etag[1 : len(etag)-1]
	}
	return etag
}

package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
)

// SubmitOptions are the optional parts of a job submission.
type SubmitOptions struct {
	// IdempotencyKey makes a retried submission return the original job
	// instead of creating a duplicate (DESIGN.md §6.1).
	IdempotencyKey string
	// Filename is sent as the media part's filename. It is informational;
	// the server sniffs the content.
	Filename string
}

// Submit creates a job on queue, streaming media without buffering it: the
// multipart body is produced through a pipe, the params part first and then
// the media part. created is false when the server returned an existing job
// for the same idempotency key (200 instead of 201).
func (c *Client) Submit(ctx context.Context, queue string, params api.JobParams, media io.Reader, opt SubmitOptions) (job *api.Job, created bool, err error) {
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return nil, false, err
	}
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	go func() { pw.CloseWithError(writeSubmitBody(mw, paramsJSON, media, opt.Filename)) }()
	// Unblock the writer goroutine if the request never reads the body.
	defer pr.Close()

	req, err := c.newRequest(ctx, http.MethodPost, c.endpoint("queue", queue), pr)
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Accept", "application/json")
	if opt.IdempotencyKey != "" {
		req.Header.Set("Idempotency-Key", opt.IdempotencyKey)
	}
	resp, err := c.send(req, http.StatusCreated, http.StatusOK)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()
	job = new(api.Job)
	if err := json.NewDecoder(resp.Body).Decode(job); err != nil {
		return nil, false, fmt.Errorf("submit: decoding job: %w", err)
	}
	return job, resp.StatusCode == http.StatusCreated, nil
}

// SubmitURL asks the server to have a downloader worker fetch req.URL into a
// new job on queue. The server never fetches the URL itself; with no eligible
// downloader it answers 409 (problem type no-downloader), returned as *Error.
// A 200 means idempotencyKey matched an existing job.
func (c *Client) SubmitURL(ctx context.Context, queue string, req api.FetchRequest, idempotencyKey string) (*api.Job, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	hr, err := c.newRequest(ctx, http.MethodPost, c.endpoint("queue", queue, "fetch"), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	hr.Header.Set("Content-Type", "application/json")
	hr.Header.Set("Accept", "application/json")
	if idempotencyKey != "" {
		hr.Header.Set("Idempotency-Key", idempotencyKey)
	}
	resp, err := c.send(hr, http.StatusCreated, http.StatusOK)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	job := new(api.Job)
	if err := json.NewDecoder(resp.Body).Decode(job); err != nil {
		return nil, fmt.Errorf("submit url: decoding job: %w", err)
	}
	return job, nil
}

func writeSubmitBody(mw *multipart.Writer, params []byte, media io.Reader, filename string) error {
	h := textproto.MIMEHeader{}
	h.Set("Content-Disposition", `form-data; name="params"`)
	h.Set("Content-Type", "application/json")
	pw, err := mw.CreatePart(h)
	if err != nil {
		return err
	}
	if _, err := pw.Write(params); err != nil {
		return err
	}
	if filename == "" {
		filename = "media"
	}
	h = textproto.MIMEHeader{}
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="media"; filename=%q`, filename))
	h.Set("Content-Type", "application/octet-stream")
	mpw, err := mw.CreatePart(h)
	if err != nil {
		return err
	}
	if _, err := io.Copy(mpw, media); err != nil {
		return fmt.Errorf("reading media: %w", err)
	}
	return mw.Close()
}

// ListOptions filter GET /queue/{q}. Zero values are omitted.
type ListOptions struct {
	State api.JobState
	Since time.Time
	// Sort is "state" or "created" (the server default); Order is "asc"
	// (the default) or "desc", for Sort "created" only.
	Sort, Order string
	Limit       int
	Cursor      string
}

// ListJobs lists the jobs on queue visible to the caller, one page at a time.
func (c *Client) ListJobs(ctx context.Context, queue string, opt ListOptions) (*api.JobList, error) {
	u := c.endpoint("queue", queue)
	q := url.Values{}
	if opt.State != "" {
		q.Set("state", string(opt.State))
	}
	if !opt.Since.IsZero() {
		q.Set("since", opt.Since.UTC().Format(time.RFC3339Nano))
	}
	if opt.Sort != "" {
		q.Set("sort", opt.Sort)
	}
	if opt.Order != "" {
		q.Set("order", opt.Order)
	}
	if opt.Limit > 0 {
		q.Set("limit", strconv.Itoa(opt.Limit))
	}
	if opt.Cursor != "" {
		q.Set("cursor", opt.Cursor)
	}
	u.RawQuery = q.Encode()
	var out api.JobList
	if _, err := c.doJSON(ctx, http.MethodGet, u, nil, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// PatchQueue changes a queue's weight, description or upload limit.
func (c *Client) PatchQueue(ctx context.Context, queue string, patch api.QueuePatch) (*api.Queue, error) {
	var out api.Queue
	if _, err := c.doJSON(ctx, http.MethodPatch, c.endpoint("queue", queue), patch, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// Job returns a job's status document.
func (c *Client) Job(ctx context.Context, id string) (*api.Job, error) {
	var out api.Job
	if _, err := c.doJSON(ctx, http.MethodGet, c.endpoint("job", id), nil, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// Transcript streams a finished job's text/plain transcript, provenance
// header included. The caller must close the reader. A job that is not done
// yields an error for which IsNotDone is true.
func (c *Client) Transcript(ctx context.Context, id string) (io.ReadCloser, error) {
	return c.transcript(ctx, id, false)
}

// PartialTranscript is Transcript, except that a job that is not done
// yields what is transcribed so far, each gap marked and the header ending
// in "; partial=true; missing=K/N" (a done job's transcript is complete).
func (c *Client) PartialTranscript(ctx context.Context, id string) (io.ReadCloser, error) {
	return c.transcript(ctx, id, true)
}

func (c *Client) transcript(ctx context.Context, id string, partial bool) (io.ReadCloser, error) {
	u := c.endpoint("job", id, "transcript")
	if partial {
		u.RawQuery = "partial=1"
	}
	req, err := c.newRequest(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/plain")
	resp, err := c.send(req, http.StatusOK)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// Result returns a finished job's segments with absolute times.
func (c *Client) Result(ctx context.Context, id string) (*api.JobResult, error) {
	return c.result(ctx, id, false)
}

// PartialResult is Result, except that a job that is not done yields the
// segments so far with Partial set and the gaps in Missing.
func (c *Client) PartialResult(ctx context.Context, id string) (*api.JobResult, error) {
	return c.result(ctx, id, true)
}

func (c *Client) result(ctx context.Context, id string, partial bool) (*api.JobResult, error) {
	u := c.endpoint("job", id, "result")
	if partial {
		u.RawQuery = "partial=1"
	}
	var out api.JobResult
	if _, err := c.doJSON(ctx, http.MethodGet, u, nil, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// RetryJob retries a failed job: done tasks are kept, the rest run again.
// A job that is not failed, or whose media was purged, is a 409
// not-retryable error.
func (c *Client) RetryJob(ctx context.Context, id string) (*api.Job, error) {
	var out api.Job
	if _, err := c.doJSON(ctx, http.MethodPost, c.endpoint("job", id, "retry"), nil, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// Bulk retries or deletes many jobs of queue (POST /queue/{q}/bulk). A
// filter selection larger than api.MaxBulkJobs returns NextCursor; send
// the request again with Filter.Cursor set to continue.
func (c *Client) Bulk(ctx context.Context, queue string, req api.BulkRequest) (*api.BulkResult, error) {
	var out api.BulkResult
	if _, err := c.doJSON(ctx, http.MethodPost, c.endpoint("queue", queue, "bulk"), req, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// DeleteJob deletes a job, its media and its live leases.
func (c *Client) DeleteJob(ctx context.Context, id string) error {
	_, err := c.doJSON(ctx, http.MethodDelete, c.endpoint("job", id), nil, nil, http.StatusNoContent, http.StatusOK)
	return err
}

// CreateClient creates an API client holding a subset of the caller's grants.
// The returned Token is shown only this once.
func (c *Client) CreateClient(ctx context.Context, in api.CreateClientRequest) (*api.Client, error) {
	var out api.Client
	if _, err := c.doJSON(ctx, http.MethodPost, c.endpoint("client"), in, &out, http.StatusCreated, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetClient returns an API client's details.
func (c *Client) GetClient(ctx context.Context, id string) (*api.Client, error) {
	var out api.Client
	if _, err := c.doJSON(ctx, http.MethodGet, c.endpoint("client", id), nil, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// RevokeClient revokes an API client's token.
func (c *Client) RevokeClient(ctx context.Context, id string) error {
	_, err := c.doJSON(ctx, http.MethodDelete, c.endpoint("client", id), nil, nil, http.StatusNoContent, http.StatusOK)
	return err
}

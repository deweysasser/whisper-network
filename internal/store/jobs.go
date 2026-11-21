package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
)

// taskCancelled is the internal state of a task that will not run because
// its job failed: it was pending (or its lease ended without a result) after
// a sibling task failed for good, or the job was failed outright. TaskCounts
// reports such tasks as Cancelled.
const taskCancelled = "cancelled"

// Media describes an uploaded original.
type Media struct {
	Bytes  int64
	SHA256 string
	Type   string // sniffed media type
}

// Canonical describes canonical audio (16 kHz mono 16-bit FLAC).
type Canonical struct {
	Bytes   int64
	SHA256  string
	Samples int64
}

// SampleRange is a half-open range [Start, End) of canonical samples: the
// input of one transcribe task.
type SampleRange struct {
	Start, End int64
}

// NewJob holds the fields of a job to create.
type NewJob struct {
	// ID is optional; when empty the store generates one. Servers that stream
	// the upload to disk first take it from Store.NewJobID.
	ID             string
	QueueID        int64
	ClientID       string // owner principal id
	IdempotencyKey string // optional
	Params         api.JobParams
	Original       Media
	// Canonical is nil when the upload needs a prepare task. When set, Ranges
	// must split it into transcribe tasks and the job starts queued.
	Canonical *Canonical
	Ranges    []SampleRange
}

// Job is a stored job plus the derived data needed for api.Job.
type Job struct {
	ID                string
	QueueID           int64
	QueueName         string
	ClientID          string
	IdempotencyKey    string
	State             api.JobState
	Weight            api.Weight
	Model             string
	Language          string
	Prompt            string
	Label             string
	Source            string
	Metadata          json.RawMessage
	Original          Media
	OriginalPurgedAt  *time.Time
	Canonical         *Canonical // nil until canonical audio is accepted
	CanonicalPurgedAt *time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
	StartedAt         *time.Time
	CompletedAt       *time.Time // set when done or failed
	// ComputeMS is the sum of task compute time, including prepare.
	ComputeMS         int64
	Error             string
	DownloadCount     int
	FirstDownloadedAt *time.Time
	LastDownloadedAt  *time.Time
	// Retries counts how many times the failed job was retried.
	Retries int
	// FetchURL and FetchSite are a URL job's URL and its site key; empty
	// for uploads.
	FetchURL, FetchSite string
	Tasks               api.TaskCounts
}

// Terminal reports whether the job is done or failed.
func (j *Job) Terminal() bool { return j.State == api.JobDone || j.State == api.JobFailed }

// API builds the wire status document. now is used for the running values
// of wait_s (not yet started) and elapsed_s (not yet completed).
func (j *Job) API(now time.Time) api.Job {
	out := api.Job{
		ID: j.ID, Queue: j.QueueName, State: j.State, Weight: j.Weight, Model: j.Model, Language: j.Language,
		Prompt: j.Prompt, Label: j.Label, Source: j.Source, Metadata: j.Metadata,
		Media: api.MediaInfo{
			OriginalBytes:  j.Original.Bytes,
			OriginalType:   j.Original.Type,
			OriginalPurged: j.OriginalPurgedAt != nil,
			// The audio is gone once the canonical audio is purged, or, for
			// a job that never got canonical audio, its original.
			AudioPurged: j.CanonicalPurgedAt != nil || j.Canonical == nil && j.OriginalPurgedAt != nil,
		},
		Tasks:       j.Tasks,
		CreatedAt:   j.CreatedAt,
		StartedAt:   j.StartedAt,
		CompletedAt: j.CompletedAt,
		ComputeS:    float64(j.ComputeMS) / 1000,
		Downloads:   api.Downloads{Count: j.DownloadCount, First: j.FirstDownloadedAt, Last: j.LastDownloadedAt},
		Retries:     j.Retries,
		Retryable:   j.Retryable(),
	}
	if j.Canonical != nil {
		out.Media.DurationS = float64(j.Canonical.Samples) / canonicalSampleRate
	}
	seconds := func(d time.Duration) *float64 { v := d.Seconds(); return &v }
	switch {
	case j.StartedAt != nil:
		out.WaitS = seconds(j.StartedAt.Sub(j.CreatedAt))
		end := now
		if j.CompletedAt != nil {
			end = *j.CompletedAt
		}
		out.ElapsedS = seconds(end.Sub(*j.StartedAt))
	case !j.Terminal():
		out.WaitS = seconds(now.Sub(j.CreatedAt))
	}
	if j.Error != "" {
		e := j.Error
		out.Error = &e
	}
	return out
}

func validRanges(rs []SampleRange, samples int64) error {
	if len(rs) == 0 {
		return fmt.Errorf("%w: canonical audio needs at least one task range", ErrInvalid)
	}
	var prev int64
	for i, r := range rs {
		if r.Start < prev || r.End <= r.Start || r.End > samples {
			return fmt.Errorf("%w: task range %d [%d,%d) invalid for %d samples", ErrInvalid, i, r.Start, r.End, samples)
		}
		prev = r.End
	}
	return nil
}

// CreateJob creates a job and its first tasks. With an IdempotencyKey that
// this client already used, it returns the existing job and created = false
// without changing anything.
//
// Without Canonical the job starts in preparing with a seq-0 prepare task;
// with Canonical it starts queued with one transcribe task per range
// (seq 1..N). The scheduler's reactivation rule is applied either way.
func (s *Store) CreateJob(ctx context.Context, nj NewJob) (job *Job, created bool, err error) {
	err = s.InTx(ctx, func(tx *Tx) error {
		job, created, err = tx.CreateJob(ctx, nj)
		return err
	})
	if err != nil {
		return nil, false, err
	}
	return job, created, nil
}

// CreateJob is Store.CreateJob inside the transaction.
func (t *Tx) CreateJob(ctx context.Context, nj NewJob) (job *Job, created bool, err error) {
	return t.createJob(ctx, nj, nil)
}

// NewFetchJob holds the fields of a URL job to create.
type NewFetchJob struct {
	// ID is optional; when empty the store generates one.
	ID             string
	QueueID        int64
	ClientID       string // owner principal id
	IdempotencyKey string // optional
	Params         api.JobParams
	// URL is the media to download and Site its policy site key
	// (fetchpolicy.SiteKey), both computed and validated by the caller.
	URL, Site string
}

// CreateFetchJob creates a URL job: state fetching, with one fetch task at
// FetchSeq costing Config.FetchCost. Idempotency and the scheduler's
// reactivation rule are as for CreateJob.
func (s *Store) CreateFetchJob(ctx context.Context, nj NewFetchJob) (job *Job, created bool, err error) {
	err = s.InTx(ctx, func(tx *Tx) error {
		job, created, err = tx.CreateFetchJob(ctx, nj)
		return err
	})
	if err != nil {
		return nil, false, err
	}
	return job, created, nil
}

// CreateFetchJob is Store.CreateFetchJob inside the transaction.
func (t *Tx) CreateFetchJob(ctx context.Context, nj NewFetchJob) (job *Job, created bool, err error) {
	if nj.URL == "" || nj.Site == "" {
		return nil, false, fmt.Errorf("%w: url job needs a url and a site", ErrInvalid)
	}
	return t.createJob(ctx, NewJob{ID: nj.ID, QueueID: nj.QueueID, ClientID: nj.ClientID,
		IdempotencyKey: nj.IdempotencyKey, Params: nj.Params}, &nj)
}

// createJob creates an upload job, or with fetch set a URL job (whose nj
// carries no media).
func (t *Tx) createJob(ctx context.Context, nj NewJob, fetch *NewFetchJob) (job *Job, created bool, err error) {
	s, tx := t.s, t.tx
	p := nj.Params
	if p.Weight == "" {
		p.Weight = api.WeightMedium
	}
	if p.Language == "" {
		p.Language = "en"
	}
	switch {
	case !p.Weight.Valid():
		return nil, false, fmt.Errorf("%w: weight %q", ErrInvalid, p.Weight)
	case p.Model == "":
		return nil, false, fmt.Errorf("%w: model is required", ErrInvalid)
	case nj.ClientID == "":
		return nil, false, fmt.Errorf("%w: client id is required", ErrInvalid)
	}
	if nj.Canonical != nil {
		if err := validRanges(nj.Ranges, nj.Canonical.Samples); err != nil {
			return nil, false, err
		}
	} else if len(nj.Ranges) > 0 {
		return nil, false, fmt.Errorf("%w: task ranges without canonical audio", ErrInvalid)
	}
	if nj.ID == "" {
		nj.ID = s.NewJobID()
	}
	if nj.IdempotencyKey != "" {
		var id string
		err := tx.QueryRowContext(ctx, `SELECT id FROM jobs WHERE client_id = ? AND idempotency_key = ?`,
			nj.ClientID, nj.IdempotencyKey).Scan(&id)
		if err == nil {
			job, err = getJob(ctx, tx, id)
			return job, false, err
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, false, fmt.Errorf("create job: idempotency lookup: %w", err)
		}
	}
	if _, err := queueByID(ctx, tx, nj.QueueID); err != nil {
		return nil, false, err
	}
	if err := reactivate(ctx, tx, nj.QueueID, p.Weight); err != nil {
		return nil, false, err
	}
	now := toMS(s.now())
	state := api.JobPreparing
	var fetchURL, fetchSite sql.NullString
	if fetch != nil {
		state = api.JobFetching
		fetchURL = sql.NullString{String: fetch.URL, Valid: true}
		fetchSite = sql.NullString{String: fetch.Site, Valid: true}
	}
	var canonBytes, canonSamples sql.NullInt64
	var canonSHA sql.NullString
	if c := nj.Canonical; c != nil {
		state = api.JobQueued
		canonBytes = sql.NullInt64{Int64: c.Bytes, Valid: true}
		canonSamples = sql.NullInt64{Int64: c.Samples, Valid: true}
		canonSHA = sql.NullString{String: c.SHA256, Valid: true}
	}
	var meta sql.NullString
	if len(p.Metadata) > 0 {
		meta = sql.NullString{String: string(p.Metadata), Valid: true}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO jobs (id, queue_id, client_id, idempotency_key, state, weight,
		model, language, prompt, label, source, metadata, original_bytes, original_sha256, original_type,
		canonical_bytes, canonical_sha256, audio_samples, created_at, updated_at, fetch_url, fetch_site)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		nj.ID, nj.QueueID, nj.ClientID, sql.NullString{String: nj.IdempotencyKey, Valid: nj.IdempotencyKey != ""},
		string(state), string(p.Weight), p.Model, p.Language, p.Prompt, p.Label, p.Source, meta,
		nj.Original.Bytes, nj.Original.SHA256, nj.Original.Type, canonBytes, canonSHA, canonSamples, now, now, fetchURL, fetchSite)
	if isUnique(err) {
		return nil, false, fmt.Errorf("create job %s: %w", nj.ID, ErrConflict)
	}
	if err != nil {
		return nil, false, fmt.Errorf("create job: %w", err)
	}
	switch {
	case fetch != nil:
		if _, err := tx.ExecContext(ctx, `INSERT INTO tasks (job_id, seq, kind, cost, state, pending_since)
			VALUES (?,?,?,?,?,?)`, nj.ID, FetchSeq, string(api.KindFetch), s.cfg.FetchCost, string(api.TaskPending),
			now); err != nil {
			return nil, false, fmt.Errorf("create fetch task: %w", err)
		}
	case nj.Canonical == nil:
		if err := s.insertPrepareTask(ctx, tx, nj.ID); err != nil {
			return nil, false, err
		}
	default:
		if err := s.insertTranscribeTasks(ctx, tx, nj.ID, p.Model, nj.Ranges); err != nil {
			return nil, false, err
		}
	}
	job, err = getJob(ctx, tx, nj.ID)
	if err != nil {
		return nil, false, err
	}
	return job, true, nil
}

func (s *Store) insertPrepareTask(ctx context.Context, tx *sql.Tx, jobID string) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO tasks (job_id, seq, kind, cost, state, pending_since)
		VALUES (?,0,?,?,?,?)`, jobID, string(api.KindPrepare), s.cfg.PrepareCost, string(api.TaskPending),
		toMS(s.now())); err != nil {
		return fmt.Errorf("create prepare task: %w", err)
	}
	return nil
}

func (s *Store) insertTranscribeTasks(ctx context.Context, tx *sql.Tx, jobID, model string, rs []SampleRange) error {
	now := toMS(s.now())
	for i, r := range rs {
		if _, err := tx.ExecContext(ctx, `INSERT INTO tasks (job_id, seq, kind, start_sample, end_sample, cost, state,
			pending_since) VALUES (?,?,?,?,?,?,?,?)`, jobID, i+1, string(api.KindTranscribe), r.Start, r.End,
			s.TranscribeCost(model, r.End-r.Start), string(api.TaskPending), now); err != nil {
			return fmt.Errorf("create transcribe task %d: %w", i+1, err)
		}
	}
	return nil
}

const jobSelect = `SELECT j.id, j.queue_id, q.name, j.client_id, COALESCE(j.idempotency_key, ''), j.state, j.weight,
	j.model, j.language, j.prompt, j.label, j.source, j.metadata, j.original_bytes, j.original_sha256,
	j.original_type, j.original_purged_at, j.canonical_bytes, j.canonical_sha256, j.audio_samples,
	j.canonical_purged_at, j.created_at, j.updated_at, j.started_at, j.completed_at, j.compute_ms,
	COALESCE(j.error, ''), j.download_count, j.first_downloaded_at, j.last_downloaded_at, j.retries,
	COALESCE(j.fetch_url, ''), COALESCE(j.fetch_site, ''),
	COUNT(t.seq), COALESCE(SUM(t.state = 'pending'), 0), COALESCE(SUM(t.state = 'leased'), 0),
	COALESCE(SUM(t.state = 'done'), 0), COALESCE(SUM(t.state = 'failed'), 0),
	COALESCE(SUM(t.state = 'cancelled'), 0), COALESCE(SUM(t.compute_ms), 0)
	FROM jobs j JOIN queues q ON q.id = j.queue_id LEFT JOIN tasks t ON t.job_id = j.id`

func scanJob(row interface{ Scan(...any) error }) (*Job, error) {
	var j Job
	var meta, canonSHA sql.NullString
	var origPurged, canonBytes, samples, canonPurged, started, completed, computeMS, firstDL, lastDL sql.NullInt64
	var created, updated, taskCompute int64
	err := row.Scan(&j.ID, &j.QueueID, &j.QueueName, &j.ClientID, &j.IdempotencyKey, &j.State, &j.Weight,
		&j.Model, &j.Language, &j.Prompt, &j.Label, &j.Source, &meta, &j.Original.Bytes, &j.Original.SHA256,
		&j.Original.Type, &origPurged, &canonBytes, &canonSHA, &samples, &canonPurged, &created, &updated,
		&started, &completed, &computeMS, &j.Error, &j.DownloadCount, &firstDL, &lastDL, &j.Retries,
		&j.FetchURL, &j.FetchSite, &j.Tasks.Total, &j.Tasks.Pending, &j.Tasks.Leased, &j.Tasks.Done, &j.Tasks.Failed, &j.Tasks.Cancelled, &taskCompute)
	if err != nil {
		return nil, err
	}
	if meta.Valid {
		j.Metadata = json.RawMessage(meta.String)
	}
	if canonBytes.Valid {
		j.Canonical = &Canonical{Bytes: canonBytes.Int64, SHA256: canonSHA.String, Samples: samples.Int64}
	}
	j.OriginalPurgedAt = optTime(origPurged)
	j.CanonicalPurgedAt = optTime(canonPurged)
	j.CreatedAt = fromMS(created)
	j.UpdatedAt = fromMS(updated)
	j.StartedAt = optTime(started)
	j.CompletedAt = optTime(completed)
	j.ComputeMS = taskCompute
	if computeMS.Valid {
		j.ComputeMS = computeMS.Int64
	}
	j.FirstDownloadedAt = optTime(firstDL)
	j.LastDownloadedAt = optTime(lastDL)
	return &j, nil
}

func getJob(ctx context.Context, q querier, id string) (*Job, error) {
	j, err := scanJob(q.QueryRowContext(ctx, jobSelect+` WHERE j.id = ? GROUP BY j.id`, id))
	if err != nil {
		return nil, notFound(err, "job "+id)
	}
	return j, nil
}

// Job returns a job by id. Visibility checks are the caller's job.
func (s *Store) Job(ctx context.Context, id string) (*Job, error) { return getJob(ctx, s.r, id) }

// JobByIdempotencyKey returns the job a client created with key, so a server
// can answer a retried upload before reading its body.
func (s *Store) JobByIdempotencyKey(ctx context.Context, clientID, key string) (*Job, error) {
	j, err := scanJob(s.r.QueryRowContext(ctx,
		jobSelect+` WHERE j.client_id = ? AND j.idempotency_key = ? GROUP BY j.id`, clientID, key))
	if err != nil {
		return nil, notFound(err, "job by idempotency key")
	}
	return j, nil
}

// JobSort is a job list order.
type JobSort string

// The job list orders (DESIGN.md §6.1).
const (
	// SortCreated orders by creation time, oldest first unless Desc (the
	// default).
	SortCreated JobSort = "created"
	// SortState puts running jobs first, then queued and preparing (oldest
	// first), then failed, then done (most recently finished first).
	SortState JobSort = "state"
)

// Valid reports whether o is a known order ("" is SortCreated).
func (o JobSort) Valid() bool { return o == "" || o == SortCreated || o == SortState }

// jobStateRankSQL and jobStateKeySQL (over j = jobs) are the state order's
// group and the time within the group. They must match the expressions of
// the jobs_queue_state_order index (migrations 0009, 0012) exactly.
const (
	jobStateRankSQL = `(CASE j.state WHEN 'running' THEN 0 WHEN 'queued' THEN 1 WHEN 'preparing' THEN 1 ` +
		`WHEN 'fetching' THEN 1 WHEN 'failed' THEN 2 ELSE 3 END)`
	jobStateKeySQL = `(CASE WHEN j.state IN ('failed','done') THEN -COALESCE(j.completed_at, j.created_at) ` +
		`ELSE j.created_at END)`
)

// stateOrder returns a job's place in the state order, as jobStateRankSQL
// and jobStateKeySQL compute it.
func (j *Job) stateOrder() (rank, key int64) {
	switch j.State {
	case api.JobRunning:
		return 0, toMS(j.CreatedAt)
	case api.JobQueued, api.JobPreparing, api.JobFetching:
		return 1, toMS(j.CreatedAt)
	}
	rank, t := int64(3), j.CreatedAt
	if j.State == api.JobFailed {
		rank = 2
	}
	if j.CompletedAt != nil {
		t = *j.CompletedAt
	}
	return rank, -toMS(t)
}

// JobFilter selects jobs for ListJobs.
type JobFilter struct {
	QueueID int64
	// ClientID, when non-empty, restricts the list to that client's jobs
	// (callers without view_all on the queue).
	ClientID string
	States   []api.JobState
	// Since, when non-zero, keeps only jobs updated after it.
	Since time.Time
	// Sort is the order; "" is SortCreated.
	Sort JobSort
	// Desc reverses SortCreated (newest first). SortState ignores it.
	Desc bool
	// Limit defaults to 100 and is capped at 1000.
	Limit int
	// Cursor is the opaque next-page token from a previous call with the
	// same order; a cursor of another order is ErrInvalid.
	Cursor string
}

// ListJobs lists jobs in f's order and returns the cursor for the next page
// ("" when there is none). Paging is by keyset, so it is stable under
// inserts and deletes; under SortState a job that changes state between
// two pages moves to its new group and may be listed twice or not at all.
func (s *Store) ListJobs(ctx context.Context, f JobFilter) ([]Job, string, error) {
	if !f.Sort.Valid() {
		return nil, "", fmt.Errorf("%w: sort %q", ErrInvalid, f.Sort)
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	limit = min(limit, 1000)

	where := []string{"j.queue_id = ?"}
	args := []any{f.QueueID}
	if f.ClientID != "" {
		where = append(where, "j.client_id = ?")
		args = append(args, f.ClientID)
	}
	if len(f.States) > 0 {
		where = append(where, "j.state IN ("+placeholders(len(f.States))+")")
		for _, st := range f.States {
			args = append(args, string(st))
		}
	}
	if !f.Since.IsZero() {
		where = append(where, "j.updated_at > ?")
		args = append(args, toMS(f.Since))
	}
	kind := cursorCreated
	order := "j.created_at, j.id"
	switch {
	case f.Sort == SortState:
		kind, order = cursorState, jobStateRankSQL+", "+jobStateKeySQL+", j.id"
	case f.Desc:
		kind, order = cursorCreatedDesc, "j.created_at DESC, j.id DESC"
	}
	if f.Cursor != "" {
		c, err := decodeCursor(f.Cursor, kind)
		if err != nil {
			return nil, "", err
		}
		switch kind {
		case cursorCreated:
			where = append(where, "(j.created_at, j.id) > (?, ?)")
			args = append(args, c.key, c.id)
		case cursorCreatedDesc:
			where = append(where, "(j.created_at, j.id) < (?, ?)")
			args = append(args, c.key, c.id)
		case cursorState:
			where = append(where, "("+jobStateRankSQL+", "+jobStateKeySQL+", j.id) > (?, ?, ?)")
			args = append(args, c.rank, c.key, c.id)
		}
	}
	args = append(args, limit+1)
	// The page is chosen on jobs alone, so the order and the cursor can use
	// an index; the task counts are joined for that page only.
	rows, err := s.r.QueryContext(ctx, jobSelect+" WHERE j.id IN (SELECT j.id FROM jobs j WHERE "+
		strings.Join(where, " AND ")+" ORDER BY "+order+" LIMIT ?) GROUP BY j.id ORDER BY "+order, args...)
	if err != nil {
		return nil, "", fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, "", fmt.Errorf("list jobs: %w", err)
		}
		out = append(out, *j)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("list jobs: %w", err)
	}
	var next string
	if len(out) > limit {
		out = out[:limit]
		last := &out[limit-1]
		c := cursor{kind: kind, key: toMS(last.CreatedAt), id: last.ID}
		if kind == cursorState {
			c.rank, c.key = last.stateOrder()
		}
		next = c.encode()
	}
	return out, next, nil
}

// CountJobs counts the jobs f selects, ignoring its order, limit and cursor.
func (s *Store) CountJobs(ctx context.Context, f JobFilter) (int, error) {
	where := []string{"queue_id = ?"}
	args := []any{f.QueueID}
	if f.ClientID != "" {
		where = append(where, "client_id = ?")
		args = append(args, f.ClientID)
	}
	if len(f.States) > 0 {
		where = append(where, "state IN ("+placeholders(len(f.States))+")")
		for _, st := range f.States {
			args = append(args, string(st))
		}
	}
	if !f.Since.IsZero() {
		where = append(where, "updated_at > ?")
		args = append(args, toMS(f.Since))
	}
	var n int
	if err := s.r.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE `+strings.Join(where, " AND "),
		args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("count jobs: %w", err)
	}
	return n, nil
}

// cursor is a decoded ListJobs page token. A creation-order cursor keeps
// the original "<created_ms>:<id>" form, so cursors handed out before the
// other orders existed still work; the others carry a prefix naming their
// order.
type cursor struct {
	kind      string
	rank, key int64
	id        string
}

const (
	cursorCreated     = ""
	cursorCreatedDesc = "d"
	cursorState       = "s"
)

func (c cursor) encode() string {
	var raw string
	switch c.kind {
	case cursorState:
		raw = "s:" + strconv.FormatInt(c.rank, 10) + ":" + strconv.FormatInt(c.key, 10) + ":" + c.id
	case cursorCreatedDesc:
		raw = "d:" + strconv.FormatInt(c.key, 10) + ":" + c.id
	default:
		raw = strconv.FormatInt(c.key, 10) + ":" + c.id
	}
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeCursor parses a page token of the order kind.
func decodeCursor(token, kind string) (cursor, error) {
	bad := fmt.Errorf("%w: cursor", ErrInvalid)
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return cursor{}, bad
	}
	raw := string(b)
	c := cursor{kind: kind}
	if kind != cursorCreated {
		rest, ok := strings.CutPrefix(raw, kind+":")
		if !ok {
			return cursor{}, bad
		}
		raw = rest
	}
	if kind == cursorState {
		rankStr, rest, ok := strings.Cut(raw, ":")
		if c.rank, err = strconv.ParseInt(rankStr, 10, 64); !ok || err != nil {
			return cursor{}, bad
		}
		raw = rest
	}
	keyStr, id, ok := strings.Cut(raw, ":")
	if c.key, err = strconv.ParseInt(keyStr, 10, 64); !ok || err != nil || id == "" {
		return cursor{}, bad
	}
	c.id = id
	return c, nil
}

// DeleteJob deletes a job and its tasks, ending its live leases as
// cancelled (their holders get ErrLeaseGone). It returns the cancelled lease
// ids. Blob removal is the caller's job.
func (s *Store) DeleteJob(ctx context.Context, id string) (cancelled []string, err error) {
	return inTx1(ctx, s, func(tx *Tx) ([]string, error) { return tx.DeleteJob(ctx, id) })
}

// DeleteJob is Store.DeleteJob inside the transaction.
func (t *Tx) DeleteJob(ctx context.Context, id string) (cancelled []string, err error) {
	cancelled, err = cancelJobLeases(ctx, t.tx, id, t.s.now())
	if err != nil {
		return nil, err
	}
	res, err := t.tx.ExecContext(ctx, `DELETE FROM jobs WHERE id = ?`, id)
	if err := mustAffect(res, err, "delete job "+id); err != nil {
		return nil, err
	}
	return cancelled, nil
}

// cancelJobLeases ends every live lease of a job as cancelled, without
// touching task state, and returns their ids.
func cancelJobLeases(ctx context.Context, tx *sql.Tx, jobID string, now time.Time) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `UPDATE leases SET ended_at = ?, outcome = ?
		WHERE job_id = ? AND ended_at IS NULL RETURNING id`, toMS(now), string(api.OutcomeCancelled), jobID)
	if err != nil {
		return nil, fmt.Errorf("cancel leases of job %s: %w", jobID, err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("cancel leases of job %s: %w", jobID, err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// RecordDownload counts a transcript or result download. It does not bump
// updated_at, so pollers using Since are not woken by their own downloads.
func (s *Store) RecordDownload(ctx context.Context, id string) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		now := toMS(s.now())
		res, err := tx.ExecContext(ctx, `UPDATE jobs SET download_count = download_count + 1,
			first_downloaded_at = COALESCE(first_downloaded_at, ?), last_downloaded_at = ? WHERE id = ?`, now, now, id)
		return mustAffect(res, err, "record download")
	})
}

// MarkOriginalPurged records that a job's original upload was deleted.
func (s *Store) MarkOriginalPurged(ctx context.Context, id string) error {
	return s.InTx(ctx, func(tx *Tx) error { return tx.MarkOriginalPurged(ctx, id) })
}

// MarkCanonicalPurged records that a job's canonical audio was deleted.
func (s *Store) MarkCanonicalPurged(ctx context.Context, id string) error {
	return s.InTx(ctx, func(tx *Tx) error { return tx.MarkCanonicalPurged(ctx, id) })
}

// MarkOriginalPurged is Store.MarkOriginalPurged inside the transaction.
func (t *Tx) MarkOriginalPurged(ctx context.Context, id string) error {
	return t.markPurged(ctx, id, "original_purged_at")
}

// MarkCanonicalPurged is Store.MarkCanonicalPurged inside the transaction.
func (t *Tx) MarkCanonicalPurged(ctx context.Context, id string) error {
	return t.markPurged(ctx, id, "canonical_purged_at")
}

func (t *Tx) markPurged(ctx context.Context, id, col string) error {
	now := toMS(t.s.now())
	res, err := t.tx.ExecContext(ctx, `UPDATE jobs SET `+col+` = COALESCE(`+col+`, ?), updated_at = ? WHERE id = ?`,
		now, now, id)
	return mustAffect(res, err, "mark "+col)
}

// FailJob fails a job with reason, cancelling its pending and leased tasks
// and their leases (returned). Failing a failed job is a no-op; failing a
// done job is ErrConflict.
func (s *Store) FailJob(ctx context.Context, id, reason string) (cancelled []string, err error) {
	err = s.write(ctx, func(tx *sql.Tx) error {
		var state api.JobState
		if err := tx.QueryRowContext(ctx, `SELECT state FROM jobs WHERE id = ?`, id).Scan(&state); err != nil {
			return notFound(err, "job "+id)
		}
		switch state {
		case api.JobFailed:
			return nil
		case api.JobDone:
			return fmt.Errorf("fail job %s: already done: %w", id, ErrConflict)
		}
		cancelled, err = failJob(ctx, tx, id, reason, s.now())
		return err
	})
	return cancelled, err
}

// failJob marks a job failed at once: it cancels the job's unfinished tasks
// and their live leases, and returns the cancelled lease ids. A task that
// fails for good does not use it (see doomJob).
func failJob(ctx context.Context, tx *sql.Tx, jobID, reason string, now time.Time) ([]string, error) {
	ids, err := cancelJobLeases(ctx, tx, jobID, now)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE tasks SET state = ? WHERE job_id = ? AND state IN ('pending','leased')`,
		taskCancelled, jobID); err != nil {
		return nil, fmt.Errorf("fail job %s: %w", jobID, err)
	}
	ms := toMS(now)
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET state = 'failed', error = ?, completed_at = ?, updated_at = ?,
		compute_ms = (SELECT COALESCE(SUM(compute_ms), 0) FROM tasks WHERE job_id = jobs.id)
		WHERE id = ? AND state NOT IN ('done','failed')`, reason, ms, ms, jobID); err != nil {
		return nil, fmt.Errorf("fail job %s: %w", jobID, err)
	}
	return ids, nil
}

// A job whose task failed for good (exhausted its attempts, or no other
// worker took it within the retry wait limit) is failing (DESIGN.md §4.2):
// it can no longer be done, but its tasks that are already leased run to
// completion so their results are kept. doomJob makes a job failing; the
// job becomes failed when settleJob finds no live lease left.

// doomJob records reason as a failing job's error (the first task to fail
// names it) and cancels the job's pending tasks, so no new lease is issued
// for it. Live leases are left alone.
func doomJob(ctx context.Context, tx *sql.Tx, jobID, reason string, now time.Time) error {
	if _, err := tx.ExecContext(ctx, `UPDATE tasks SET state = ? WHERE job_id = ? AND state = 'pending'`,
		taskCancelled, jobID); err != nil {
		return fmt.Errorf("fail job %s: %w", jobID, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET error = COALESCE(error, ?), updated_at = ?
		WHERE id = ? AND state NOT IN ('done','failed')`, reason, toMS(now), jobID); err != nil {
		return fmt.Errorf("fail job %s: %w", jobID, err)
	}
	return nil
}

// jobFailing reports whether a job has a task that failed for good.
func jobFailing(ctx context.Context, q querier, jobID string) (bool, error) {
	var failing bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM tasks WHERE job_id = ? AND state = 'failed')`,
		jobID).Scan(&failing)
	if err != nil {
		return false, fmt.Errorf("job %s failing: %w", jobID, err)
	}
	return failing, nil
}

// settleJob marks a failing job failed once it has no live lease, adding
// the job's progress to its error: "...; 2 of 12 tasks done, 4 cancelled"
// (transcribe tasks; a job that failed preparing has none). It reports
// whether the job became failed.
func settleJob(ctx context.Context, tx *sql.Tx, jobID string, now time.Time) (bool, error) {
	var live bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM leases WHERE job_id = ? AND ended_at IS NULL)`,
		jobID).Scan(&live); err != nil {
		return false, fmt.Errorf("settle job %s: %w", jobID, err)
	}
	if live {
		return false, nil
	}
	var total, done, cancelled int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(state = 'done'), 0),
		COALESCE(SUM(state = 'cancelled'), 0) FROM tasks WHERE job_id = ? AND kind = 'transcribe'`,
		jobID).Scan(&total, &done, &cancelled); err != nil {
		return false, fmt.Errorf("settle job %s: %w", jobID, err)
	}
	var progress string
	if total > 0 {
		progress = fmt.Sprintf("; %d of %d tasks done, %d cancelled", done, total, cancelled)
	}
	ms := toMS(now)
	res, err := tx.ExecContext(ctx, `UPDATE jobs SET state = 'failed', error = COALESCE(error, 'a task failed') || ?,
		completed_at = ?, updated_at = ?,
		compute_ms = (SELECT COALESCE(SUM(compute_ms), 0) FROM tasks WHERE job_id = jobs.id)
		WHERE id = ? AND state NOT IN ('done','failed')
		AND EXISTS (SELECT 1 FROM tasks WHERE job_id = jobs.id AND state = 'failed')`, progress, ms, ms, jobID)
	if err != nil {
		return false, fmt.Errorf("settle job %s: %w", jobID, err)
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// SettleFailingJobs marks failed every failing job with no live lease left
// and returns their ids. Lease ends and completions settle their job in the
// same transaction, so this is the reaper's backstop.
func (s *Store) SettleFailingJobs(ctx context.Context) ([]string, error) {
	var out []string
	err := s.write(ctx, func(tx *sql.Tx) error {
		out = nil
		rows, err := tx.QueryContext(ctx, `SELECT j.id FROM jobs j WHERE j.state IN ('fetching','preparing','queued','running')
			AND EXISTS (SELECT 1 FROM tasks t WHERE t.job_id = j.id AND t.state = 'failed')
			AND NOT EXISTS (SELECT 1 FROM leases l WHERE l.job_id = j.id AND l.ended_at IS NULL)`)
		if err != nil {
			return fmt.Errorf("failing jobs: %w", err)
		}
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return fmt.Errorf("failing jobs: %w", err)
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("failing jobs: %w", err)
		}
		for _, id := range ids {
			ok, err := settleJob(ctx, tx, id, s.now())
			if err != nil {
				return err
			}
			if ok {
				out = append(out, id)
			}
		}
		return nil
	})
	return out, err
}

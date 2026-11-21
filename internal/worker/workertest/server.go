package workertest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/media"
)

// Server is a fake whisper-network server implementing the worker API of
// DESIGN.md §6.2 in memory. Tests add tasks and models, steer worker state
// and inject faults, then observe what the worker did. All methods are safe
// for concurrent use.
type Server struct {
	URL string
	srv *httptest.Server

	// HeartbeatIntervalS is sent in every lease (default 30).
	HeartbeatIntervalS int
	// PollIntervalS is sent in GET /worker (default 60).
	PollIntervalS int
	// MaxAttempts bounds a task's leases (default 2).
	MaxAttempts int

	mu           sync.Mutex
	wake         chan struct{} // closed and replaced when a task becomes claimable
	nextID       int
	workers      map[string]*Worker
	tasks        []*Task
	leases       map[string]*Lease
	models       map[string][]byte
	claims       []api.ClaimRequest
	gaugeReports []api.Gauges

	inputCuts      int // remaining input responses to cut
	inputCutAt     int
	resultFailures int // remaining result PUTs to fail
	resultStatus   int
	modelDelay     time.Duration
	ackDelay       time.Duration // between accepting a result and answering
	modelGets      map[string]int
	registerFail   int
	resultPuts     int
}

// Worker is the server's record of a registered worker.
type Worker struct {
	ID      string
	Token   string
	State   api.WorkerState
	Revoked bool
	Facts   api.WorkerFacts
	Patches int
}

// Task is one unit of work in the fake's queue.
type Task struct {
	ID       string
	Kind     api.TaskKind
	Input    []byte
	Model    string
	Language string
	Prompt   string
	VADModel string
	// SendInputHash makes prepare leases carry input_sha256. Prepare leases
	// always carry input_bytes, as the real server's do.
	SendInputHash bool
	// LeaseInputBytes and LeaseAudioSamples, when positive, replace the
	// input_bytes (prepare) or audio_samples (transcribe) the lease
	// advertises, so the input can be made larger than the lease says.
	LeaseInputBytes, LeaseAudioSamples int64
	// OmitSize leaves input_bytes and audio_samples out of the lease.
	OmitSize bool
	// InputURL, if set, replaces the lease's input_url.
	InputURL string
	// URL, Site and MaxOutputBytes are a fetch lease's url, site and byte
	// cap (0: none).
	URL, Site      string
	MaxOutputBytes int64

	State    api.TaskState
	Attempts int
	// Outcomes.
	Transcript    *api.TranscribeResult
	FLAC          []byte
	ComputeMS     string
	FFmpegVersion string
	Failures      []api.FailureReport
	// Fetched and FetchMethod are an accepted fetch result and its
	// X-Fetch-Method.
	Fetched     []byte
	FetchMethod string
	// AbortedResults counts result PUTs whose body broke off (a chunked
	// body that never ended cleanly); nothing of them is kept.
	AbortedResults int
}

// Lease is the server's record of a lease.
type Lease struct {
	ID         string
	TaskID     string
	WorkerID   string
	Live       bool
	Outcome    api.LeaseOutcome
	Heartbeats int
	Gauges     []api.Gauges
	Inputs     []string // Range header of each input request
}

// NewServer starts a fake server, closed when the test ends.
func NewServer(t testing.TB) *Server {
	s := &Server{
		HeartbeatIntervalS: 30, PollIntervalS: 60, MaxAttempts: 2,
		wake: make(chan struct{}), workers: map[string]*Worker{}, leases: map[string]*Lease{},
		models: map[string][]byte{}, modelGets: map[string]int{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /worker", s.register)
	mux.HandleFunc("GET /worker/{id}", s.auth(s.getWorker))
	mux.HandleFunc("PATCH /worker/{id}", s.auth(s.patchWorker))
	mux.HandleFunc("POST /worker/{id}/leases", s.auth(s.claim))
	mux.HandleFunc("POST /worker/{id}/gauges", s.auth(s.reportGauges))
	mux.HandleFunc("GET /lease/{id}/input", s.auth(s.leaseOp(s.input)))
	mux.HandleFunc("POST /lease/{id}/heartbeat", s.auth(s.leaseOp(s.heartbeat)))
	mux.HandleFunc("PUT /lease/{id}/result", s.auth(s.leaseOp(s.result)))
	mux.HandleFunc("POST /lease/{id}/failure", s.auth(s.leaseOp(s.failure)))
	mux.HandleFunc("DELETE /lease/{id}", s.auth(s.leaseOp(s.release)))
	mux.HandleFunc("GET /models/{name}", s.auth(s.model))
	s.srv = httptest.NewServer(mux)
	s.URL = s.srv.URL
	t.Cleanup(s.srv.Close)
	return s
}

// ---------------------------------------------------------------------------
// Test controls

// AddModel serves data at /models/{name}.
func (s *Server) AddModel(name string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.models[name] = data
}

// SlowModels delays every model response by d (to overlap downloads).
func (s *Server) SlowModels(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.modelDelay = d
}

// AddTask queues a task and returns its ID. Zero fields get defaults: state
// pending, language "en".
func (s *Server) AddTask(t Task) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	if t.ID == "" {
		t.ID = fmt.Sprintf("job%d/%d", s.nextID, 1)
	}
	t.State = api.TaskPending
	if t.Language == "" && t.Kind == api.KindTranscribe {
		t.Language = "en"
	}
	s.tasks = append(s.tasks, &t)
	s.wakeLocked()
	return t.ID
}

// SetState changes a worker's state (approve, pause, resume, reject).
// Rejecting ends the worker's live leases.
func (s *Server) SetState(workerID string, st api.WorkerState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.workers[workerID]
	if w == nil {
		panic("unknown worker " + workerID)
	}
	w.State = st
	if st == api.WorkerRejected {
		for _, l := range s.leases {
			if l.WorkerID == workerID && l.Live {
				s.endLocked(l, api.OutcomeCancelled, false)
			}
		}
	}
	s.wakeLocked()
}

// Revoke makes the worker's token unknown: every call returns 401.
func (s *Server) Revoke(workerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workers[workerID].Revoked = true
}

// CancelLease ends a lease as if its job had been deleted: heartbeats and
// uploads get 410.
func (s *Server) CancelLease(leaseID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if l := s.leases[leaseID]; l != nil && l.Live {
		s.endLocked(l, api.OutcomeCancelled, false)
	}
}

// CutInputs makes the next n input responses break after `at` bytes.
func (s *Server) CutInputs(n, at int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inputCuts, s.inputCutAt = n, at
}

// FailResults makes the next n result uploads fail with status.
func (s *Server) FailResults(n, status int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resultFailures, s.resultStatus = n, status
}

// SlowResultAcks makes the server accept each result (ending the lease as
// done) and only answer d later, as a slow network would. Heartbeats in
// between get 410.
func (s *Server) SlowResultAcks(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ackDelay = d
}

// FailRegistrations makes the next n registrations fail with 503.
func (s *Server) FailRegistrations(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registerFail = n
}

// ---------------------------------------------------------------------------
// Observations (copies, safe to inspect)

// Workers returns the registered workers.
func (s *Server) Workers() []Worker {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Worker
	for _, w := range s.workers {
		out = append(out, *w)
	}
	return out
}

// OnlyWorker returns the single registered worker, or ok=false.
func (s *Server) OnlyWorker() (Worker, bool) {
	ws := s.Workers()
	if len(ws) != 1 {
		return Worker{}, false
	}
	return ws[0], true
}

// Task returns a copy of a task.
func (s *Server) Task(id string) Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.tasks {
		if t.ID == id {
			return *t
		}
	}
	panic("unknown task " + id)
}

// Leases returns copies of all leases, oldest first.
func (s *Server) Leases() []Lease {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Lease, 0, len(s.leases))
	for _, l := range s.leases {
		out = append(out, *l)
	}
	slices.SortFunc(out, func(a, b Lease) int { return strings.Compare(a.ID, b.ID) })
	return out
}

// LeasesFor returns the leases of one task, oldest first.
func (s *Server) LeasesFor(taskID string) []Lease {
	var out []Lease
	for _, l := range s.Leases() {
		if l.TaskID == taskID {
			out = append(out, l)
		}
	}
	return out
}

// Claims returns every claim request body received.
func (s *Server) Claims() []api.ClaimRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.claims)
}

// GaugeReports returns every POST /worker/{id}/gauges body received.
func (s *Server) GaugeReports() []api.Gauges {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.gaugeReports)
}

func (s *Server) reportGauges(w http.ResponseWriter, r *http.Request, _ *Worker) {
	var g api.Gauges
	if err := json.NewDecoder(r.Body).Decode(&g); err != nil {
		problem(w, http.StatusBadRequest, api.ProblemBadRequest, "")
		return
	}
	s.mu.Lock()
	s.gaugeReports = append(s.gaugeReports, g)
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// ResultUploads returns how many result PUTs were received.
func (s *Server) ResultUploads() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resultPuts
}

// ModelDownloads returns how many times a model was fetched.
func (s *Server) ModelDownloads(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.modelGets[name]
}

// ---------------------------------------------------------------------------
// Handlers

func problem(w http.ResponseWriter, status int, typ, state string) {
	w.Header().Set("Content-Type", api.ProblemContentType)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(api.Problem{Type: typ, Title: http.StatusText(status), Status: status, State: state})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) wakeLocked() {
	close(s.wake)
	s.wake = make(chan struct{})
}

func (s *Server) register(w http.ResponseWriter, r *http.Request) {
	var f api.WorkerFacts
	if err := json.NewDecoder(r.Body).Decode(&f); err != nil {
		problem(w, http.StatusBadRequest, api.ProblemBadRequest, "")
		return
	}
	s.mu.Lock()
	if s.registerFail > 0 {
		s.registerFail--
		s.mu.Unlock()
		problem(w, http.StatusServiceUnavailable, api.ProblemInternal, "")
		return
	}
	s.nextID++
	wk := &Worker{ID: fmt.Sprintf("w%d", s.nextID), Token: fmt.Sprintf("%sfaketoken%08d", api.WorkerTokenPrefix, s.nextID),
		State: api.WorkerPending, Facts: f}
	s.workers[wk.ID] = wk
	resp := api.RegisterResponse{ID: wk.ID, Token: wk.Token, State: wk.State}
	s.mu.Unlock()
	writeJSON(w, http.StatusCreated, resp)
}

// auth resolves the bearer token to a worker; unknown or revoked tokens get
// 401, a worker addressing another worker's path 403.
func (s *Server) auth(h func(http.ResponseWriter, *http.Request, *Worker)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		s.mu.Lock()
		var wk *Worker
		for _, c := range s.workers {
			if c.Token == tok && !c.Revoked {
				wk = c
			}
		}
		s.mu.Unlock()
		if wk == nil {
			problem(w, http.StatusUnauthorized, api.ProblemUnauthorized, "")
			return
		}
		if id := r.PathValue("id"); strings.HasPrefix(r.URL.Path, "/worker/") && id != wk.ID {
			problem(w, http.StatusForbidden, api.ProblemForbidden, "")
			return
		}
		h(w, r, wk)
	}
}

func (s *Server) getWorker(w http.ResponseWriter, _ *http.Request, wk *Worker) {
	s.mu.Lock()
	out := api.Worker{ID: wk.ID, State: wk.State, Facts: wk.Facts,
		HeartbeatIntervalS: s.HeartbeatIntervalS, PollIntervalS: s.PollIntervalS}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) patchWorker(w http.ResponseWriter, r *http.Request, wk *Worker) {
	var f api.WorkerFacts
	if err := json.NewDecoder(r.Body).Decode(&f); err != nil {
		problem(w, http.StatusBadRequest, api.ProblemBadRequest, "")
		return
	}
	s.mu.Lock()
	wk.Facts = f
	wk.Patches++
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// stateProblem answers a claim or heartbeat from a worker that may not work.
func stateProblem(w http.ResponseWriter, st api.WorkerState) bool {
	switch st {
	case api.WorkerPending:
		problem(w, http.StatusForbidden, api.ProblemWorkerPending, string(st))
	case api.WorkerPaused:
		problem(w, http.StatusConflict, api.ProblemWorkerPaused, string(st))
	case api.WorkerRejected:
		problem(w, http.StatusGone, api.ProblemWorkerRejected, string(st))
	default:
		return false
	}
	return true
}

func (s *Server) claim(w http.ResponseWriter, r *http.Request, wk *Worker) {
	var req api.ClaimRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		problem(w, http.StatusBadRequest, api.ProblemBadRequest, "")
		return
	}
	wait, _ := time.ParseDuration(r.URL.Query().Get("wait"))
	deadline := time.NewTimer(min(wait, 5*time.Second))
	defer deadline.Stop()
	s.mu.Lock()
	s.claims = append(s.claims, req)
	s.mu.Unlock()
	for {
		s.mu.Lock()
		if stateProblem(w, wk.State) {
			s.mu.Unlock()
			return
		}
		if t := s.pickLocked(req); t != nil {
			l := s.leaseLocked(t, wk)
			s.mu.Unlock()
			writeJSON(w, http.StatusCreated, l)
			return
		}
		wake := s.wake
		s.mu.Unlock()
		select {
		case <-wake:
		case <-deadline.C:
			w.WriteHeader(http.StatusNoContent)
			return
		case <-r.Context().Done():
			return
		}
	}
}

func (s *Server) pickLocked(req api.ClaimRequest) *Task {
	for _, t := range s.tasks {
		if t.State != api.TaskPending || !slices.Contains(req.Kinds, t.Kind) {
			continue
		}
		if t.Kind == api.KindTranscribe && !slices.Contains(req.Models, t.Model) {
			continue
		}
		return t
	}
	return nil
}

func (s *Server) leaseLocked(t *Task, wk *Worker) api.Lease {
	s.nextID++
	id := fmt.Sprintf("lse_%06d", s.nextID)
	t.State = api.TaskLeased
	t.Attempts++
	s.leases[id] = &Lease{ID: id, TaskID: t.ID, WorkerID: wk.ID, Live: true}
	l := api.Lease{ID: id, Kind: t.Kind, Job: strings.Split(t.ID, "/")[0], Seq: 1,
		InputURL: "/lease/" + id + "/input", HeartbeatIntervalS: s.HeartbeatIntervalS,
		ExpiresAt: time.Now().Add(3 * time.Duration(s.HeartbeatIntervalS) * time.Second).UTC()}
	if t.InputURL != "" {
		l.InputURL = t.InputURL
	}
	switch t.Kind {
	case api.KindPrepare:
		l.Seq = 0
		l.InputBytes = int64(len(t.Input))
		if t.SendInputHash {
			sum := sha256.Sum256(t.Input)
			l.InputSHA256 = hex.EncodeToString(sum[:])
		}
		if t.LeaseInputBytes > 0 {
			l.InputBytes = t.LeaseInputBytes
		}
		if t.OmitSize {
			l.InputBytes = 0
		}
	case api.KindFetch:
		l.Seq = 0
		l.URL, l.Site, l.MaxOutputBytes = t.URL, t.Site, t.MaxOutputBytes
	case api.KindTranscribe:
		l.AudioSamples = int64(max(len(t.Input)-44, 0) / media.BytesPerSample)
		if t.LeaseAudioSamples > 0 {
			l.AudioSamples = t.LeaseAudioSamples
		}
		if t.OmitSize {
			l.AudioSamples = 0
		}
		l.Model, l.Language, l.Prompt, l.VADModel = t.Model, t.Language, t.Prompt, t.VADModel
		if data, ok := s.models[t.Model]; ok {
			sum := sha256.Sum256(data)
			l.ModelSHA256 = hex.EncodeToString(sum[:])
		}
	}
	return l
}

// leaseOp resolves the lease and checks the caller holds it.
func (s *Server) leaseOp(h func(http.ResponseWriter, *http.Request, *Lease, *Task, *Worker)) func(http.ResponseWriter, *http.Request, *Worker) {
	return func(w http.ResponseWriter, r *http.Request, wk *Worker) {
		s.mu.Lock()
		l := s.leases[r.PathValue("id")]
		var t *Task
		if l != nil {
			for _, c := range s.tasks {
				if c.ID == l.TaskID {
					t = c
				}
			}
		}
		state := wk.State
		s.mu.Unlock()
		if l == nil || l.WorkerID != wk.ID {
			problem(w, http.StatusNotFound, api.ProblemNotFound, "")
			return
		}
		if state == api.WorkerRejected {
			problem(w, http.StatusGone, api.ProblemWorkerRejected, string(state))
			return
		}
		h(w, r, l, t, wk)
	}
}

// live reports whether l is live, answering 410 if not.
func (s *Server) live(w http.ResponseWriter, l *Lease) bool {
	s.mu.Lock()
	ok := l.Live
	s.mu.Unlock()
	if !ok {
		problem(w, http.StatusGone, api.ProblemLeaseGone, "")
	}
	return ok
}

// cutWriter aborts the response after limit body bytes.
type cutWriter struct {
	http.ResponseWriter
	left int
}

func (c *cutWriter) Write(p []byte) (int, error) {
	if len(p) > c.left {
		n, _ := c.ResponseWriter.Write(p[:c.left])
		c.left -= n
		c.ResponseWriter.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	}
	c.left -= len(p)
	return c.ResponseWriter.Write(p)
}

func (s *Server) input(w http.ResponseWriter, r *http.Request, l *Lease, t *Task, _ *Worker) {
	if !s.live(w, l) {
		return
	}
	s.mu.Lock()
	l.Inputs = append(l.Inputs, r.Header.Get("Range"))
	var out http.ResponseWriter = w
	if s.inputCuts > 0 {
		s.inputCuts--
		out = &cutWriter{ResponseWriter: w, left: s.inputCutAt}
	}
	s.mu.Unlock()
	if t.Kind == api.KindPrepare {
		// Prepare input is the stored original: Range is honoured.
		w.Header().Set("Content-Type", "application/octet-stream")
		http.ServeContent(out, r, "", time.Time{}, bytes.NewReader(t.Input))
		return
	}
	// Transcribe input is regenerated per request: no Range.
	w.Header().Set("Content-Type", "audio/wav")
	w.Header().Set("Content-Length", fmt.Sprint(len(t.Input)))
	_, _ = out.Write(t.Input)
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request, l *Lease, _ *Task, _ *Worker) {
	var hb api.HeartbeatRequest
	_ = json.NewDecoder(r.Body).Decode(&hb)
	if !s.live(w, l) {
		return
	}
	s.mu.Lock()
	l.Heartbeats++
	if hb.Gauges != nil {
		l.Gauges = append(l.Gauges, *hb.Gauges)
	}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, api.HeartbeatResponse{ExpiresAt: time.Now().Add(90 * time.Second).UTC()})
}

func (s *Server) result(w http.ResponseWriter, r *http.Request, l *Lease, t *Task, _ *Worker) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		// The upload broke off: keep nothing, as the real server does.
		s.mu.Lock()
		t.AbortedResults++
		s.mu.Unlock()
		return
	}
	s.mu.Lock()
	s.resultPuts++
	s.mu.Unlock()
	if !s.live(w, l) {
		return
	}
	s.mu.Lock()
	if s.resultFailures > 0 {
		s.resultFailures--
		s.mu.Unlock()
		problem(w, s.resultStatus, api.ProblemInternal, "")
		return
	}
	s.mu.Unlock()
	switch t.Kind {
	case api.KindPrepare:
		if r.Header.Get("Content-Type") != "audio/flac" {
			problem(w, http.StatusUnsupportedMediaType, api.ProblemBadRequest, "")
			return
		}
		if _, err := media.ValidateFLAC(bytes.NewReader(body), 0); err != nil {
			s.reject(w, l, t)
			return
		}
		s.mu.Lock()
		t.FLAC, t.ComputeMS, t.FFmpegVersion = body, r.Header.Get(api.HeaderComputeMS), r.Header.Get(api.HeaderFFmpegVersion)
	case api.KindTranscribe:
		var res api.TranscribeResult
		if err := json.Unmarshal(body, &res); err != nil || !validSegments(res.Segments, int64(len(t.Input)-44)/32) {
			s.reject(w, l, t)
			return
		}
		s.mu.Lock()
		t.Transcript = &res
	case api.KindFetch:
		if r.Header.Get("Content-Type") != "application/octet-stream" {
			problem(w, http.StatusUnsupportedMediaType, api.ProblemBadRequest, "")
			return
		}
		s.mu.Lock()
		t.Fetched, t.FetchMethod = body, r.Header.Get(api.HeaderFetchMethod)
	}
	t.State = api.TaskDone
	s.endLocked(l, api.OutcomeDone, false)
	delay := s.ackDelay
	s.mu.Unlock()
	time.Sleep(delay)
	w.WriteHeader(http.StatusOK)
}

// validSegments mirrors the server's checks: monotonic, within the task.
func validSegments(segs []api.Segment, durMS int64) bool {
	var prev int64
	for _, sg := range segs {
		if sg.StartMS < prev || sg.EndMS < sg.StartMS || sg.EndMS > durMS {
			return false
		}
		prev = sg.StartMS
	}
	return true
}

func (s *Server) reject(w http.ResponseWriter, l *Lease, t *Task) {
	s.mu.Lock()
	s.endLocked(l, api.OutcomeFailed, true)
	t.Failures = append(t.Failures, api.FailureReport{Reason: "result invalid", Retryable: true})
	s.mu.Unlock()
	problem(w, http.StatusUnprocessableEntity, api.ProblemResultInvalid, "")
}

func (s *Server) failure(w http.ResponseWriter, r *http.Request, l *Lease, t *Task, _ *Worker) {
	var rep api.FailureReport
	if err := json.NewDecoder(r.Body).Decode(&rep); err != nil {
		problem(w, http.StatusBadRequest, api.ProblemBadRequest, "")
		return
	}
	if !s.live(w, l) {
		return
	}
	s.mu.Lock()
	t.Failures = append(t.Failures, rep)
	s.endLocked(l, api.OutcomeFailed, rep.Retryable)
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) release(w http.ResponseWriter, _ *http.Request, l *Lease, t *Task, _ *Worker) {
	if !s.live(w, l) {
		return
	}
	s.mu.Lock()
	t.Attempts-- // a voluntary release does not consume an attempt
	s.endLocked(l, api.OutcomeReleased, true)
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// endLocked ends a lease and requeues or fails its task.
func (s *Server) endLocked(l *Lease, outcome api.LeaseOutcome, requeue bool) {
	l.Live, l.Outcome = false, outcome
	for _, t := range s.tasks {
		if t.ID != l.TaskID || t.State != api.TaskLeased {
			continue
		}
		switch {
		case outcome == api.OutcomeDone:
		case requeue && t.Attempts < s.MaxAttempts:
			t.State = api.TaskPending
			s.wakeLocked()
		default:
			t.State = api.TaskFailed
		}
	}
}

func (s *Server) model(w http.ResponseWriter, r *http.Request, wk *Worker) {
	s.mu.Lock()
	data, ok := s.models[r.PathValue("name")]
	s.modelGets[r.PathValue("name")]++
	delay, st := s.modelDelay, wk.State
	s.mu.Unlock()
	if st != api.WorkerActive && st != api.WorkerPaused {
		problem(w, http.StatusForbidden, api.ProblemForbidden, string(st))
		return
	}
	if !ok {
		problem(w, http.StatusNotFound, api.ProblemNotFound, "")
		return
	}
	time.Sleep(delay)
	sum := sha256.Sum256(data)
	w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:])+`"`)
	w.Header().Set("Content-Length", fmt.Sprint(len(data)))
	_, _ = w.Write(data)
}

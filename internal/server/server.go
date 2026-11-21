// Package server implements the whisper-network coordination server
// (DESIGN.md §6, §8, §10.3): the client, worker and admin HTTP APIs, upload
// ingestion and canonicalization, lease fencing, transcript assembly, the
// long-poll dispatcher, the reaper and the metrics refresher.
//
// All state lives in internal/store (SQLite) and internal/blobs (files); the
// server holds only hints and caches in memory. Dependencies (clock, disk
// probe, upstream HTTP client, model table, logger, metrics) are injected
// through Deps so every behaviour is testable without real time or network.
package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/auth"
	"github.com/deweysasser/whisper-network/internal/blobs"
	"github.com/deweysasser/whisper-network/internal/media"
	"github.com/deweysasser/whisper-network/internal/models"
	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/store"
)

// Config holds the server's tunables. Zero values select the defaults noted
// on each field (see DefaultConfig).
type Config struct {
	// Version is the server's own whisper-network version, reported by
	// GET /admin/version and whisper_server_info and compared with each
	// worker's (version skew, §6.3). Empty means "unknown".
	Version string
	// DataDir is the data directory: free-space checks run against it and
	// the model cache lives in DataDir/models. Required.
	DataDir string
	// PublicURL, when set, prefixes Location headers (e.g. https://wn.example).
	PublicURL string

	// HeartbeatInterval is sent to workers; leases live 3× this. Default 30s.
	HeartbeatInterval time.Duration
	// PollInterval is the hint for pending/paused workers. Default 30s.
	PollInterval time.Duration
	// MaxClaimWait caps a claim's ?wait= long-poll. Default 60s.
	MaxClaimWait time.Duration

	// TaskTarget and SplitWindow control splitting (DESIGN.md §8.1).
	// Defaults 60m and 60s; 2×SplitWindow must be below TaskTarget.
	TaskTarget  time.Duration
	SplitWindow time.Duration
	// MaxAudio is the longest accepted canonical audio. Default 12h.
	MaxAudio time.Duration

	// MaxUploadBytes is the default per-upload cap; queues may override it.
	// Default 2 GiB.
	MaxUploadBytes int64
	// MinFreeBytes: uploads are refused (507) and /readyz fails below this
	// much free space in DataDir. Default 1 GiB.
	MinFreeBytes int64

	// AudioRetention keeps canonical audio after a job ends. Default 24h.
	AudioRetention time.Duration
	// FailedRetention keeps a failed job's retained media (the original if
	// prepare never succeeded, else the canonical audio) at least this long
	// after it failed, so the job can be retried. Default 24h.
	FailedRetention time.Duration
	// FailedMaxDisk caps the data filesystem's use: while it is fuller, the
	// reaper purges the media of the oldest failed jobs first (never a live
	// job's). Default 80%.
	FailedMaxDisk DiskCeiling
	// JobRetention keeps finished jobs and transcripts. Default 720h.
	JobRetention time.Duration
	// PendingWorkerTTL deletes pending workers unseen this long. Default 168h.
	PendingWorkerTTL time.Duration
	// RetryWaitLimit bounds how long a task that failed on every worker
	// that took it waits for another eligible worker: the reaper then fails
	// its job, naming those workers (DESIGN.md §8.2). Default 24h.
	RetryWaitLimit time.Duration
	// EndedLeaseRetention keeps ended lease rows for debugging. Default 720h.
	EndedLeaseRetention time.Duration
	// RejectedWorkerRetention keeps rejected workers (so their tokens get a
	// "rejected, exit" answer) before deleting them. Default 720h.
	RejectedWorkerRetention time.Duration

	// Models is the whisper model allowlist. Default small.en, large-v3-turbo.
	Models []string
	// VADModel is the voice-activity model sent with transcribe leases; ""
	// disables VAD. It may be fetched from /models like a whisper model.
	VADModel string
	// ModelUpstream, when set, replaces the built-in upstream URLs: a model
	// is fetched from ModelUpstream + "/" + its file name.
	ModelUpstream string

	// MaxPendingWorkers caps registrations awaiting approval. Default 100.
	MaxPendingWorkers int
	// MaxPendingPerSource caps the pending workers registered from one
	// source address (auth.IPKey: an IPv4 address or IPv6 /64), so one
	// host cannot fill MaxPendingWorkers. Default 3.
	MaxPendingPerSource int
	// RegisterInterval and RegisterBurst rate-limit POST /worker per client
	// IP. Defaults one per minute with a burst of 5.
	RegisterInterval time.Duration
	RegisterBurst    int
	// TrustProxy makes the server believe X-Forwarded-For (rightmost entry)
	// for the client IP. Only enable it behind a proxy that sets the header.
	TrustProxy bool

	// SessionTTL is the UI session lifetime. Default 12h.
	SessionTTL time.Duration
	// CookieSecure sets the Secure attribute on session cookies.
	CookieSecure bool
	// PasswordParams are the argon2id costs. Default auth.DefaultPasswordParams.
	PasswordParams auth.PasswordParams

	// ShutdownTimeout bounds graceful shutdown. Default 15s.
	ShutdownTimeout time.Duration
	// ReapInterval, StatsInterval and SweepInterval pace the background
	// loops. Defaults 10s, 15s and 24h.
	ReapInterval  time.Duration
	StatsInterval time.Duration
	SweepInterval time.Duration
	// OrphanGrace is how old a ".part" file or row-less job directory must
	// be before the sweep deletes it. Default 1h.
	OrphanGrace time.Duration

	// ReadTimeout bounds reading a request's headers and body. Bodiless
	// requests are not limited after their headers; streaming uploads
	// (POST /queue/{q}, PUT /lease/{id}/result) instead must make progress
	// every ReadTimeout and finish within ReadTimeout plus their size cap at
	// MinTransferRate. Default 60s.
	ReadTimeout time.Duration
	// MinTransferRate (bytes/s) sizes the deadlines of streaming uploads and
	// of large downloads (inputs, models, transcripts): a peer slower than
	// this is cut off. Default 64 KiB/s.
	MinTransferRate int64
	// BusyWait is how long a request waits for a password-hashing or media
	// analysis slot before it is refused with 503. Default 30s.
	BusyWait time.Duration
	// PasswordMemoryBudget bounds the memory of concurrent argon2id
	// hashing: max(2, min(GOMAXPROCS, budget / per-hash memory)) run at once.
	// Default 512 MiB.
	PasswordMemoryBudget int64
	// AnalysisSlots bounds concurrent media validation and encoding (CPU
	// bound). Default GOMAXPROCS.
	AnalysisSlots int
	// AnalysesPerPrincipal bounds one principal's share of AnalysisSlots;
	// further analyses wait up to BusyWait, then get 429. Default 1.
	AnalysesPerPrincipal int
	// UploadsPerPrincipal caps one principal's job uploads in progress at
	// once; more are refused with 429. Default 2.
	UploadsPerPrincipal int

	// MaxLeaseAge and LeaseAgeFactor bound how long heartbeats can keep a
	// lease alive: a lease older than max(MaxLeaseAge, LeaseAgeFactor ×
	// the task's cost in seconds) is refused further heartbeats (410) and
	// expires. Cost is audio seconds × model factor (DESIGN.md §8.2).
	// Defaults 6h and 4.
	MaxLeaseAge    time.Duration
	LeaseAgeFactor float64
}

// DefaultConfig returns the documented defaults (DESIGN.md §11.1).
func DefaultConfig() Config {
	return Config{
		HeartbeatInterval:       30 * time.Second,
		PollInterval:            30 * time.Second,
		MaxClaimWait:            60 * time.Second,
		TaskTarget:              media.DefaultTarget,
		SplitWindow:             media.DefaultWindow,
		MaxAudio:                12 * time.Hour,
		MaxUploadBytes:          2 << 30,
		MinFreeBytes:            1 << 30,
		AudioRetention:          24 * time.Hour,
		FailedRetention:         24 * time.Hour,
		FailedMaxDisk:           DiskCeiling{Percent: 80},
		JobRetention:            720 * time.Hour,
		PendingWorkerTTL:        7 * 24 * time.Hour,
		RetryWaitLimit:          24 * time.Hour,
		EndedLeaseRetention:     30 * 24 * time.Hour,
		RejectedWorkerRetention: 30 * 24 * time.Hour,
		Models:                  []string{"small.en", "large-v3-turbo"},
		VADModel:                "silero-v5.1.2",
		MaxPendingWorkers:       100,
		RegisterInterval:        time.Minute,
		RegisterBurst:           5,
		SessionTTL:              12 * time.Hour,
		PasswordParams:          auth.DefaultPasswordParams(),
		ShutdownTimeout:         15 * time.Second,
		ReapInterval:            10 * time.Second,
		StatsInterval:           15 * time.Second,
		SweepInterval:           24 * time.Hour,
		OrphanGrace:             time.Hour,
		ReadTimeout:             60 * time.Second,
		MinTransferRate:         64 << 10,
		BusyWait:                30 * time.Second,
		PasswordMemoryBudget:    512 << 20,
		AnalysisSlots:           runtime.GOMAXPROCS(0),
		AnalysesPerPrincipal:    1,
		UploadsPerPrincipal:     2,
		MaxPendingPerSource:     3,
		MaxLeaseAge:             6 * time.Hour,
		LeaseAgeFactor:          4,
	}
}

func (c *Config) applyDefaults() {
	d := DefaultConfig()
	setDur := func(v *time.Duration, def time.Duration) {
		if *v <= 0 {
			*v = def
		}
	}
	setDur(&c.HeartbeatInterval, d.HeartbeatInterval)
	setDur(&c.PollInterval, d.PollInterval)
	setDur(&c.MaxClaimWait, d.MaxClaimWait)
	setDur(&c.TaskTarget, d.TaskTarget)
	setDur(&c.SplitWindow, d.SplitWindow)
	setDur(&c.MaxAudio, d.MaxAudio)
	setDur(&c.AudioRetention, d.AudioRetention)
	setDur(&c.FailedRetention, d.FailedRetention)
	if c.FailedMaxDisk == (DiskCeiling{}) {
		c.FailedMaxDisk = d.FailedMaxDisk
	}
	setDur(&c.JobRetention, d.JobRetention)
	setDur(&c.PendingWorkerTTL, d.PendingWorkerTTL)
	setDur(&c.RetryWaitLimit, d.RetryWaitLimit)
	setDur(&c.EndedLeaseRetention, d.EndedLeaseRetention)
	setDur(&c.RegisterInterval, d.RegisterInterval)
	setDur(&c.SessionTTL, d.SessionTTL)
	setDur(&c.ShutdownTimeout, d.ShutdownTimeout)
	setDur(&c.ReapInterval, d.ReapInterval)
	setDur(&c.StatsInterval, d.StatsInterval)
	setDur(&c.SweepInterval, d.SweepInterval)
	setDur(&c.OrphanGrace, d.OrphanGrace)
	setDur(&c.RejectedWorkerRetention, d.RejectedWorkerRetention)
	setDur(&c.ReadTimeout, d.ReadTimeout)
	setDur(&c.BusyWait, d.BusyWait)
	if c.MinTransferRate <= 0 {
		c.MinTransferRate = d.MinTransferRate
	}
	if c.PasswordMemoryBudget <= 0 {
		c.PasswordMemoryBudget = d.PasswordMemoryBudget
	}
	if c.AnalysisSlots <= 0 {
		c.AnalysisSlots = d.AnalysisSlots
	}
	if c.AnalysesPerPrincipal <= 0 {
		c.AnalysesPerPrincipal = d.AnalysesPerPrincipal
	}
	if c.UploadsPerPrincipal <= 0 {
		c.UploadsPerPrincipal = d.UploadsPerPrincipal
	}
	if c.MaxPendingPerSource <= 0 {
		c.MaxPendingPerSource = d.MaxPendingPerSource
	}
	setDur(&c.MaxLeaseAge, d.MaxLeaseAge)
	if c.LeaseAgeFactor <= 0 {
		c.LeaseAgeFactor = d.LeaseAgeFactor
	}
	if c.MaxUploadBytes <= 0 {
		c.MaxUploadBytes = d.MaxUploadBytes
	}
	c.PublicURL = strings.TrimRight(c.PublicURL, "/")
	if c.MinFreeBytes < 0 {
		c.MinFreeBytes = 0
	}
	if c.Models == nil {
		c.Models = d.Models
	}
	if c.MaxPendingWorkers <= 0 {
		c.MaxPendingWorkers = d.MaxPendingWorkers
	}
	if c.RegisterBurst <= 0 {
		c.RegisterBurst = d.RegisterBurst
	}
	if c.PasswordParams == (auth.PasswordParams{}) {
		c.PasswordParams = d.PasswordParams
	}
}

// Validate reports configuration errors that defaults cannot repair.
func (c *Config) Validate() error {
	if c.DataDir == "" {
		return errors.New("data directory is required")
	}
	if 2*c.SplitWindow >= c.TaskTarget {
		return fmt.Errorf("split window %v must be under half the task target %v", c.SplitWindow, c.TaskTarget)
	}
	if c.HeartbeatInterval < time.Second {
		return fmt.Errorf("heartbeat interval %v is under 1s", c.HeartbeatInterval)
	}
	if len(c.Models) == 0 {
		return errors.New("the model allowlist is empty")
	}
	return nil
}

// Deps are the server's collaborators.
type Deps struct {
	// Store and Blobs are required.
	Store *store.Store
	Blobs *blobs.Store
	// Logger receives JSONL events; it should already carry mode=server.
	Logger zerolog.Logger
	// Metrics records the catalog; nil creates a private set.
	Metrics *obs.Metrics
	// Now is the clock (time.Now). Use the store's clock.
	Now func() time.Time
	// FreeBytes reports free space for a path (FreeBytes).
	FreeBytes func(path string) (int64, error)
	// TotalBytes reports the size of the filesystem holding a path
	// (TotalBytes), for FailedMaxDisk. When FreeBytes is set and this is
	// not, the size is unknown and FailedMaxDisk is not enforced.
	TotalBytes func(path string) (int64, error)
	// HTTPClient fetches models from upstream (a client with no overall
	// timeout; fetches are bounded by context).
	HTTPClient *http.Client
	// LookupModel resolves a model name to its pinned file (models.Lookup).
	LookupModel func(name string) (models.Model, error)
	// NewTicker drives the background loops (time.NewTicker).
	NewTicker func(time.Duration) Ticker
	// UI, if set, builds the handler mounted at /ui/ (it sees full paths,
	// no prefix is stripped; "/" redirects to /ui/). It receives the server
	// so it can use the session helpers, Store, Ops and Notify (session.go).
	// Without it /ui/ serves a placeholder page.
	UI func(*Server) http.Handler
}

// Ticker is the part of *time.Ticker the background loops use.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()               { r.t.Stop() }

// Server is the coordination server. Create it with New.
type Server struct {
	// afterClaim, if set, runs after a claim commits and before the response
	// is written, with the request context. Tests use it to simulate a worker
	// disconnecting there.
	afterClaim func(ctx context.Context)

	// llms is the rendered /llms.txt (llms.go).
	llms []byte
	// bulkLimit caps the jobs of one bulk request (api.MaxBulkJobs; tests
	// lower it).
	bulkLimit int

	cfg         Config
	st          *store.Store
	blobs       *blobs.Store
	log         zerolog.Logger
	metrics     *obs.Metrics
	now         func() time.Time
	freeBytes   func(string) (int64, error)
	totalBytes  func(string) (int64, error)
	httpClient  *http.Client
	lookupModel func(string) (models.Model, error)
	newTicker   func(time.Duration) Ticker

	ops        *Ops
	disp       *dispatcher
	locks      *jobLocks
	regLimiter *auth.RateLimiter
	models     *modelCache
	allowed    map[string]models.Model // whisper model allowlist
	vad        *models.Model
	ui         http.Handler
	handler    http.Handler

	// Admission control (limits.go).
	passwords  *gate        // argon2id hashing, bounded by memory
	analysis   *gate        // media validation and encoding, CPU bound
	analysisBy keyedGate    // each principal's share of analysis
	uploading  uploadCounts // each principal's uploads in progress
	disk       diskBudget   // free space promised to in-flight uploads
	uploads    inflightJobs // job ids whose upload is still being received

	// bg is the server's lifetime context for detached work (model
	// fetches); Close cancels it. loops tracks the background loops.
	bg       context.Context
	bgCancel context.CancelFunc
	loops    sync.WaitGroup
}

// New builds a server. It does not start background loops or listeners;
// use Serve (or Handler plus Start in tests).
func New(cfg Config, deps Deps) (*Server, error) {
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if deps.Store == nil || deps.Blobs == nil {
		return nil, errors.New("server: store and blobs are required")
	}
	s := &Server{
		cfg: cfg, st: deps.Store, blobs: deps.Blobs, log: deps.Logger, metrics: deps.Metrics,
		now: deps.Now, freeBytes: deps.FreeBytes, totalBytes: deps.TotalBytes, httpClient: deps.HTTPClient,
		lookupModel: deps.LookupModel, newTicker: deps.NewTicker,
		disp: newDispatcher(), locks: newJobLocks(), bulkLimit: api.MaxBulkJobs,
	}
	if s.metrics == nil {
		s.metrics = obs.NewMetrics(newMetricsSet())
	}
	if s.cfg.Version == "" {
		s.cfg.Version = "unknown"
	}
	s.metrics.SetServerVersion(s.cfg.Version)
	if s.now == nil {
		s.now = time.Now
	}
	if s.freeBytes == nil {
		s.freeBytes = FreeBytes
	}
	switch {
	case s.totalBytes != nil:
	case deps.FreeBytes != nil:
		// The probes must agree: a faked free space against the real
		// filesystem size would trip the ceiling at random.
		s.totalBytes = func(string) (int64, error) { return 0, errors.New("no filesystem size probe") }
	default:
		s.totalBytes = TotalBytes
	}
	if s.httpClient == nil {
		s.httpClient = &http.Client{}
	}
	if s.lookupModel == nil {
		s.lookupModel = models.Lookup
	}
	if s.newTicker == nil {
		s.newTicker = func(d time.Duration) Ticker { return realTicker{time.NewTicker(d)} }
	}
	s.passwords = newGate(passwordSlots(cfg.PasswordMemoryBudget, cfg.PasswordParams))
	s.analysis = newGate(cfg.AnalysisSlots)
	s.regLimiter = auth.NewRateLimiter(auth.LimiterConfig{
		Interval: cfg.RegisterInterval, Burst: cfg.RegisterBurst, Now: s.now,
	})
	s.allowed = map[string]models.Model{}
	for _, name := range cfg.Models {
		m, err := s.lookupModel(name)
		if err != nil {
			return nil, fmt.Errorf("model allowlist: %w", err)
		}
		if m.Kind != models.KindWhisper {
			return nil, fmt.Errorf("model allowlist: %s is not a whisper model", name)
		}
		s.allowed[name] = m
	}
	if cfg.VADModel != "" {
		m, err := s.lookupModel(cfg.VADModel)
		if err != nil {
			return nil, fmt.Errorf("vad model: %w", err)
		}
		if m.Kind != models.KindVAD {
			return nil, fmt.Errorf("vad model: %s is not a VAD model", cfg.VADModel)
		}
		s.vad = &m
	}
	s.bg, s.bgCancel = context.WithCancel(context.Background())
	s.models = newModelCache(filepath.Join(cfg.DataDir, "models"), cfg.ModelUpstream, s.httpClient, s.log)
	s.ops = &Ops{Store: s.st, Log: s.log, PasswordParams: cfg.PasswordParams, Notify: s.disp.Notify, JobFinished: s.jobFinished,
		PasswordSlot: s.PasswordSlot, MaxUploadBytes: cfg.MaxUploadBytes}
	if deps.UI != nil {
		s.ui = deps.UI(s)
	}
	llms, err := renderLLMs(s.cfg)
	if err != nil {
		return nil, err
	}
	s.llms = llms
	s.handler = s.accessLog(s.recoverer(s.secureHeaders(s.deadlines(s.routes()))))
	return s, nil
}

// Handler returns the root HTTP handler (API, health and UI).
func (s *Server) Handler() http.Handler { return s.handler }

// Version returns the server's own whisper-network version.
func (s *Server) Version() string { return s.cfg.Version }

// Metrics returns the server's metrics recorder, for the metrics listener.
func (s *Server) Metrics() *obs.Metrics { return s.metrics }

// Close releases long-polls, cancels detached work and waits for the
// background loops and model downloads to stop, so the store can be closed
// safely afterwards. Serve calls it on return; tests that use Handler
// directly should call it themselves. It is idempotent.
func (s *Server) Close() {
	s.disp.Close()
	s.bgCancel()
	s.loops.Wait()
	s.models.wait()
}

// leaseTTL is how long a lease lives without a heartbeat (DESIGN.md §8.3).
func (s *Server) leaseTTL() time.Duration { return 3 * s.cfg.HeartbeatInterval }

func (s *Server) maxSamples() int64 {
	return int64(s.cfg.MaxAudio / time.Second * media.SampleRate)
}

// Start prepares the data for serving and starts the background loops,
// which stop when ctx is cancelled: live leases are extended by one TTL
// (restart grace, DESIGN.md §8.3) *before* the reaper can expire them, and
// orphaned partial files are swept.
func (s *Server) Start(ctx context.Context) error {
	n, err := s.st.ExtendLiveLeases(ctx, s.leaseTTL())
	if err != nil {
		return fmt.Errorf("extend live leases: %w", err)
	}
	if n > 0 {
		s.log.Info().Int64("leases", n).Msg("extended live leases for restart grace")
	}
	s.sweep(ctx)
	s.refreshStats(ctx)
	// The loops stop when ctx is cancelled or the server is closed,
	// whichever comes first; Close waits for them.
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.bg, cancel)
	var running sync.WaitGroup
	running.Go(func() { s.loop(ctx, s.cfg.ReapInterval, s.reapOnce) })
	running.Go(func() { s.loop(ctx, s.cfg.StatsInterval, s.refreshStats) })
	running.Go(func() { s.loop(ctx, s.cfg.SweepInterval, s.sweep) })
	s.loops.Go(func() {
		running.Wait()
		stop()
		cancel()
	})
	return nil
}

func (s *Server) loop(ctx context.Context, every time.Duration, fn func(context.Context)) {
	t := s.newTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C():
			fn(ctx)
		}
	}
}

// Serve starts the background loops and serves HTTP on ln (with TLS when
// tlsConfig is non-nil) until ctx is cancelled, then shuts down gracefully:
// waiting claims return 204 at once, in-flight requests get
// Config.ShutdownTimeout to finish, and the rest are cut off.
func (s *Server) Serve(ctx context.Context, ln net.Listener, tlsConfig *tls.Config) error {
	defer s.Close()
	loopCtx, stopLoops := context.WithCancel(ctx)
	defer stopLoops()
	if err := s.Start(loopCtx); err != nil {
		if ctx.Err() != nil {
			return nil // asked to stop while starting: a clean shutdown
		}
		return err
	}

	// Request contexts derive from base, which is cancelled only after the
	// graceful period, so in-flight uploads are not cut short by the signal.
	base, cancelBase := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelBase()
	// There is deliberately no WriteTimeout: long-poll claims and large
	// downloads would break under any global value. Downloads set their own
	// write deadline from their size (extendWrite), and ReadTimeout is
	// relaxed per request by the deadlines middleware (limits.go).
	hs := &http.Server{
		Handler:           s.handler,
		ReadHeaderTimeout: min(30*time.Second, s.cfg.ReadTimeout),
		ReadTimeout:       s.cfg.ReadTimeout,
		IdleTimeout:       2 * time.Minute,
		BaseContext:       func(net.Listener) context.Context { return base },
		ErrorLog:          newStdLogger(s.log),
	}
	if tlsConfig != nil {
		ln = tls.NewListener(ln, tlsConfig)
	}
	errc := make(chan error, 1)
	go func() { errc <- hs.Serve(ln) }()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	s.disp.Close()
	sctx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()
	err := hs.Shutdown(sctx)
	if err != nil {
		cancelBase()
		_ = hs.Close()
		s.log.Warn().Err(err).Msg("graceful shutdown timed out; connections closed")
	}
	if serr := <-errc; serr != nil && !errors.Is(serr, http.ErrServerClosed) {
		return serr
	}
	return nil
}

// HeartbeatHints returns the heartbeat and poll intervals sent to workers.
func (s *Server) heartbeatSeconds() int { return int(s.cfg.HeartbeatInterval / time.Second) }

// publicURL makes path absolute when Config.PublicURL is set.
func (s *Server) publicURL(path string) string { return s.cfg.PublicURL + path }

// workerHints fills the server hints of a worker document.
func (s *Server) workerHints(w *api.Worker) {
	w.HeartbeatIntervalS = s.heartbeatSeconds()
	w.PollIntervalS = int(s.cfg.PollInterval / time.Second)
	w.MaxPrepareOutputBytes = s.maxCanonicalBytes()
}

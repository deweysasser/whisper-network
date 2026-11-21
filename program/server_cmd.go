package program

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/rs/zerolog"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/auth"
	"github.com/deweysasser/whisper-network/internal/blobs"
	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/sched"
	"github.com/deweysasser/whisper-network/internal/server"
	"github.com/deweysasser/whisper-network/internal/store"
	"github.com/deweysasser/whisper-network/internal/ui"
)

// ServerCmd runs the coordination server (DESIGN.md §11.1). Every flag has a
// WHISPER_* environment variable.
type ServerCmd struct {
	Listen        string `default:":8080" env:"WHISPER_LISTEN" help:"API listen address"`
	MetricsListen string `default:"127.0.0.1:9090" env:"WHISPER_METRICS_LISTEN" help:"Metrics listen address (loopback by default); empty disables it"`

	MetricsPushURL      string        `env:"WHISPER_METRICS_PUSH_URL" help:"Push metrics to this /api/v1/import/prometheus URL"`
	MetricsPushInterval time.Duration `default:"15s" env:"WHISPER_METRICS_PUSH_INTERVAL" help:"Metrics push interval"`
	MetricsLabels       string        `env:"WHISPER_METRICS_LABELS" help:"Extra labels on pushed metrics: k=v,k=v"`

	Data       string `default:"/data" env:"WHISPER_DATA" help:"Data directory: database, blobs/, models/"`
	PublicURL  string `env:"WHISPER_PUBLIC_URL" help:"External base URL, used in Location headers"`
	TLSCert    string `name:"tls-cert" env:"WHISPER_TLS_CERT" help:"TLS certificate file"`
	TLSKey     string `name:"tls-key" env:"WHISPER_TLS_KEY" help:"TLS key file"`
	RequireTLS bool   `name:"require-tls" env:"WHISPER_REQUIRE_TLS" help:"Refuse to start without TLS"`
	TrustProxy bool   `env:"WHISPER_TRUST_PROXY" help:"Trust X-Forwarded-For (only behind a proxy that sets it)"`

	HeartbeatInterval time.Duration `default:"30s" env:"WHISPER_HEARTBEAT_INTERVAL" help:"Worker heartbeat interval; leases live 3x this"`
	MaxAttempts       int           `default:"2" env:"WHISPER_MAX_ATTEMPTS" help:"Distinct workers a task may fail on (or let its lease expire) before the job fails; a worker never retries a task it failed"`
	RetryWaitLimit    time.Duration `default:"24h" env:"WHISPER_RETRY_WAIT_LIMIT" help:"Fail a job whose failed task no other eligible worker has taken for this long"`
	WorkerFailLimit   int           `name:"worker-failure-limit" default:"3" env:"WHISPER_WORKER_FAILURE_LIMIT" help:"Pause a worker after this many failed or expired leases in a row, across at least 2 jobs; 0 disables"`
	TaskTarget        time.Duration `default:"60m" env:"WHISPER_TASK_TARGET" help:"Target audio length of one transcribe task"`
	SplitWindow       time.Duration `default:"60s" env:"WHISPER_SPLIT_WINDOW" help:"Search window around each split point"`
	ClassWeights      string        `default:"low=1,medium=2,high=4" env:"WHISPER_CLASS_WEIGHTS" help:"Job weight-class shares"`
	ModelFactors      string        `default:"small.en=1,large-v3-turbo=3" env:"WHISPER_MODEL_FACTORS" help:"Scheduling cost factor per model"`
	PrepareMinSandbox string        `default:"full" enum:"full,fs-only,none" env:"WHISPER_PREPARE_MIN_SANDBOX" help:"Least sandbox level that may run prepare tasks"`
	FetchMinSandbox   string        `default:"full" enum:"full,fs-only,none" env:"WHISPER_FETCH_MIN_SANDBOX" help:"Least sandbox level a downloader needs to fetch URLs (full: filesystem restricted and TCP connect limited to its egress proxy)"`
	MaxUploadBytes    string        `default:"2GiB" env:"WHISPER_MAX_UPLOAD_BYTES" help:"Default upload size cap (queues may override)"`
	MinFreeBytes      string        `default:"1GiB" env:"WHISPER_MIN_FREE_BYTES" help:"Refuse uploads below this much free disk"`
	AudioRetention    time.Duration `default:"24h" env:"WHISPER_AUDIO_RETENTION" help:"Keep canonical audio this long after a job is done"`
	FailedRetention   time.Duration `default:"24h" env:"WHISPER_FAILED_RETENTION" help:"Keep a failed job's media at least this long after it failed, so it can be retried"`
	FailedMaxDisk     string        `name:"failed-retention-max-disk" default:"80%" env:"WHISPER_FAILED_RETENTION_MAX_DISK" help:"When the data disk is fuller than this (a percentage, or a size such as 200GiB), purge the oldest failed jobs' media first"`
	JobRetention      time.Duration `default:"720h" env:"WHISPER_JOB_RETENTION" help:"Keep finished jobs and transcripts this long"`
	PendingWorkerTTL  time.Duration `default:"168h" env:"WHISPER_PENDING_WORKER_TTL" help:"Delete workers still pending this long after registering"`
	RejectedRetention time.Duration `name:"rejected-worker-retention" default:"720h" env:"WHISPER_REJECTED_WORKER_RETENTION" help:"Delete rejected workers this long after rejection"`
	MaxPendingWorkers int           `default:"100" env:"WHISPER_MAX_PENDING_WORKERS" help:"Cap on workers awaiting approval"`
	MaxPendingSource  int           `name:"max-pending-per-source" default:"3" env:"WHISPER_MAX_PENDING_PER_SOURCE" help:"Cap on workers awaiting approval registered from one address (IPv6: one /64)"`
	MaxUploadsEach    int           `name:"max-uploads-per-principal" default:"2" env:"WHISPER_MAX_UPLOADS_PER_PRINCIPAL" help:"Uploads one client or user may have in progress at once; more get 429"`
	MaxAnalysesEach   int           `name:"max-analyses-per-principal" default:"1" env:"WHISPER_MAX_ANALYSES_PER_PRINCIPAL" help:"Media analyses one principal may run at once; more wait, then get 429"`
	MaxLeaseAge       time.Duration `default:"6h" env:"WHISPER_MAX_LEASE_AGE" help:"Least age at which heartbeats stop extending a lease (see --lease-age-factor)"`
	LeaseAgeFactor    float64       `default:"4" env:"WHISPER_LEASE_AGE_FACTOR" help:"A lease may live this many times its task's cost in seconds (audio seconds x model factor), if more than --max-lease-age"`
	Models            []string      `default:"small.en,large-v3-turbo" env:"WHISPER_MODELS" help:"Whisper model allowlist"`
	VADModel          string        `name:"vad-model" default:"silero-v5.1.2" env:"WHISPER_VAD_MODEL" help:"VAD model sent with transcribe leases; empty disables"`
	ModelUpstream     string        `env:"WHISPER_MODEL_UPSTREAM" help:"Base URL to fetch model files from instead of the built-in upstreams"`
	MaxClaimWait      time.Duration `default:"60s" env:"WHISPER_MAX_CLAIM_WAIT" help:"Cap on a worker claim's long-poll"`
	SessionTTL        time.Duration `default:"12h" env:"WHISPER_SESSION_TTL" help:"UI session lifetime"`
	ShutdownTimeout   time.Duration `default:"15s" env:"WHISPER_SHUTDOWN_TIMEOUT" help:"Graceful shutdown limit"`
	ReadTimeout       time.Duration `default:"60s" env:"WHISPER_READ_TIMEOUT" help:"Limit on reading a request; streaming uploads must instead progress this often"`
	MinTransferRate   string        `default:"64KiB" env:"WHISPER_MIN_TRANSFER_RATE" help:"Slowest accepted upload or download rate, per second; sizes transfer deadlines"`
	PasswordMemory    string        `name:"password-memory-budget" default:"512MiB" env:"WHISPER_PASSWORD_MEMORY_BUDGET" help:"Memory for concurrent password hashing (bounds parallel logins)"`
	AdminPassword     string        `env:"WHISPER_ADMIN_PASSWORD" help:"Initial admin password (first start only; random if unset)"`
}

// Validate checks flag combinations kong cannot.
func (c *ServerCmd) Validate() error {
	if (c.TLSCert == "") != (c.TLSKey == "") {
		return errors.New("--tls-cert and --tls-key must be given together")
	}
	if c.RequireTLS && c.TLSCert == "" {
		return errors.New("--require-tls is set but no --tls-cert/--tls-key were given")
	}
	if c.MaxAttempts < 1 {
		return errors.New("--max-attempts must be at least 1")
	}
	if c.RetryWaitLimit <= 0 {
		return errors.New("--retry-wait-limit must be positive")
	}
	if c.WorkerFailLimit < 0 {
		return errors.New("--worker-failure-limit must not be negative")
	}
	if c.MaxPendingSource < 1 || c.MaxUploadsEach < 1 || c.MaxAnalysesEach < 1 {
		return errors.New("--max-pending-per-source, --max-uploads-per-principal and --max-analyses-per-principal must be at least 1")
	}
	if c.MaxLeaseAge <= 0 || c.LeaseAgeFactor <= 0 {
		return errors.New("--max-lease-age and --lease-age-factor must be positive")
	}
	if c.PublicURL != "" && !strings.HasPrefix(c.PublicURL, "http://") && !strings.HasPrefix(c.PublicURL, "https://") {
		return errors.New("--public-url must start with http:// or https://")
	}
	_, _, err := c.storeTuning()
	return err
}

func (c *ServerCmd) storeTuning() (sched.ClassWeights, map[string]float64, error) {
	cw, err := server.ParseFactors(c.ClassWeights)
	if err != nil {
		return sched.ClassWeights{}, nil, fmt.Errorf("--class-weights: %w", err)
	}
	for k := range cw {
		if !api.Weight(k).Valid() {
			return sched.ClassWeights{}, nil, fmt.Errorf("--class-weights: unknown class %q", k)
		}
	}
	def := sched.DefaultClassWeights()
	weights := def
	for k, v := range cw {
		switch api.Weight(k) {
		case api.WeightLow:
			weights.Low = v
		case api.WeightMedium:
			weights.Medium = v
		case api.WeightHigh:
			weights.High = v
		}
	}
	mf, err := server.ParseFactors(c.ModelFactors)
	if err != nil {
		return sched.ClassWeights{}, nil, fmt.Errorf("--model-factors: %w", err)
	}
	return weights, mf, nil
}

func (c *ServerCmd) storeConfig() (store.Config, error) {
	weights, factors, err := c.storeTuning()
	if err != nil {
		return store.Config{}, err
	}
	maxUpload, err := server.ParseByteSize(c.MaxUploadBytes)
	if err != nil {
		return store.Config{}, fmt.Errorf("--max-upload-bytes: %w", err)
	}
	return store.Config{
		Path: server.DBPath(c.Data), ClassWeights: weights, ModelFactors: factors,
		MaxAttempts: c.MaxAttempts, WorkerFailureLimit: c.WorkerFailLimit,
		PrepareMinSandbox: api.SandboxLevel(c.PrepareMinSandbox),
		FetchMinSandbox:   api.SandboxLevel(c.FetchMinSandbox),
		// A fetch lease's byte cap: the queue's upload cap, never above the
		// server's.
		FetchMaxBytes: maxUpload,
	}, nil
}

func (c *ServerCmd) serverConfig() (server.Config, error) {
	maxUpload, err := server.ParseByteSize(c.MaxUploadBytes)
	if err != nil {
		return server.Config{}, fmt.Errorf("--max-upload-bytes: %w", err)
	}
	minFree, err := server.ParseByteSize(c.MinFreeBytes)
	if err != nil {
		return server.Config{}, fmt.Errorf("--min-free-bytes: %w", err)
	}
	rate, err := server.ParseByteSize(c.MinTransferRate)
	if err != nil || rate <= 0 {
		return server.Config{}, fmt.Errorf("--min-transfer-rate: must be a positive size")
	}
	pwMem, err := server.ParseByteSize(c.PasswordMemory)
	if err != nil || pwMem <= 0 {
		return server.Config{}, fmt.Errorf("--password-memory-budget: must be a positive size")
	}
	failedMax, err := server.ParseDiskCeiling(c.FailedMaxDisk)
	if err != nil {
		return server.Config{}, fmt.Errorf("--failed-retention-max-disk: %w", err)
	}
	cfg := server.DefaultConfig()
	cfg.DataDir = c.Data
	cfg.PublicURL = c.PublicURL
	cfg.Version = Version
	cfg.HeartbeatInterval = c.HeartbeatInterval
	cfg.MaxClaimWait = c.MaxClaimWait
	cfg.TaskTarget = c.TaskTarget
	cfg.SplitWindow = c.SplitWindow
	cfg.MaxUploadBytes = maxUpload
	cfg.MinFreeBytes = minFree
	cfg.AudioRetention = c.AudioRetention
	cfg.FailedRetention = c.FailedRetention
	cfg.FailedMaxDisk = failedMax
	cfg.JobRetention = c.JobRetention
	cfg.RetryWaitLimit = c.RetryWaitLimit
	cfg.PendingWorkerTTL = c.PendingWorkerTTL
	cfg.RejectedWorkerRetention = c.RejectedRetention
	cfg.ReadTimeout = c.ReadTimeout
	cfg.MinTransferRate = rate
	cfg.PasswordMemoryBudget = pwMem
	cfg.MaxPendingWorkers = c.MaxPendingWorkers
	cfg.MaxPendingPerSource = c.MaxPendingSource
	cfg.UploadsPerPrincipal = c.MaxUploadsEach
	cfg.AnalysesPerPrincipal = c.MaxAnalysesEach
	cfg.MaxLeaseAge = c.MaxLeaseAge
	cfg.LeaseAgeFactor = c.LeaseAgeFactor
	cfg.Models = c.Models
	cfg.VADModel = c.VADModel
	cfg.ModelUpstream = strings.TrimRight(c.ModelUpstream, "/")
	cfg.TrustProxy = c.TrustProxy
	cfg.SessionTTL = c.SessionTTL
	cfg.ShutdownTimeout = c.ShutdownTimeout
	// Browsers drop Secure cookies over plain HTTP (except on localhost),
	// so mark them Secure whenever the UI is reached over HTTPS.
	cfg.CookieSecure = c.TLSCert != "" || strings.HasPrefix(c.PublicURL, "https://")
	return cfg, nil
}

// prepareDataDir creates the data directory private to the server's user
// (it holds the database and media), and tightens an existing one. Failing
// to tighten (e.g. a mount point owned by another user) only warns.
func prepareDataDir(dir string, lg zerolog.Logger) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("data directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		lg.Warn().Err(err).Str("data", dir).Msg("could not make the data directory private")
	}
	return nil
}

// Run starts the server and blocks until ctx is cancelled (SIGINT/SIGTERM).
func (c *ServerCmd) Run(ctx context.Context, logger zerolog.Logger) error {
	// mewkiz/flac reports oddities in hostile input through the standard
	// logger; those lines are neither JSONL nor useful, so drop them.
	log.SetOutput(io.Discard)
	lg := obs.NewLogger(logger, obs.ModeServer)

	cfg, err := c.serverConfig()
	if err != nil {
		return err
	}
	scfg, err := c.storeConfig()
	if err != nil {
		return err
	}
	if err := prepareDataDir(c.Data, lg); err != nil {
		return err
	}
	st, err := store.Open(ctx, scfg)
	if err != nil {
		return err
	}
	defer st.Close()
	if _, err := server.Bootstrap(ctx, st, c.AdminPassword, auth.DefaultPasswordParams(), os.Stderr); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	bs, err := blobs.New(server.BlobsPath(c.Data))
	if err != nil {
		return err
	}
	m := obs.NewMetrics(metrics.NewSet())
	srv, err := server.New(cfg, server.Deps{Store: st, Blobs: bs, Logger: lg, Metrics: m, UI: ui.New})
	if err != nil {
		return err
	}

	var tlsConfig *tls.Config
	if c.TLSCert != "" {
		cert, err := tls.LoadX509KeyPair(c.TLSCert, c.TLSKey)
		if err != nil {
			return fmt.Errorf("loading TLS key pair: %w", err)
		}
		tlsConfig = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	}
	ln, err := net.Listen("tcp", c.Listen)
	if err != nil {
		return err
	}
	if tlsConfig == nil && !server.IsLoopbackAddr(c.Listen) {
		lg.Warn().Str("listen", c.Listen).
			Msg("serving plain HTTP on a non-loopback address; tokens cross the network in cleartext unless a TLS proxy fronts this server")
	}
	if c.MetricsListen != "" {
		stop, err := server.ListenMetrics(ctx, c.MetricsListen, m, lg)
		if err != nil {
			_ = ln.Close()
			return err
		}
		defer stop()
	}
	if c.MetricsPushURL != "" {
		labels, err := obs.ParseLabels(c.MetricsLabels)
		if err != nil {
			_ = ln.Close()
			return err
		}
		if _, err := m.StartPush(ctx, obs.PushConfig{URL: c.MetricsPushURL, Interval: c.MetricsPushInterval,
			ExtraLabels: labels, Logger: lg}); err != nil {
			_ = ln.Close()
			return fmt.Errorf("metrics push: %w", err)
		}
	}
	lg.Info().Str("listen", ln.Addr().String()).Bool("tls", tlsConfig != nil).Str("data", c.Data).Msg("server started")
	err = srv.Serve(ctx, ln, tlsConfig)
	lg.Info().Msg("server stopped")
	return err
}

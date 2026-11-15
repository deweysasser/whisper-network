package program

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/rs/zerolog"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/client"
	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/worker"
)

// WorkerCmd runs a transcription worker (DESIGN.md §7, flags §11.1).
type WorkerCmd struct {
	Server         string        `required:"" env:"WHISPER_SERVER" help:"Server URL, e.g. https://wn.example"`
	State          string        `default:"/var/lib/whisper-network" env:"WHISPER_STATE" type:"path" help:"State directory: credentials and model cache. Mount it as a volume."`
	Description    string        `env:"WHISPER_DESCRIPTION" help:"Description shown to approvers (default: hostname)"`
	ThreadsPerSlot int           `default:"4" env:"WHISPER_THREADS_PER_SLOT" help:"whisper-cli threads per slot"`
	Slots          int           `env:"WHISPER_SLOTS" help:"Override the derived slot count"`
	MemoryReserve  ByteSize      `default:"1GiB" env:"WHISPER_MEMORY_RESERVE" help:"Memory kept free for the system"`
	Memory         ByteSize      `env:"WHISPER_MEMORY" help:"Override the probed memory limit (required where it cannot be probed)"`
	DiskReserve    ByteSize      `default:"1GiB" env:"WHISPER_DISK_RESERVE" help:"Disk kept free on the state filesystem; a task whose input and output would not fit above it is handed back"`
	ModelCacheMax  ByteSize      `default:"0" env:"WHISPER_MODEL_CACHE_MAX" help:"Cap on the model cache; least recently used models not in use are evicted (0: unlimited)"`
	Kinds          []string      `default:"prepare,transcribe" enum:"prepare,transcribe" sep:"," env:"WHISPER_KINDS" help:"Task kinds to accept"`
	WhisperCLI     string        `name:"whisper-cli" default:"whisper-cli" env:"WHISPER_WHISPER_CLI" help:"whisper-cli binary"`
	FFmpeg         string        `name:"ffmpeg" default:"ffmpeg" env:"WHISPER_FFMPEG" help:"ffmpeg binary"`
	EngineVersion  string        `env:"WHISPER_ENGINE_VERSION" help:"Override the engine version reported by whisper-cli"`
	ReleaseOnTerm  bool          `name:"release-on-term" env:"WHISPER_RELEASE_ON_TERM" help:"On SIGTERM/SIGINT, release running tasks to other workers at once instead of finishing them first"`
	Drain          bool          `hidden:"" env:"WHISPER_DRAIN" help:"Deprecated: draining is now the default"`
	MaxHostLoad    float64       `default:"70" env:"WHISPER_MAX_HOST_LOAD" help:"Stop claiming new tasks while the host's CPU use by other processes (percent, 1-minute average) is above this; running tasks continue (0: disabled; Linux only)"`
	HostLoadResume float64       `name:"max-host-load-resume" default:"60" env:"WHISPER_MAX_HOST_LOAD_RESUME" help:"Resume claiming once the host's CPU use by other processes falls below this (percent)"`
	ChildTimeout   float64       `default:"4" env:"WHISPER_CHILD_TIMEOUT_FACTOR" help:"whisper-cli time limit as a multiple of the audio duration (plus 10 min)"`
	ChildUID       int           `name:"child-uid" env:"WHISPER_CHILD_UID" help:"Unprivileged uid, dedicated to whisper-network, to run ffmpeg and whisper-cli as. Required when the worker runs as root; nothing else may run as it."`
	ChildGID       int           `name:"child-gid" env:"WHISPER_CHILD_GID" help:"Unprivileged gid to run ffmpeg and whisper-cli as. Required when the worker runs as root."`
	MetricsListen  string        `env:"WHISPER_METRICS_LISTEN" help:"Serve /metrics on this address (optional; e.g. 127.0.0.1:9091 — the endpoint is unauthenticated)"`
	MetricsPushURL string        `env:"WHISPER_METRICS_PUSH_URL" help:"Push metrics to this VictoriaMetrics import URL (optional)"`
	MetricsPushInt time.Duration `name:"metrics-push-interval" default:"15s" env:"WHISPER_METRICS_PUSH_INTERVAL" help:"Metrics push interval"`
	MetricsLabels  string        `env:"WHISPER_METRICS_LABELS" help:"Extra labels for pushed metrics, k=v,k=v"`
}

// Validate checks the flags that relate to each other.
func (c *WorkerCmd) Validate() error {
	return worker.CheckHostLoad(c.MaxHostLoad, c.HostLoadResume)
}

// Run runs the worker until the context is cancelled (SIGINT/SIGTERM).
func (c *WorkerCmd) Run(ctx context.Context, logger zerolog.Logger) error {
	if err := workerSupported(runtime.GOOS); err != nil {
		return err
	}
	log := obs.NewLogger(logger, obs.ModeWorker)
	cl, err := client.New(c.Server, client.WithUserAgent("whisper-network-worker/"+Version),
		client.WithHTTPClient(&http.Client{}))
	if err != nil {
		return err
	}
	m, stopMetrics, err := c.startMetrics(ctx, log)
	if err != nil {
		return err
	}
	defer stopMetrics()

	kinds := make([]api.TaskKind, len(c.Kinds))
	for i, k := range c.Kinds {
		kinds[i] = api.TaskKind(k)
	}
	// The first SIGTERM/SIGINT cancels ctx (main.go): the worker stops
	// claiming and finishes running tasks. A second one aborts the drain.
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigs)
	stopWatch := make(chan struct{})
	defer close(stopWatch)
	abort := secondSignal(sigs, stopWatch)

	w, err := worker.New(worker.Config{
		Client: cl, StateDir: c.State, Description: c.Description,
		ThreadsPerSlot: c.ThreadsPerSlot, Slots: c.Slots,
		MemoryReserve: int64(c.MemoryReserve), Memory: int64(c.Memory), Kinds: kinds,
		DiskReserve: int64(c.DiskReserve), ModelCacheMax: int64(c.ModelCacheMax),
		MaxHostLoad: c.MaxHostLoad, MaxHostLoadResume: c.HostLoadResume,
		WhisperCLI: c.WhisperCLI, FFmpeg: c.FFmpeg, EngineVersion: c.EngineVersion, Drain: !c.ReleaseOnTerm, Abort: abort,
		ChildUID: c.ChildUID, ChildGID: c.ChildGID,
		Version: Version, Logger: log, Metrics: m,
		Limits: worker.Limits{TimeoutFactor: c.ChildTimeout},
	})
	if err != nil {
		return err
	}
	return w.Run(ctx)
}

// startMetrics starts the optional scrape listener and push loop.
func (c *WorkerCmd) startMetrics(ctx context.Context, log zerolog.Logger) (*obs.Metrics, func(), error) {
	if c.MetricsListen == "" && c.MetricsPushURL == "" {
		return nil, func() {}, nil
	}
	m := obs.NewMetrics(metrics.NewSet())
	var stops []func()
	stop := func() {
		for _, s := range stops {
			s()
		}
	}
	if c.MetricsListen != "" {
		lis, err := net.Listen("tcp", c.MetricsListen)
		if err != nil {
			return nil, stop, fmt.Errorf("metrics listener: %w", err)
		}
		mux := http.NewServeMux()
		mux.Handle("/metrics", m.Handler())
		srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		go func() {
			if err := srv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Warn().Err(err).Msg("metrics listener stopped")
			}
		}()
		stops = append(stops, func() { _ = srv.Close() })
	}
	if c.MetricsPushURL != "" {
		labels, err := obs.ParseLabels(c.MetricsLabels)
		if err != nil {
			stop()
			return nil, func() {}, err
		}
		if _, err := m.StartPush(ctx, obs.PushConfig{URL: c.MetricsPushURL, Interval: c.MetricsPushInt,
			ExtraLabels: labels, Logger: log}); err != nil {
			stop()
			return nil, func() {}, fmt.Errorf("metrics push: %w", err)
		}
	}
	return m, stop, nil
}

// errWorkerUnsupported is returned by the worker on platforms where it
// cannot confine the media tools it runs (ffmpeg and whisper-cli parse
// untrusted input).
var errWorkerUnsupported = errors.New("the worker is not supported on Windows: " +
	"it cannot confine the ffmpeg and whisper-cli child processes there " +
	"(see https://github.com/deweysasser/whisper-network/issues/26)")

// workerSupported reports whether the worker may run on goos.
func workerSupported(goos string) error {
	if goos == "windows" {
		return errWorkerUnsupported
	}
	return nil
}

// secondSignal returns a channel closed when the second signal arrives on
// sigs, which aborts a drain. It stops watching when done is closed.
func secondSignal(sigs <-chan os.Signal, done <-chan struct{}) <-chan struct{} {
	abort := make(chan struct{})
	go func() {
		seen := 0
		for {
			select {
			case <-sigs:
				if seen++; seen == 2 {
					close(abort)
					return
				}
			case <-done:
				return
			}
		}
	}()
	return abort
}

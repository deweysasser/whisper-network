package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/client"
	"github.com/deweysasser/whisper-network/internal/models"
	"github.com/deweysasser/whisper-network/internal/obs"
)

// Errors that end Run.
var (
	// ErrRejected means an operator rejected this worker. Terminal: a new
	// registration would be a new worker.
	ErrRejected = errors.New("worker was rejected by the server")
	// ErrUnknownWorker means the server does not accept this worker's
	// credentials (401). The worker does not silently re-register.
	ErrUnknownWorker = errors.New("server does not recognize this worker's credentials")
)

// Worker is one worker process. Create it with New and call Run once.
type Worker struct {
	cfg Config
	log zerolog.Logger
	sys System

	api   *client.Client // authenticated once credentials are known
	creds *Credentials
	facts api.WorkerFacts
	tools tools
	// offered are the whisper models in facts, smallest first.
	offered []models.Model

	mem      *memBudget
	cache    *ModelCache
	disk     *diskMeter
	gate     *diskGate
	load     *loadGate
	children children
	busy     atomic.Int32

	pollInterval time.Duration
	// serverOutCap is the server's bound on a prepare result
	// (api.Worker.MaxPrepareOutputBytes, refreshed from each prepare
	// lease); 0 until the server has said.
	serverOutCap atomic.Int64
	fatal        context.CancelCauseFunc
}

// tools are the resolved child binaries and their versions.
type tools struct {
	whisper, ffmpeg              string // absolute paths, "" if absent
	engineVersion, ffmpegVersion string
	// whisperRO and ffmpegRO are what each tool's sandbox may read besides
	// the system paths (see toolReadOnly).
	whisperRO, ffmpegRO []string
}

// New validates cfg and returns a worker.
func New(cfg Config) (*Worker, error) {
	if err := cfg.setDefaults(); err != nil {
		return nil, err
	}
	numCPU := cfg.NumCPU
	if numCPU <= 0 {
		numCPU = runtime.NumCPU()
	}
	return &Worker{
		cfg:          cfg,
		log:          cfg.Logger,
		sys:          System{Root: cfg.SysRoot, NumCPU: numCPU},
		pollInterval: cfg.PausedPoll,
	}, nil
}

// Facts returns the facts probed by Run (zero before Run probes).
func (w *Worker) Facts() api.WorkerFacts { return w.facts }

// ID returns the worker ID once registered, else "".
func (w *Worker) ID() string {
	if w.creds == nil {
		return ""
	}
	return w.creds.ID
}

func (w *Worker) credentialsPath() string { return filepath.Join(w.cfg.StateDir, CredentialsFile) }
func (w *Worker) workRoot() string        { return filepath.Join(w.cfg.StateDir, "work") }
func (w *Worker) modelDir() string        { return filepath.Join(w.cfg.StateDir, "models") }

// Run registers if needed, waits for approval and processes tasks until ctx
// is cancelled (returning nil) or the server rejects the worker (returning
// ErrRejected or ErrUnknownWorker).
//
// On cancellation the worker stops claiming. By default it kills running
// children and releases their leases so the tasks requeue at once; with
// Config.Drain it lets running tasks finish and upload first.
func (w *Worker) Run(ctx context.Context) error {
	if err := os.MkdirAll(w.cfg.StateDir, 0o700); err != nil {
		return err
	}
	// Work directories of a previous process are garbage: their leases are
	// long gone or will expire.
	_ = os.RemoveAll(w.workRoot())
	if err := os.MkdirAll(w.workRoot(), 0o700); err != nil {
		return err
	}
	w.disk = newDiskMeter(w.cfg.StateDir, w.modelDir(), w.workRoot(), w.cfg.Catalog, w.cfg.FreeBytes)
	w.gate = newDiskGate(w.disk, w.cfg.DiskReserve)
	// Early, so the load meter has a reading by the time slots start.
	w.load = w.newLoadGate()
	if err := w.shareWithChildren(); err != nil {
		return err
	}
	if err := w.checkDedicatedChildUID(); err != nil {
		return err
	}
	if err := w.probe(ctx); err != nil {
		return err
	}
	if err := w.ensureCredentials(ctx); err != nil {
		return err
	}
	w.log = obs.WithWorker(w.log, w.creds.ID)

	runCtx, fatal := context.WithCancelCause(ctx)
	defer fatal(nil)
	w.fatal = fatal
	if err := w.awaitActive(runCtx); err != nil {
		return w.result(ctx, runCtx, err)
	}

	workCtx := runCtx
	if w.cfg.Drain {
		// Running tasks survive the shutdown signal but not a fatal error.
		var cancelWork context.CancelCauseFunc
		workCtx, cancelWork = context.WithCancelCause(context.WithoutCancel(ctx))
		defer cancelWork(nil)
		stop := context.AfterFunc(runCtx, func() {
			if cause := context.Cause(runCtx); isFatal(cause) {
				cancelWork(cause)
			}
		})
		defer stop()
	}

	w.mem = newMemBudget(w.budget())
	w.cache = NewModelCache(workCtx, w.modelDir(), w.fetchModel).WithMax(w.cfg.ModelCacheMax)
	w.cache.evict() // a lowered cap applies at once

	w.log.Info().Int("slots", w.facts.Slots).Strs("models", w.facts.Models).
		Strs("kinds", kindStrings(w.facts.Kinds)).Msg("worker active; starting slots")
	var wg sync.WaitGroup
	for i := range w.facts.Slots {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.slot(runCtx, workCtx, i)
		}()
	}
	wg.Wait()
	return w.result(ctx, runCtx, nil)
}

// result maps how Run ended to its return value.
func (w *Worker) result(ctx, runCtx context.Context, err error) error {
	if cause := context.Cause(runCtx); isFatal(cause) {
		return cause
	}
	if err != nil && ctx.Err() == nil {
		return err
	}
	w.log.Info().Msg("worker stopped")
	return nil
}

// fail stops the whole worker with a fatal error.
func (w *Worker) fail(err error) {
	switch {
	case client.IsRejected(err):
		err = ErrRejected
	case client.IsUnauthorized(err):
		err = fmt.Errorf("%w; delete %s to register as a new worker", ErrUnknownWorker, w.credentialsPath())
	}
	w.log.Error().Err(err).Msg("stopping worker")
	w.fatal(err)
}

// isFatal reports errors that must stop the worker.
func isFatal(err error) bool {
	return errors.Is(err, ErrRejected) || errors.Is(err, ErrUnknownWorker) ||
		client.IsRejected(err) || client.IsUnauthorized(err)
}

// budget is the memory children may use.
func (w *Worker) budget() int64 { return w.memoryLimit() - w.cfg.MemoryReserve }

func (w *Worker) memoryLimit() int64 {
	if w.cfg.Memory > 0 {
		return w.cfg.Memory
	}
	return w.sys.MemoryLimit()
}

// fetchModel downloads a model from the server.
func (w *Worker) fetchModel(ctx context.Context, m models.Model) (io.ReadCloser, error) {
	d, err := w.api.Model(ctx, m.Name)
	if err != nil {
		return nil, err
	}
	if d.Size >= 0 && d.Size != m.Size {
		d.Body.Close()
		return nil, fmt.Errorf("%w: server offers %d bytes, want %d", ErrModelMismatch, d.Size, m.Size)
	}
	return d.Body, nil
}

func kindStrings(ks []api.TaskKind) []string {
	out := make([]string, len(ks))
	for i, k := range ks {
		out[i] = string(k)
	}
	return out
}

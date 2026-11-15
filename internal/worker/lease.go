package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/client"
	"github.com/deweysasser/whisper-network/internal/obs"
)

// taskError is a task failure to report to the server.
type taskError struct {
	reason    string
	stderr    string
	retryable bool
	err       error
	// stage is how far the task got (DESIGN.md §8.2); see staged.
	stage api.FailureStage
}

func (e *taskError) Error() string {
	if e.err != nil {
		return e.reason + ": " + e.err.Error()
	}
	return e.reason
}

func (e *taskError) Unwrap() error { return e.err }

// retryableErr wraps err as a retryable task failure.
func retryableErr(reason string, err error) *taskError {
	return &taskError{reason: reason, retryable: true, err: err}
}

// preWork is a failure before the worker touched the task's input: a
// problem with the worker's own configuration.
func preWork(reason string, err error) *taskError {
	return &taskError{reason: reason, retryable: true, err: err, stage: api.StagePreWork}
}

// staged sets the stage of a task failure that has none yet. Other errors
// (cancellation, lease loss) pass through unchanged.
func staged(stage api.FailureStage, err error) error {
	var te *taskError
	if errors.As(err, &te) && te.stage == "" {
		te.stage = stage
	}
	return err
}

// taskStats describe a successful task for logs and metrics.
type taskStats struct {
	model     string
	computeMS int64
	audioS    float64
}

// errFinished is the cancellation cause once a task has run its course.
var errFinished = errors.New("task finished")

// errAborted cancels running tasks when a drain is aborted; they are
// released, not reported as failed.
var errAborted = errors.New("drain aborted")

// leaseRun is the state a lease's task shares with its heartbeat.
type leaseRun struct {
	// uploading is set once the result upload starts. From then on the
	// upload's own response decides the outcome: a heartbeat's 410 may only
	// mean the server already accepted the result (the lease ended as
	// done) before the worker heard back, so it must not cancel the upload.
	// If the lease is really gone, the upload gets its own 410.
	uploading atomic.Bool
}

// handleLease runs one lease to its end: done, failed, abandoned because the
// server ended it, or released on shutdown.
func (w *Worker) handleLease(runCtx, workCtx context.Context, l *api.Lease) {
	log := w.log.With().Str(obs.FieldJobID, l.Job).Int(obs.FieldTaskSeq, l.Seq).
		Str(obs.FieldTaskKind, string(l.Kind)).Str(obs.FieldLeaseID, l.ID).Logger()
	if l.Model != "" {
		log = log.With().Str(obs.FieldModel, l.Model).Logger()
	}
	log.Info().Msg("lease claimed")

	taskCtx, cancel := context.WithCancelCause(workCtx)
	defer cancel(nil)
	run := &leaseRun{}
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		w.heartbeat(taskCtx, l, run, cancel, log)
	}()

	stats, err := w.execute(taskCtx, l, run, log)
	cancel(errFinished)
	<-hbDone
	cause := context.Cause(taskCtx)

	switch {
	case err == nil:
		w.logDone(log, l, stats)
	case isFatal(cause) || isFatal(err):
		// The worker is stopping; the server already revoked everything.
	case errors.Is(cause, errLeaseGone) || client.IsLeaseGone(err):
		obs.Warn(&log, obs.EventTaskFailed).Str(obs.FieldOutcome, string(api.OutcomeCancelled)).
			Msg("lease ended by the server; task abandoned")
	case runCtx.Err() != nil && workCtx.Err() != nil:
		w.release(log, l)
	case client.IsResultInvalid(err):
		obs.Warn(&log, obs.EventTaskFailed).Str(obs.FieldOutcome, string(api.OutcomeFailed)).Err(err).
			Msg("server rejected the result")
	default:
		w.reportFailure(workCtx, log, l, err)
	}
}

func (w *Worker) execute(ctx context.Context, l *api.Lease, run *leaseRun, log zerolog.Logger) (taskStats, error) {
	dir, err := w.leaseDir(l.ID)
	if err != nil {
		return taskStats{}, preWork("creating work directory", err)
	}
	w.disk.invalidate()
	defer func() {
		_ = os.RemoveAll(dir)
		w.disk.invalidate()
	}()
	if err := w.giveToChild(dir); err != nil {
		return taskStats{}, preWork("preparing work directory", err)
	}
	switch l.Kind {
	case api.KindPrepare:
		return w.prepare(ctx, l, dir, run, log)
	case api.KindTranscribe:
		return w.transcribe(ctx, l, dir, run, log)
	}
	return taskStats{}, preWork(fmt.Sprintf("unknown task kind %q", l.Kind), nil)
}

// checkInputURL refuses, before any work, a lease whose input_url the
// client would not fetch (another origin): that is the worker's or the
// server's configuration, not the task's fault.
func (w *Worker) checkInputURL(l *api.Lease) error {
	if err := w.api.CheckInputURL(l.ID, l.InputURL); err != nil {
		return preWork("refusing the lease's input_url", err)
	}
	return nil
}

var unsafeChars = regexp.MustCompile(`[^A-Za-z0-9_.-]`)

// leaseDir creates the lease's private work directory.
func (w *Worker) leaseDir(leaseID string) (string, error) {
	name := unsafeChars.ReplaceAllString(leaseID, "_")
	if name == "" || name == "." || name == ".." {
		name = "lease"
	}
	return os.MkdirTemp(w.workRoot(), name+"-")
}

// download fetches the lease's input into path with Range resume, refusing
// more than maxBytes (0: no bound).
func (w *Worker) download(ctx context.Context, l *api.Lease, path string, maxBytes int64, log zerolog.Logger) (int64, error) {
	f, err := w.createForChild(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n, err := w.api.DownloadInput(ctx, l.ID, l.InputURL, f, client.ResumeOptions{
		Retry: w.cfg.RetryBackoff, Sleep: w.cfg.Sleep, MaxAttempts: 8, MaxBytes: maxBytes,
		OnRetry: func(attempt int, offset int64, err error) {
			log.Warn().Err(err).Int("attempt", attempt).Int64("offset", offset).Msg("input download interrupted; resuming")
		},
	})
	if err != nil {
		return n, err
	}
	return n, f.Close()
}

// putWithRetry retries a result upload until it succeeds or fails for a
// reason retrying cannot fix (410, 422, 4xx, cancellation).
func (w *Worker) putWithRetry(ctx context.Context, log zerolog.Logger, put func(context.Context) error) error {
	for attempt := 0; ; attempt++ {
		err := put(ctx)
		if err == nil || !client.IsTransient(err) || ctx.Err() != nil {
			return err
		}
		d := w.cfg.RetryBackoff.Delay(attempt)
		log.Warn().Err(err).Dur("retry_in", d).Msg("result upload failed; retrying")
		if err := w.cfg.Sleep(ctx, d); err != nil {
			return err
		}
	}
}

// reportTimeout bounds the failure report and the release on shutdown.
const reportTimeout = 30 * time.Second

func (w *Worker) reportFailure(workCtx context.Context, log zerolog.Logger, l *api.Lease, err error) {
	var te *taskError
	if !errors.As(err, &te) {
		te = retryableErr("worker error", err)
	}
	rep := api.FailureReport{Reason: te.Error(), StderrTail: obs.StderrTail(te.stderr), Retryable: te.retryable,
		Stage: te.stage}
	ev := obs.Warn(&log, obs.EventTaskFailed).Str(obs.FieldOutcome, string(api.OutcomeFailed)).
		Str(obs.FieldError, rep.Reason).Bool("retryable", rep.Retryable).Str("stage", string(rep.Stage))
	if rep.StderrTail != "" {
		ev = ev.Str(obs.FieldStderrTail, rep.StderrTail)
	}
	ev.Msg("task failed")

	ctx, cancel := context.WithTimeout(context.WithoutCancel(workCtx), reportTimeout)
	defer cancel()
	if err := w.putWithRetry(ctx, log, func(ctx context.Context) error {
		return w.api.ReportFailure(ctx, l.ID, rep)
	}); err != nil {
		log.Warn().Err(err).Msg("could not report the failure; the lease will expire")
	}
}

func (w *Worker) release(log zerolog.Logger, l *api.Lease) {
	ctx, cancel := context.WithTimeout(context.Background(), reportTimeout)
	defer cancel()
	if err := w.api.Release(ctx, l.ID); err != nil {
		log.Warn().Err(err).Msg("could not release the lease on shutdown; it will expire")
		return
	}
	obs.Info(&log, obs.EventLeaseReleased).Str(obs.FieldOutcome, string(api.OutcomeReleased)).
		Msg("lease released on shutdown")
}

func (w *Worker) logDone(log zerolog.Logger, l *api.Lease, s taskStats) {
	ev := obs.Info(&log, obs.EventTaskDone).Str(obs.FieldOutcome, string(api.OutcomeDone)).
		Int64(obs.FieldComputeMS, s.computeMS)
	if s.audioS > 0 {
		rtf := float64(s.computeMS) / 1000 / s.audioS
		ev = ev.Float64(obs.FieldAudioSeconds, s.audioS).Float64(obs.FieldRTF, rtf)
		if w.cfg.Metrics != nil && l.Kind == api.KindTranscribe {
			w.cfg.Metrics.TaskRealtimeFactor(s.model, w.creds.ID, rtf)
			w.cfg.Metrics.WorkerAudio(w.creds.ID, s.model, s.audioS)
		}
	}
	if w.cfg.Metrics != nil {
		w.cfg.Metrics.TaskCompute(l.Kind, s.model, float64(s.computeMS)/1000)
	}
	ev.Msg("task done")
}

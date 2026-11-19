package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/client"
	"github.com/deweysasser/whisper-network/internal/obs"
)

// ensureCredentials loads the credentials file, registering first if there is
// none. Credentials minted by another server are refused rather than sent to
// this one.
func (w *Worker) ensureCredentials(ctx context.Context) error {
	path := w.credentialsPath()
	creds, err := LoadCredentials(path)
	switch {
	case err == nil:
		if creds.Server != "" && creds.Server != w.cfg.Client.BaseURL() {
			return fmt.Errorf("%s belongs to server %s, not %s; use another --state or delete the file to register anew",
				path, creds.Server, w.cfg.Client.BaseURL())
		}
	case errors.Is(err, os.ErrNotExist):
		creds, err = w.register(ctx)
		if err != nil {
			return err
		}
		if err := SaveCredentials(path, creds); err != nil {
			return fmt.Errorf("saving credentials: %w", err)
		}
	default:
		return err
	}
	w.creds = creds
	w.api = w.cfg.Client.WithToken(creds.Token)
	return nil
}

func (w *Worker) register(ctx context.Context) (*Credentials, error) {
	var reg *api.RegisterResponse
	err := w.retry(ctx, "register", func(ctx context.Context) (err error) {
		reg, err = w.cfg.Client.Register(ctx, w.facts)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("registering: %w", err)
	}
	w.log.Info().Str(obs.FieldWorkerID, reg.ID).Msg("registered with server")
	return &Credentials{Server: w.cfg.Client.BaseURL(), ID: reg.ID, Token: reg.Token, RegisteredAt: time.Now().UTC()}, nil
}

// awaitActive polls the worker's state until it is active (or paused, which
// still lets slots run: their claims slow-poll until resumed), patching the
// facts on the server when they changed since the last start.
func (w *Worker) awaitActive(ctx context.Context) error {
	announced := false
	patched := false
	for attempt := 0; ; attempt++ {
		var st *api.Worker
		err := w.retry(ctx, "get worker state", func(ctx context.Context) (err error) {
			st, err = w.api.Worker(ctx, w.creds.ID)
			return err
		})
		if err != nil {
			if isFatal(err) {
				w.fail(err)
				return context.Cause(ctx)
			}
			return err
		}
		if st.PollIntervalS > 0 {
			w.pollInterval = time.Duration(st.PollIntervalS) * time.Second
		}
		if st.MaxPrepareOutputBytes > 0 {
			w.serverOutCap.Store(st.MaxPrepareOutputBytes)
		}
		if !patched {
			patched = true
			if err := w.patchFacts(ctx, st.Facts); err != nil {
				if isFatal(err) {
					w.fail(err)
					return context.Cause(ctx)
				}
				w.log.Warn().Err(err).Msg("could not update facts on the server")
			}
		}
		switch st.State {
		case api.WorkerActive, api.WorkerPaused:
			if st.State == api.WorkerPaused {
				w.log.Warn().Msg("worker is paused; slots will poll slowly until resumed")
			}
			return nil
		case api.WorkerRejected:
			w.fail(ErrRejected)
			return context.Cause(ctx)
		}
		if !announced {
			announced = true
			w.log.Warn().Str("description", w.facts.Description).Str("hostname", w.facts.Hostname).
				Msg("worker awaiting approval; approve this worker_id on the server")
		}
		if err := w.cfg.Sleep(ctx, w.cfg.PendingBackoff.Delay(attempt)); err != nil {
			return err
		}
	}
}

// patchFacts sends the probed facts if the server's copy differs.
func (w *Worker) patchFacts(ctx context.Context, server api.WorkerFacts) error {
	if factsEqual(server, w.facts) {
		return nil
	}
	w.log.Info().Msg("facts changed since last start; updating server")
	return w.retry(ctx, "patch facts", func(ctx context.Context) error {
		return w.api.PatchWorker(ctx, w.creds.ID, w.facts)
	})
}

func factsEqual(a, b api.WorkerFacts) bool {
	norm := func(f api.WorkerFacts) api.WorkerFacts {
		f.Models = slices.Sorted(slices.Values(f.Models))
		f.Kinds = slices.Sorted(slices.Values(f.Kinds))
		return f
	}
	a, b = norm(a), norm(b)
	return slices.Equal(a.Models, b.Models) && slices.Equal(a.Kinds, b.Kinds) &&
		a.Hostname == b.Hostname && a.Description == b.Description && a.CPUModel == b.CPUModel &&
		a.Cores == b.Cores && a.MemBytes == b.MemBytes && a.OS == b.OS && a.Arch == b.Arch &&
		a.EngineVersion == b.EngineVersion && a.FFmpegVersion == b.FFmpegVersion &&
		a.SandboxLevel == b.SandboxLevel && a.Slots == b.Slots && a.Version == b.Version
}

// retry runs op until it succeeds, fails non-transiently, or ctx ends,
// spacing attempts with the retry backoff.
func (w *Worker) retry(ctx context.Context, what string, op func(context.Context) error) error {
	for attempt := 0; ; attempt++ {
		err := op(ctx)
		if err == nil || !client.IsTransient(err) || ctx.Err() != nil {
			return err
		}
		d := w.cfg.RetryBackoff.Delay(attempt)
		w.log.Warn().Err(err).Dur("retry_in", d).Msg(what + " failed; retrying")
		if err := w.cfg.Sleep(ctx, d); err != nil {
			return err
		}
	}
}

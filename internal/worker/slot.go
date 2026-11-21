package worker

import (
	"context"
	"time"

	"github.com/rs/zerolog"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/client"
	"github.com/deweysasser/whisper-network/internal/obs"
)

// claimSlack is how much longer than the server's long-poll wait the client
// waits for the response.
const claimSlack = 15 * time.Second

// slot is one slot goroutine (DESIGN.md §7.2): wait while the host is busy
// with other work (§7.3), reserve memory for what fits, claim, run the lease, release the memory, repeat. It returns when runCtx is
// cancelled, after its current lease has been finished or released.
func (w *Worker) slot(runCtx, workCtx context.Context, n int) {
	log := w.log.With().Int("slot", n).Logger()
	failures := 0
	var sl slotLoad
	defer w.load.leave(&sl)
	for runCtx.Err() == nil {
		if w.load.enabled() && !w.waitForLoad(runCtx, &sl, log) {
			return
		}
		off, res, err := w.mem.reserve(runCtx, w.facts.Kinds, w.offered, w.cfg.Limits.PrepareMemory, w.cfg.Limits.FetchMemory)
		if err != nil {
			return
		}
		if off = w.gate.filter(off); off.empty() {
			// Every kind is withheld for lack of disk: wait for space.
			res.release()
			log.Debug().Dur("retry_in", w.pollInterval).Msg("no disk space for any offered work; waiting")
			if w.cfg.Sleep(runCtx, w.pollInterval) != nil {
				return
			}
			continue
		}
		lease, err := w.claim(runCtx, off)
		if err != nil || lease == nil {
			res.release()
			if err == nil {
				failures = 0
				continue
			}
			var delay time.Duration
			switch {
			case runCtx.Err() != nil:
				return
			case isFatal(err):
				w.fail(err)
				return
			case client.IsPaused(err), client.IsPending(err):
				delay = w.pollInterval
				log.Debug().Err(err).Dur("retry_in", delay).Msg("not allowed to claim; polling slowly")
			default:
				delay = w.cfg.RetryBackoff.Delay(failures)
				failures++
				log.Warn().Err(err).Dur("retry_in", delay).Msg("claim failed")
			}
			if w.cfg.Sleep(runCtx, delay) != nil {
				return
			}
			continue
		}
		failures = 0
		need := w.leaseDisk(lease)
		releaseDisk, avail, ok := w.gate.admit(lease.Kind, need)
		if !ok {
			res.release()
			w.refuse(log, lease, need, avail)
			continue
		}
		res.shrink(w.leaseMemory(lease))
		w.busy.Add(1)
		w.handleLease(runCtx, workCtx, lease)
		w.busy.Add(-1)
		releaseDisk()
		res.release()
	}
}

// claim long-polls for a lease. The request carries the worker's gauges:
// heartbeats only flow while a lease is held, so this is how an idle worker
// keeps its disk and load readings current on the server.
func (w *Worker) claim(ctx context.Context, off offer) (*api.Lease, error) {
	ctx, cancel := context.WithTimeout(ctx, w.cfg.ClaimWait+claimSlack)
	defer cancel()
	g := w.gauges()
	return w.api.Claim(ctx, w.creds.ID, api.ClaimRequest{Kinds: off.kinds, Models: off.models, Gauges: &g}, w.cfg.ClaimWait)
}

// refuse hands back a lease whose task would not fit on disk. A voluntary
// release consumes no attempt, so the task goes to a worker with room; this
// worker withholds the kind from its claims until space frees (diskGate).
func (w *Worker) refuse(log zerolog.Logger, l *api.Lease, need, avail int64) {
	log = log.With().Str(obs.FieldLeaseID, l.ID).Str(obs.FieldJobID, l.Job).Int(obs.FieldTaskSeq, l.Seq).
		Str(obs.FieldTaskKind, string(l.Kind)).Logger()
	ctx, cancel := context.WithTimeout(context.Background(), reportTimeout)
	defer cancel()
	if err := w.api.Release(ctx, l.ID); err != nil {
		log.Warn().Err(err).Msg("could not release a lease that does not fit on disk; it will expire")
		return
	}
	obs.Warn(&log, obs.EventLeaseReleased).Str(obs.FieldOutcome, string(api.OutcomeReleased)).
		Str(obs.FieldReason, "insufficient disk").Int64("need_bytes", need).Int64("available_bytes", avail).
		Int64("disk_reserve_bytes", w.cfg.DiskReserve).Msg("lease released: the task does not fit on disk")
}

// leaseMemory is the memory a lease's work needs.
func (w *Worker) leaseMemory(l *api.Lease) int64 {
	switch l.Kind {
	case api.KindPrepare:
		return w.cfg.Limits.PrepareMemory
	case api.KindFetch:
		return w.cfg.Limits.FetchMemory
	}
	if m, ok := w.cfg.Catalog.Lookup(l.Model); ok {
		return m.PeakRSS
	}
	return 0
}

package worker

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/rs/zerolog"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/client"
)

// errLeaseGone is the cancellation cause when the server says the lease is
// no longer live.
var errLeaseGone = errors.New("lease is no longer live")

// defaultHeartbeat is used when a lease carries no interval.
const defaultHeartbeat = 30 * time.Second

// heartbeat extends the lease every interval until ctx ends. It runs
// independently of the child, so a long whisper run never starves it. On 410
// it cancels the task with errLeaseGone, which kills the child; on a
// rejection it stops the whole worker.
func (w *Worker) heartbeat(ctx context.Context, l *api.Lease, run *leaseRun, cancel context.CancelCauseFunc, log zerolog.Logger) {
	interval := time.Duration(l.HeartbeatIntervalS) * time.Second
	if interval <= 0 {
		interval = defaultHeartbeat
	}
	delay := interval
	failures := 0
	for {
		if w.cfg.Sleep(ctx, delay) != nil {
			return
		}
		g := w.gauges()
		_, err := w.api.Heartbeat(ctx, l.ID, &g)
		switch {
		case err == nil:
			failures, delay = 0, interval
		case ctx.Err() != nil:
			return
		case isFatal(err):
			cancel(errLeaseGone)
			w.fail(err)
			return
		case client.IsLeaseGone(err) && run.uploading.Load():
			// Most likely the result was just accepted; the upload's
			// response says for sure. Stop heartbeating, cancel nothing.
			log.Debug().Msg("lease ended during the result upload; the upload decides")
			return
		case client.IsLeaseGone(err):
			log.Warn().Msg("server ended the lease; killing the task")
			cancel(errLeaseGone)
			return
		default:
			// Keep trying faster than the interval: the lease lives for
			// about three intervals.
			delay = min(interval, w.cfg.RetryBackoff.Delay(failures))
			failures++
			log.Warn().Err(err).Dur("retry_in", delay).Msg("heartbeat failed")
		}
	}
}

// gauges are the worker readings carried on heartbeats and claims (DESIGN.md
// §10.2).
func (w *Worker) gauges() api.Gauges {
	var rss int64
	for _, pid := range w.children.list() {
		rss += w.sys.RSS(pid)
	}
	g := api.Gauges{
		SlotsBusy:        int(w.busy.Load()),
		ChildRSSBytes:    rss,
		Load1:            w.sys.Load1(),
		CPUThrottledSecs: w.sys.CPUThrottled().Seconds(),
	}
	if w.disk != nil {
		r := w.disk.read()
		r.gauges(&g)
		if w.gate != nil && slices.Contains(w.facts.Kinds, api.KindPrepare) {
			w.gate.prepareBounds(r, w.prepareOutputCap(nil), &g)
		}
	}
	w.load.gauges(&g)
	return g
}

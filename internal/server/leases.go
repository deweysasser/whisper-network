package server

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/store"
)

// claimState maps a worker state to the claim refusal, nil when active.
func claimState(st api.WorkerState) *problem {
	var p *problem
	switch st {
	case api.WorkerActive:
		return nil
	case api.WorkerPending:
		p = newProblem(http.StatusForbidden, api.ProblemWorkerPending, "worker is awaiting approval")
	case api.WorkerPaused:
		p = newProblem(http.StatusConflict, api.ProblemWorkerPaused, "worker is paused")
	default:
		p = newProblem(http.StatusGone, api.ProblemWorkerRejected, "worker was rejected; exit")
	}
	p.state = string(st)
	return p
}

// eligibility computes what this claim may take: the kinds and models the
// slot offers right now (models intersected with the allowlist), and the
// worker's sandbox level, which the store compares with the prepare policy.
// The worker's queues are read by the claim transaction itself, so a
// process grant revoked while the claim long-polls is honoured.
func (s *Server) eligibility(wk *store.Worker, req api.ClaimRequest) store.Eligibility {
	kinds := req.Kinds
	if len(kinds) == 0 {
		kinds = wk.Facts.Kinds
	}
	offered := req.Models
	if len(offered) == 0 {
		offered = wk.Facts.Models
	}
	var ms []string
	for _, m := range offered {
		if _, ok := s.allowed[m]; ok && !slices.Contains(ms, m) {
			ms = append(ms, m)
		}
	}
	el := store.Eligibility{Kinds: kinds, Models: ms, SandboxLevel: wk.Facts.SandboxLevel}
	if g := req.Gauges; g != nil && g.PrepareMaxInputBytes != nil {
		// Only offer prepare tasks the worker has room for; a worker that
		// cannot fit even the smallest gets none (a bound of 0 or less).
		el.PrepareMaxInput = max(*g.PrepareMaxInputBytes, 0)
		el.PrepareBounded = true
	}
	return el
}

// handleClaim is POST /worker/{id}/leases?wait=30s: lease one task, long
// polling up to wait (capped at MaxClaimWait). Returns 201 with the lease,
// 204 when nothing became available, 403 while pending, 409 while paused.
func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request, p *Principal) error {
	ctx := r.Context()
	if p.ID != r.PathValue("id") {
		return s.hide(r, p, "another worker")
	}
	wait, err := parseWait(r.URL.Query().Get("wait"), s.cfg.MaxClaimWait)
	if err != nil {
		return err
	}
	var req api.ClaimRequest
	if err := decodeWorkerJSON(w, r, &req, true, maxJSONBody); err != nil {
		return err
	}
	for _, k := range req.Kinds {
		if k != api.KindPrepare && k != api.KindTranscribe {
			return badRequest("unknown task kind " + string(k))
		}
	}
	wk := p.Worker
	if prob := claimState(wk.State); prob != nil {
		return prob
	}
	// Idle workers send no heartbeats; their claims carry the gauges.
	if err := s.st.TouchWorker(ctx, wk.ID, sanitizeGauges(req.Gauges)); err != nil {
		return err
	}

	start := time.Now()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	el := s.eligibility(wk, req)
	for {
		// Take the wake-up channel before claiming so a notification that
		// races with an empty claim is not lost.
		woke := s.disp.Wait()
		lease, err := s.st.Claim(ctx, wk.ID, el, s.leaseTTL())
		if errors.Is(err, store.ErrWorkerInactive) {
			cur, gerr := s.st.WorkerByID(ctx, wk.ID)
			if gerr != nil {
				return gerr
			}
			return claimState(cur.State)
		}
		if err != nil {
			return err
		}
		if lease != nil {
			s.metrics.ClaimWait(time.Since(start))
			s.leased(lease)
			if s.afterClaim != nil {
				s.afterClaim(ctx)
			}
			if ctx.Err() != nil {
				// The worker disconnected (typically shutting down) while the
				// claim committed, so it will never learn of this lease. Release
				// it now: the task requeues at once with no attempt consumed,
				// instead of waiting out the lease TTL and burning an attempt.
				s.releaseOrphan(context.WithoutCancel(ctx), lease, wk.ID)
				return nil
			}
			writeJSON(w, http.StatusCreated, s.leaseAPI(lease))
			return nil
		}
		if wait == 0 {
			break
		}
		select {
		case <-woke:
			continue
		case <-timer.C:
		case <-s.disp.Closing():
		case <-ctx.Done():
			return nil // the worker went away; nothing to answer
		}
		break
	}
	s.metrics.ClaimWait(time.Since(start))
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Server) leased(l *store.Lease) {
	obs.Info(&s.log, obs.EventTaskLeased).Str(obs.FieldJobID, l.JobID).Int(obs.FieldTaskSeq, l.Seq).
		Str(obs.FieldTaskKind, string(l.Kind)).Str(obs.FieldLeaseID, l.ID).Str(obs.FieldWorkerID, l.WorkerID).
		Str(obs.FieldQueue, l.QueueName).Str(obs.FieldWeight, string(l.Weight)).Float64(obs.FieldCost, l.Cost).
		Float64(obs.FieldVTime, l.VTime).Int(obs.FieldAttempt, l.Attempt).
		Msg("task leased")
	s.metrics.ServedCost(l.QueueName, l.Weight, l.Cost)
}

// leaseAPI builds the claim response, adding model and VAD hashes.
//
// input_url stays relative: the worker resolves it against its own --server
// base, which may legitimately differ from --public-url (a worker on the
// server's host or private network reaching it directly while clients use a
// proxy). The worker refuses absolute URLs on another origin, so making
// this absolute would break every such worker (issue #29).
func (s *Server) leaseAPI(l *store.Lease) api.Lease {
	out := l.API(s.cfg.HeartbeatInterval)
	if l.Kind == api.KindPrepare {
		out.MaxOutputBytes = s.maxCanonicalBytes()
	}
	if l.Kind == api.KindTranscribe {
		if m, err := s.lookupModel(l.Model); err == nil {
			out.ModelSHA256 = m.SHA256
		}
		if s.vad != nil {
			out.VADModel, out.VADModelSHA256 = s.vad.Name, s.vad.SHA256
		}
	}
	return out
}

// handleGauges is POST /worker/{id}/gauges: record the worker's gauges
// without claiming. A worker whose host is too busy to take work sends
// these instead of claims (DESIGN.md §7.3). Active and paused workers only.
func (s *Server) handleGauges(w http.ResponseWriter, r *http.Request, p *Principal) error {
	if p.ID != r.PathValue("id") {
		return s.hide(r, p, "another worker")
	}
	var g api.Gauges
	if err := decodeWorkerJSON(w, r, &g, false, maxJSONBody); err != nil {
		return err
	}
	if st := p.Worker.State; st != api.WorkerPaused {
		if prob := claimState(st); prob != nil {
			return prob
		}
	}
	if err := s.st.TouchWorker(r.Context(), p.ID, sanitizeGauges(&g)); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// handleHeartbeat extends a live lease (and records worker gauges). 410
// tells the worker to kill its child: the lease expired, was cancelled, or
// its job was deleted; or it may not be extended any more, because the
// worker lost process on the lease's queue, is no longer active or paused,
// or the lease is older than its maximum age (leaseAgeLimit).
func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request, p *Principal) error {
	ctx := r.Context()
	var req api.HeartbeatRequest
	if err := decodeWorkerJSON(w, r, &req, true, maxJSONBody); err != nil {
		return err
	}
	if err := s.st.TouchWorker(ctx, p.ID, sanitizeGauges(req.Gauges)); err != nil {
		return err
	}
	exp, err := s.st.Heartbeat(ctx, r.PathValue("id"), p.ID, s.leaseTTL(),
		store.LeaseAgeLimit{Min: s.cfg.MaxLeaseAge, PerCost: s.cfg.LeaseAgeFactor})
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, api.HeartbeatResponse{ExpiresAt: exp})
	return nil
}

const maxReasonLen = 1000

// handleFailure ends a lease as failed; the task requeues for another worker
// if retryable and attempts remain, otherwise the task fails for good and
// the job fails once its other live leases end. A pre-work failure requeues
// without counting an attempt (DESIGN.md §8.2). An unknown stage is
// treated as none, so a newer worker's report still works.
func (s *Server) handleFailure(w http.ResponseWriter, r *http.Request, p *Principal) error {
	var rep api.FailureReport
	if err := decodeWorkerJSON(w, r, &rep, false, 64<<10); err != nil {
		return err
	}
	reason := strings.TrimSpace(rep.Reason)
	if reason == "" {
		reason = "worker reported failure"
	}
	if len(reason) > maxReasonLen {
		reason = strings.ToValidUTF8(reason[:maxReasonLen], "")
	}
	stage := rep.Stage
	if !stage.Valid() {
		stage = ""
	}
	end, err := s.st.FailStage(r.Context(), r.PathValue("id"), p.ID, reason, rep.Retryable, stage)
	if err != nil {
		return err
	}
	obs.Warn(&s.log, obs.EventTaskFailed).Str(obs.FieldLeaseID, end.LeaseID).Str(obs.FieldJobID, end.JobID).
		Int(obs.FieldTaskSeq, end.Seq).Str(obs.FieldTaskKind, string(end.Kind)).Str(obs.FieldWorkerID, p.ID).
		Int(obs.FieldAttempt, end.Attempt).Bool("retryable", rep.Retryable).Str("stage", string(stage)).
		Str(obs.FieldError, reason).
		Str(obs.FieldStderrTail, obs.StderrTail(rep.StderrTail)).Msg("task failed")
	s.leaseEnded(r.Context(), end)
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// handleRelease is DELETE /lease/{id}: a voluntary release on graceful
// worker shutdown, which does not consume an attempt.
func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request, p *Principal) error {
	end, err := s.st.Release(r.Context(), r.PathValue("id"), p.ID)
	if err != nil {
		return err
	}
	obs.Info(&s.log, obs.EventLeaseReleased).Str(obs.FieldLeaseID, end.LeaseID).Str(obs.FieldJobID, end.JobID).
		Int(obs.FieldTaskSeq, end.Seq).Str(obs.FieldWorkerID, p.ID).Int(obs.FieldAttempt, end.Attempt).
		Msg("lease released")
	s.leaseEnded(r.Context(), end)
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// releaseOrphan releases a lease granted to a worker that disconnected before
// it could receive it. A remaining window, after the response reaches the
// kernel but before the worker reads it, is covered by lease expiry.
func (s *Server) releaseOrphan(ctx context.Context, l *store.Lease, workerID string) {
	end, err := s.st.Release(ctx, l.ID, workerID)
	if err != nil {
		s.log.Warn().Err(err).Str(obs.FieldLeaseID, l.ID).Msg("could not release a lease the worker never received")
		return
	}
	obs.Info(&s.log, obs.EventLeaseReleased).Str(obs.FieldLeaseID, end.LeaseID).Str(obs.FieldJobID, end.JobID).
		Int(obs.FieldTaskSeq, end.Seq).Str(obs.FieldWorkerID, workerID).Int(obs.FieldAttempt, end.Attempt).
		Str(obs.FieldReason, "worker disconnected before receiving the lease").
		Msg("lease released")
	s.leaseEnded(ctx, end)
}

// leaseEnded does the bookkeeping common to every non-success lease end:
// metrics, the circuit breaker's report, waking claims when the task
// requeued, and job failure.
func (s *Server) leaseEnded(ctx context.Context, end *store.LeaseEnd) {
	if end == nil {
		return
	}
	s.metrics.LeaseEnded(end.Outcome, end.Kind)
	if p := end.AutoPaused; p != nil {
		// Paused and audited in the transaction that ended the lease.
		obs.Warn(&s.log, obs.EventWorkerAutoPaused).Str(obs.FieldWorkerID, p.WorkerID).
			Str(obs.FieldActor, store.SystemActor).Str(obs.FieldReason, p.Reason).
			Int(obs.FieldFailures, p.Failures).Int(obs.FieldJobs, p.Jobs).Str(obs.FieldLeaseID, end.LeaseID).
			Str(obs.FieldJobID, end.JobID).Msg("worker paused by the circuit breaker")
		s.metrics.WorkerAutoPaused(p.WorkerID)
	}
	if end.Requeued {
		s.disp.Notify()
	}
	if end.JobFailed {
		s.jobFinished(ctx, end.JobID)
	}
}

// jobFinished logs job.done / job.failed and records the job latency
// metrics for a job that just reached a terminal state.
func (s *Server) jobFinished(ctx context.Context, jobID string) {
	j, err := s.st.Job(ctx, jobID)
	if err != nil {
		s.log.Warn().Err(err).Str(obs.FieldJobID, jobID).Msg("loading finished job")
		return
	}
	a := j.API(s.now())
	var wait, elapsed float64
	if a.WaitS != nil {
		wait = *a.WaitS
	}
	if a.ElapsedS != nil {
		elapsed = *a.ElapsedS
	}
	ev := obs.Info(&s.log, obs.EventJobDone)
	msg := "job done"
	if j.State == api.JobFailed {
		ev, msg = obs.Warn(&s.log, obs.EventJobFailed).Str(obs.FieldError, j.Error), "job failed"
	}
	ev.Str(obs.FieldJobID, j.ID).Str(obs.FieldQueue, j.QueueName).Float64(obs.FieldWaitSeconds, wait).
		Float64(obs.FieldElapsedSeconds, elapsed).Float64(obs.FieldComputeSeconds, a.ComputeS).Msg(msg)
	s.metrics.JobFinished(j.QueueName, j.State, secs(wait), secs(elapsed))
}

func secs(f float64) time.Duration { return time.Duration(f * float64(time.Second)) }

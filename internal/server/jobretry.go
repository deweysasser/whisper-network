package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/auth"
	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/store"
)

// RetryJob retries a failed job (store.Tx.RetryJob), audited as job.retry
// with the previous error, and wakes waiting claims. Ops does not
// authorize; the caller checks auth.ActRetryJob.
func (o *Ops) RetryJob(ctx context.Context, a Actor, jobID string) (*store.Retried, error) {
	var r *store.Retried
	_, err := o.commit(ctx, a, func(tx *store.Tx, rec recorder) error {
		var err error
		if r, err = tx.RetryJob(ctx, jobID); err != nil {
			return err
		}
		return rec("job.retry", "job:"+jobID, map[string]any{"previous_error": r.PreviousError,
			"state": r.State, "tasks": r.Tasks, "retries": r.Retries})
	})
	if err != nil {
		return nil, err
	}
	o.notify()
	return r, nil
}

// retryJob retries a job as actor under the job lock, so the reaper cannot
// purge the job's media between the retry's check and its commit (the
// reaper re-reads the job under the same lock and leaves a live job's
// media alone). A refusal is a 409 not-retryable problem.
func (s *Server) retryJob(ctx context.Context, actor Actor, jobID string) error {
	unlock := s.locks.Lock(jobID)
	defer unlock()
	r, err := s.ops.RetryJob(ctx, actor, jobID)
	if why, ok := store.IsNotRetryable(err); ok {
		return newProblem(http.StatusConflict, api.ProblemNotRetryable, why)
	}
	if err != nil {
		return err
	}
	if r.Refetch {
		// The media is gone and will be fetched again: drop whatever is
		// left of the old run, after the store commit (a crash in between
		// leaves files the sweep or the next accept overwrites). Under the
		// job lock, so no accept or assembly writes into the directory.
		if err := s.blobs.RemoveJob(jobID); err != nil {
			s.log.Warn().Err(err).Str(obs.FieldJobID, jobID).Msg("removing stale job blobs failed")
		}
	}
	obs.Info(&s.log, obs.EventJobRetried).Str(obs.FieldJobID, jobID).Str(obs.FieldActor, actor.Name).
		Str("state", string(r.State)).Int("tasks", r.Tasks).Int("retries", r.Retries).
		Bool(obs.FieldRefetch, r.Refetch).
		Str("previous_error", r.PreviousError).Msg("job retried")
	return nil
}

// RetryJob retries a job exactly as POST /job/{id}/retry does. The caller
// must have authorized p for auth.ActRetryJob on the job.
func (s *Server) RetryJob(ctx context.Context, p *Principal, jobID string) error {
	return s.retryJob(ctx, p.Actor(), jobID)
}

func (s *Server) handleRetryJob(w http.ResponseWriter, r *http.Request, p *Principal) error {
	j, err := s.visibleJob(r, p, auth.ActRetryJob)
	if err != nil {
		return err
	}
	if err := s.retryJob(r.Context(), p.Actor(), j.ID); err != nil {
		return err
	}
	if j, err = s.st.Job(r.Context(), j.ID); err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, s.JobDoc(r.Context(), p.Principal, j))
	return nil
}

// JobDocument is the package-level JobDocument plus what depends on the
// server's configuration: when a failed job's retained media may be purged.
func (s *Server) JobDocument(p auth.Principal, j *store.Job) api.Job {
	doc := JobDocument(p, j, s.now())
	if j.State == api.JobFailed && j.CompletedAt != nil && !doc.Media.AudioPurged {
		t := j.CompletedAt.Add(s.cfg.FailedRetention)
		doc.Media.KeptUntil = &t
	}
	return doc
}

// --- bulk retry and delete (#68) ---

func (s *Server) handleBulkJobs(w http.ResponseWriter, r *http.Request, p *Principal) error {
	var req api.BulkRequest
	if err := decodeJSON(w, r, &req, false); err != nil {
		return err
	}
	res, err := s.BulkJobs(r.Context(), p, r.PathValue("q"), req)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, res)
	return nil
}

// BulkJobs retries or deletes many jobs of queue (POST /queue/{q}/bulk).
// Each job is authorized as its single-job action would be (a bulk request
// never widens what the caller may do), runs in its own transaction and
// gets its own audit entry; one job.bulk_retry or job.bulk_delete entry on
// the queue then summarizes the request. A dry run changes nothing and
// writes no audit entry.
func (s *Server) BulkJobs(ctx context.Context, p *Principal, queue string, req api.BulkRequest) (*api.BulkResult, error) {
	q, err := s.st.QueueByName(ctx, queue)
	if err != nil {
		return nil, err
	}
	vis := auth.VisibilityOn(p.Principal, q.Name)
	if vis == auth.SeesNone {
		obs.Warn(&s.log, obs.EventAuthDenied).Str(obs.FieldPrincipal, p.Name).
			Str(obs.FieldReason, "bulk: no access to queue "+q.Name).Msg("request denied")
		return nil, forbidden("no access to queue " + q.Name)
	}
	act := auth.ActRetryJob
	switch req.Action {
	case api.BulkRetry:
	case api.BulkDelete:
		act = auth.ActDeleteJob
	default:
		return nil, badRequest(`action must be "retry" or "delete"`)
	}
	res := &api.BulkResult{Action: req.Action, DryRun: req.DryRun, Jobs: []api.BulkJobResult{},
		Counts: map[string]int{}}
	var jobs []*store.Job
	switch {
	case len(req.JobIDs) > 0 && req.Filter != nil:
		return nil, badRequest("give job_ids or filter, not both")
	case len(req.JobIDs) > s.bulkLimit:
		return nil, badRequest(fmt.Sprintf("at most %d job_ids per request", s.bulkLimit))
	case len(req.JobIDs) > 0:
		seen := map[string]bool{}
		for _, id := range req.JobIDs {
			if seen[id] {
				continue
			}
			seen[id] = true
			j, err := s.st.Job(ctx, id)
			switch {
			case errors.Is(err, store.ErrNotFound):
				j = nil
			case err != nil:
				return nil, err
			}
			if j == nil || j.QueueID != q.ID ||
				!auth.Can(p.Principal, auth.ActViewJob, auth.Resource{Queue: j.QueueName, OwnerID: j.ClientID}) {
				addOutcome(res, id, api.OutcomeNotFound, "")
				continue
			}
			jobs = append(jobs, j)
		}
	case req.Filter != nil:
		f := store.JobFilter{QueueID: q.ID, Limit: s.bulkLimit, Cursor: req.Filter.Cursor}
		if vis == auth.SeesOwn {
			f.ClientID = p.ID
		}
		if !validState(req.Filter.State) {
			return nil, badRequest("filter.state must be a job state")
		}
		f.States = []api.JobState{req.Filter.State}
		if req.Filter.Since != nil {
			f.Since = *req.Filter.Since
		}
		list, next, err := s.st.ListJobs(ctx, f)
		if err != nil {
			return nil, err
		}
		res.NextCursor = next
		for i := range list {
			jobs = append(jobs, &list[i])
		}
	default:
		return nil, badRequest("give job_ids or filter")
	}

	for _, j := range jobs {
		if !auth.Can(p.Principal, act, auth.Resource{Queue: j.QueueName, OwnerID: j.ClientID}) {
			obs.Warn(&s.log, obs.EventAuthDenied).Str(obs.FieldPrincipal, p.Name).Str(obs.FieldJobID, j.ID).
				Str(obs.FieldReason, "bulk "+req.Action+": not allowed on this job").Msg("request denied")
			addOutcome(res, j.ID, api.OutcomeForbidden, "")
			continue
		}
		s.bulkOne(ctx, p, req, j, res)
	}
	if !req.DryRun && len(res.Jobs) > 0 {
		summary := map[string]any{"counts": res.Counts, "jobs": len(res.Jobs)}
		if req.Filter != nil {
			summary["filter"] = req.Filter
		}
		if _, err := s.ops.commit(ctx, p.Actor(), func(_ *store.Tx, rec recorder) error {
			return rec("job.bulk_"+req.Action, auth.QueueScope(q.Name), summary)
		}); err != nil {
			return nil, err
		}
	}
	return res, nil
}

// bulkOne applies a bulk action to one authorized job and records the
// outcome.
func (s *Server) bulkOne(ctx context.Context, p *Principal, req api.BulkRequest, j *store.Job, res *api.BulkResult) {
	switch {
	case req.Action == api.BulkDelete && req.DryRun:
		addOutcome(res, j.ID, api.OutcomeWouldDelete, "")
	case req.Action == api.BulkDelete:
		err := s.deleteJob(ctx, p.Actor(), j.ID)
		switch {
		case errors.Is(err, store.ErrNotFound):
			addOutcome(res, j.ID, api.OutcomeNotFound, "")
		case err != nil:
			s.log.Warn().Err(err).Str(obs.FieldJobID, j.ID).Msg("bulk delete failed")
			addOutcome(res, j.ID, api.OutcomeSkipped, "internal error")
		default:
			addOutcome(res, j.ID, api.OutcomeDeleted, "")
		}
	case req.DryRun:
		if !j.Retryable() {
			addOutcome(res, j.ID, api.OutcomeSkipped, retrySkipReason(j))
			return
		}
		addOutcome(res, j.ID, api.OutcomeWouldRetry, "")
	default:
		err := s.retryJob(ctx, p.Actor(), j.ID)
		var pr *problem
		switch {
		case err == nil:
			addOutcome(res, j.ID, api.OutcomeRetried, "")
		case errors.Is(err, store.ErrNotFound):
			addOutcome(res, j.ID, api.OutcomeNotFound, "")
		case errors.As(err, &pr) && pr.typ == api.ProblemNotRetryable:
			addOutcome(res, j.ID, api.OutcomeSkipped, shortRetrySkip(pr.detail))
		default:
			s.log.Warn().Err(err).Str(obs.FieldJobID, j.ID).Msg("bulk retry failed")
			addOutcome(res, j.ID, api.OutcomeSkipped, "internal error")
		}
	}
}

// retrySkipReason is the short reason a job would be skipped by a retry.
func retrySkipReason(j *store.Job) string {
	if j.State != api.JobFailed {
		return "not failed (" + string(j.State) + ")"
	}
	return "media purged"
}

// shortRetrySkip shortens a not-retryable detail for bulk results.
func shortRetrySkip(detail string) string {
	if len(detail) > 7 && detail[:7] == "job is " {
		return "not failed"
	}
	return "media purged"
}

func validState(st api.JobState) bool {
	switch st {
	case api.JobPreparing, api.JobQueued, api.JobRunning, api.JobDone, api.JobFailed:
		return true
	}
	return false
}

func addOutcome(res *api.BulkResult, jobID, outcome, reason string) {
	res.Jobs = append(res.Jobs, api.BulkJobResult{JobID: jobID, Outcome: outcome, Reason: reason})
	res.Counts[outcome]++
}

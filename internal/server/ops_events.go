package server

import (
	"context"
	"errors"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/store"
)

// Audited operations that are not administrative changes: self-service
// account changes, sign-in events, job lifecycle, worker self-registration
// and the server's own housekeeping. Like the rest of Ops, each commits its
// audit entry in the same transaction as the change.

// --- accounts and sessions ---

// ChangePassword is a user's self-service password change: it sets the new
// password (clearing must_change_password) and ends all of the user's
// sessions, audited as user.change_password. The caller has verified the
// current password and starts a fresh session.
//
// verifiedHash must be the hash the current password was verified against:
// the transaction confirms the user still has it and is not disabled,
// failing with store.ErrCredentialsChanged otherwise, so an administrator's
// reset or disable that lands between the check and the change wins rather
// than being overwritten.
func (o *Ops) ChangePassword(ctx context.Context, u *store.User, verifiedHash, password string) error {
	hash, err := o.hashPassword(ctx, password)
	if err != nil {
		return err
	}
	_, err = o.commit(ctx, userActor(u), func(tx *store.Tx, rec recorder) error {
		if err := tx.ConfirmCredentials(ctx, u.ID, verifiedHash); err != nil {
			return err
		}
		if err := tx.SetPassword(ctx, u.ID, hash, false); err != nil {
			return err
		}
		n, err := tx.DeleteUserSessions(ctx, u.ID)
		if err != nil {
			return err
		}
		return rec("user.change_password", "user:"+u.Username, map[string]any{"sessions_ended": n})
	})
	if err != nil {
		return err
	}
	// The caller usually starts a fresh session for u next, which confirms
	// the hash it holds (CreateSession).
	u.PasswordHash, u.MustChangePassword = hash, false
	return nil
}

// CreateSession stores a new login session for u, audited as auth.login.
// replaces, if not empty, is the id of a session the request already
// carried; it is deleted in the same transaction (rotation on login).
// remoteIP is recorded in the audit detail.
//
// u.PasswordHash must be the hash the password was verified against: the
// transaction confirms the user still has it and is not disabled, failing
// with store.ErrCredentialsChanged otherwise, so a password reset or a
// disable that lands between verification and this call wins.
func (o *Ops) CreateSession(ctx context.Context, u *store.User, sess store.Session, replaces, remoteIP string) error {
	_, err := o.commit(ctx, userActor(u), func(tx *store.Tx, rec recorder) error {
		if err := tx.ConfirmCredentials(ctx, u.ID, u.PasswordHash); err != nil {
			return err
		}
		if replaces != "" {
			if _, err := tx.DeleteSession(ctx, replaces); err != nil {
				return err
			}
		}
		if err := tx.CreateSession(ctx, sess); err != nil {
			return err
		}
		return rec("auth.login", "user:"+u.Username, map[string]any{"ip": remoteIP})
	})
	return err
}

// LoginFailed records a failed sign-in as auth.login_failed. The target
// names the user only when the username exists, so mistyped secrets in the
// username field are never stored; the attempted name is otherwise
// dropped. reason is a short code such as "bad_credentials".
func (o *Ops) LoginFailed(ctx context.Context, username, remoteIP, reason string) error {
	target := ""
	if u, err := o.Store.UserByUsername(ctx, username); err == nil {
		target = "user:" + u.Username
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	_, err := o.commit(ctx, Actor{Name: "anonymous"}, func(_ *store.Tx, rec recorder) error {
		return rec("auth.login_failed", target, map[string]any{"ip": remoteIP, "reason": reason,
			"known_user": target != ""})
	})
	return err
}

// Logout deletes the session sessionID (the stored id, not the cookie) and
// records auth.logout for its user. A session that no longer exists is not
// an error and is not audited.
func (o *Ops) Logout(ctx context.Context, sessionID string) error {
	sess, err := o.Store.Session(ctx, sessionID)
	if errors.Is(err, store.ErrNotFound) {
		return o.Store.DeleteSession(ctx, sessionID) // expired or disabled: just clean up
	}
	if err != nil {
		return err
	}
	u, err := o.Store.UserByID(ctx, sess.UserID)
	if err != nil {
		return err
	}
	_, err = o.commit(ctx, userActor(u), func(tx *store.Tx, rec recorder) error {
		found, err := tx.DeleteSession(ctx, sessionID)
		if err != nil || !found {
			return err
		}
		return rec("auth.logout", "user:"+u.Username, nil)
	})
	return err
}

// DeleteExpiredSessions deletes expired sessions and, when there were any,
// records one session.expire entry by the system with the count.
func (o *Ops) DeleteExpiredSessions(ctx context.Context) (int64, error) {
	var n int64
	_, err := o.commit(ctx, SystemActor(), func(tx *store.Tx, rec recorder) error {
		var err error
		if n, err = tx.DeleteExpiredSessions(ctx); err != nil || n == 0 {
			return err
		}
		return rec("session.expire", "", map[string]any{"count": n})
	})
	return n, err
}

// --- jobs ---

// CreateJob creates a job for a (its owner) on the named queue, audited as
// job.create. A replayed idempotency key returns the existing job with
// created = false and is not audited. The detail records the queue, model,
// weight and upload size, never the prompt or metadata.
func (o *Ops) CreateJob(ctx context.Context, a Actor, queue string, nj store.NewJob) (job *store.Job, created bool, err error) {
	_, err = o.commit(ctx, a, func(tx *store.Tx, rec recorder) error {
		var err error
		if job, created, err = tx.CreateJob(ctx, nj); err != nil || !created {
			return err
		}
		return rec("job.create", "job:"+job.ID, map[string]any{"queue": queue, "model": job.Model,
			"weight": job.Weight, "bytes": nj.Original.Bytes})
	})
	if err != nil {
		return nil, false, err
	}
	return job, created, nil
}

// DeleteJob deletes a job's row (ending its live leases as cancelled, whose
// ids it returns), audited as job.delete. Blob removal is the caller's job.
// Retention deletions use SystemActor.
func (o *Ops) DeleteJob(ctx context.Context, a Actor, jobID string) ([]string, error) {
	var cancelled []string
	_, err := o.commit(ctx, a, func(tx *store.Tx, rec recorder) error {
		var err error
		if cancelled, err = tx.DeleteJob(ctx, jobID); err != nil {
			return err
		}
		return rec("job.delete", "job:"+jobID, map[string]any{"cancelled_leases": len(cancelled)})
	})
	return cancelled, err
}

// Media kinds for MediaPurged.
const (
	MediaOriginal  = "original"
	MediaCanonical = "canonical"
)

// MediaPurged marks a job's original upload or canonical audio (what is
// MediaOriginal or MediaCanonical) as deleted, audited as job.purge_media
// by the system. Call it after removing the blobs.
func (o *Ops) MediaPurged(ctx context.Context, jobID, what string) error {
	_, err := o.commit(ctx, SystemActor(), func(tx *store.Tx, rec recorder) error {
		var err error
		switch what {
		case MediaOriginal:
			err = tx.MarkOriginalPurged(ctx, jobID)
		case MediaCanonical:
			err = tx.MarkCanonicalPurged(ctx, jobID)
		default:
			return badRequest("unknown media kind " + what)
		}
		if err != nil {
			return err
		}
		return rec("job.purge_media", "job:"+jobID, map[string]any{"what": what})
	})
	return err
}

// --- workers (self-service and housekeeping) ---

// RegisterWorker creates a pending worker, audited as worker.register with
// the worker itself as actor. tokenHash is the hash of the worker's new
// token; the token itself is never seen here. When maxPending is positive
// and that many workers are already pending it fails with
// store.ErrPendingFull; the count, the insert and the audit entry share one
// transaction, so concurrent registrations cannot overshoot the cap.
func (o *Ops) RegisterWorker(ctx context.Context, tokenHash string, facts api.WorkerFacts, lim store.PendingLimits) (*store.Worker, error) {
	var w *store.Worker
	err := o.Store.InTx(ctx, func(tx *store.Tx) error {
		var err error
		if w, err = tx.RegisterWorkerCapped(ctx, tokenHash, facts, lim); err != nil {
			return err
		}
		d, err := auditDetail(workerFactsDetail(facts))
		if err != nil {
			return err
		}
		_, err = tx.AppendAudit(ctx, store.AuditEntry{Actor: w.Principal(), Action: "worker.register",
			Target: w.Principal(), Detail: d})
		return err
	})
	return w, err
}

// UpdateWorkerFacts replaces a worker's reported facts, audited as
// worker.update_facts (actor a is normally the worker itself). The detail
// carries the new facts and, for each of version, engine_version and
// ffmpeg_version that changed, its previous value under "old_<name>", so a
// worker restarted on another build is traceable.
func (o *Ops) UpdateWorkerFacts(ctx context.Context, a Actor, workerID string, facts api.WorkerFacts) error {
	var orphaned store.OrphanFailures
	_, err := o.commit(ctx, a, func(tx *store.Tx, rec recorder) (err error) {
		old, err := tx.WorkerByID(ctx, workerID)
		if err != nil {
			return err
		}
		if orphaned, err = tx.UpdateWorkerFacts(ctx, workerID, facts); err != nil {
			return err
		}
		d := workerFactsDetail(facts)
		if len(orphaned.Jobs) > 0 {
			d["failed_jobs"] = orphaned.Jobs
		}
		for name, v := range map[string][2]string{
			"version":        {old.Facts.Version, facts.Version},
			"engine_version": {old.Facts.EngineVersion, facts.EngineVersion},
			"ffmpeg_version": {old.Facts.FFmpegVersion, facts.FFmpegVersion},
		} {
			if v[0] != v[1] {
				d["old_"+name] = v[0]
			}
		}
		return rec("worker.update_facts", store.WorkerPrincipal(workerID), d)
	})
	if err == nil {
		o.jobsFinished(ctx, orphaned.Jobs)
	}
	return err
}

func workerFactsDetail(f api.WorkerFacts) map[string]any {
	return map[string]any{"hostname": f.Hostname, "sandbox_level": f.SandboxLevel, "version": f.Version,
		"engine_version": f.EngineVersion, "ffmpeg_version": f.FFmpegVersion, "slots": f.Slots}
}

// PrunePendingWorkers deletes pending workers not seen since cutoff, with
// their grants, recording one worker.prune entry per worker by the system.
// It returns how many were deleted.
func (o *Ops) PrunePendingWorkers(ctx context.Context, cutoff time.Time) (int64, error) {
	var ids []string
	_, err := o.commit(ctx, SystemActor(), func(tx *store.Tx, rec recorder) error {
		var err error
		if ids, err = tx.DeletePendingWorkersUnseen(ctx, cutoff); err != nil {
			return err
		}
		for _, id := range ids {
			if err := rec("worker.prune", store.WorkerPrincipal(id), map[string]any{"state": api.WorkerPending}); err != nil {
				return err
			}
		}
		return nil
	})
	return int64(len(ids)), err
}

// PruneRejectedWorkers deletes workers rejected before cutoff, with their
// grants, the grants scoped to them and their work counters, recording one
// worker.prune entry per worker by the system. The entry keeps the final
// counters of a worker that did any work, since they are gone afterwards.
// It returns how many were deleted.
func (o *Ops) PruneRejectedWorkers(ctx context.Context, cutoff time.Time) (int64, error) {
	var ws []store.PrunedWorker
	_, err := o.commit(ctx, SystemActor(), func(tx *store.Tx, rec recorder) error {
		var err error
		if ws, err = tx.DeleteRejectedWorkers(ctx, cutoff); err != nil {
			return err
		}
		for i := range ws {
			if err := rec("worker.prune", store.WorkerPrincipal(ws[i].ID), prunedDetail(&ws[i])); err != nil {
				return err
			}
		}
		return nil
	})
	return int64(len(ws)), err
}

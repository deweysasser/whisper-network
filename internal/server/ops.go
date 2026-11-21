package server

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/rs/zerolog"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/auth"
	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/store"
)

// Ops performs audited changes. Each one applies its store writes and
// appends its audit entries in one write transaction, so a change and its
// audit record commit together or not at all: if the audit append fails,
// the change fails. Administrative changes are then logged as admin.change
// (plus the worker.* lifecycle events).
//
// Ops does not authorize: the HTTP and UI layers check auth.Can /
// auth.CanGrantTo / auth.CanDelegateAtCreation / auth.CanApproveWorker
// first, and the offline admin command runs with the operator's filesystem
// access to the database.
//
// Ops speaks the wire forms: queue scopes by name ("queue:public") and
// principals as "user:<username>", "client:<id>" or "worker:<id>". It
// translates to the store's id-based forms. Audit details never contain
// secrets or requester content (passwords, tokens, prompts, metadata).
type Ops struct {
	Store          *store.Store
	Log            zerolog.Logger
	PasswordParams auth.PasswordParams
	// Notify, if set, wakes waiting claims after changes that can make work
	// claimable.
	Notify func()
	// JobFinished, if set, is called after the commit for each job a change
	// failed (the server logs job.failed and records its metrics).
	JobFinished func(ctx context.Context, jobID string)
	// PasswordSlot, if set, bounds concurrent password hashing (the server
	// sets it to Server.PasswordSlot). Every hash Ops computes holds a slot
	// for exactly the duration of the hash.
	PasswordSlot func(ctx context.Context) (release func(), err error)
	// MaxUploadBytes, if positive, is the server's upload cap
	// (--max-upload-bytes). A queue's own cap may not exceed it: uploads are
	// clamped to it regardless, so a higher queue cap would only mislead.
	MaxUploadBytes int64
}

// checkQueueCap rejects a queue upload cap above the server's.
func (o *Ops) checkQueueCap(n int64) error {
	if o.MaxUploadBytes > 0 && n > o.MaxUploadBytes {
		return badRequest(fmt.Sprintf("max_upload_bytes %d exceeds the server's upload cap of %d bytes "+
			"(set by the server's --max-upload-bytes flag)", n, o.MaxUploadBytes))
	}
	return nil
}

// hashPassword hashes password under the PasswordSlot gate. A busy gate is
// returned as is (a 503 problem); a password the hasher refuses is a 400.
func (o *Ops) hashPassword(ctx context.Context, password string) (string, error) {
	if o.PasswordSlot != nil {
		release, err := o.PasswordSlot(ctx)
		if err != nil {
			return "", err
		}
		defer release()
	}
	h, err := auth.HashPassword(password, o.PasswordParams)
	if err != nil {
		return "", badRequest(err.Error())
	}
	return h, nil
}

// Actor identifies who makes a change: Name is the readable form recorded
// in the audit log, Grant the stored principal recorded as granted_by.
type Actor struct {
	Name, Grant string
}

// Actor returns the principal as an Actor.
func (p *Principal) Actor() Actor { return Actor{Name: p.Name, Grant: p.Grant} }

// CLIActor is the actor for changes made with the offline admin command.
func CLIActor() Actor { return Actor{Name: "admin-cli", Grant: store.SystemActor} }

// SystemActor is the actor for automatic changes the server makes itself
// (retention, pruning, session expiry).
func SystemActor() Actor { return Actor{Name: store.SystemActor, Grant: store.SystemActor} }

// userActor is the actor for a user acting on their own account.
func userActor(u *store.User) Actor { return Actor{Name: "user:" + u.Username, Grant: u.Principal()} }

// jobsFinished reports jobs a committed change failed.
func (o *Ops) jobsFinished(ctx context.Context, ids []string) {
	if o.JobFinished == nil {
		return
	}
	for _, id := range ids {
		o.JobFinished(ctx, id)
	}
}

func (o *Ops) notify() {
	if o.Notify != nil {
		o.Notify()
	}
}

// recorder appends one audit entry inside the current transaction.
type recorder func(action, target string, detail any) error

// auditRecord is an entry written by a recorder, kept for logging after
// the commit.
type auditRecord struct{ action, target string }

// commit runs fn in one write transaction. Every rec call appends an audit
// entry by a in the same transaction, so the change and its audit entries
// commit together; any error rolls everything back.
func (o *Ops) commit(ctx context.Context, a Actor, fn func(tx *store.Tx, rec recorder) error) ([]auditRecord, error) {
	var recs []auditRecord
	err := o.Store.InTx(ctx, func(tx *store.Tx) error {
		recs = recs[:0]
		return fn(tx, func(action, target string, detail any) error {
			d, err := auditDetail(detail)
			if err != nil {
				return err
			}
			if _, err := tx.AppendAudit(ctx, store.AuditEntry{Actor: a.Name, Action: action, Target: target,
				Detail: d}); err != nil {
				return err
			}
			recs = append(recs, auditRecord{action, target})
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return recs, nil
}

// change is commit for administrative changes: after the commit each audit
// entry is also logged as admin.change.
func (o *Ops) change(ctx context.Context, a Actor, fn func(tx *store.Tx, rec recorder) error) error {
	recs, err := o.commit(ctx, a, fn)
	if err != nil {
		return err
	}
	for _, r := range recs {
		obs.Info(&o.Log, obs.EventAdminChange).Str(obs.FieldActor, a.Name).Str(obs.FieldAction, r.action).
			Str(obs.FieldTarget, r.target).Msg("admin change")
	}
	return nil
}

// auditDetail renders an audit detail as JSON (strings are kept as is).
func auditDetail(detail any) (string, error) {
	switch v := detail.(type) {
	case nil:
		return "", nil
	case string:
		return v, nil
	}
	b, err := json.Marshal(detail)
	if err != nil {
		return "", fmt.Errorf("audit detail: %w", err)
	}
	return string(b), nil
}

// --- translation between wire and store forms ---

// WireGrants returns a stored principal's grants with queue scopes by name.
// Grants on queues that no longer exist are dropped.
func (o *Ops) WireGrants(ctx context.Context, principal string) ([]api.Grant, error) {
	gs, err := o.Store.GrantsFor(ctx, principal)
	if err != nil {
		return nil, err
	}
	var names map[int64]string
	out := make([]api.Grant, 0, len(gs))
	for _, g := range gs {
		scope := g.Scope
		if strings.HasPrefix(scope, "queue:") {
			if names == nil {
				if names, err = o.queueNames(ctx); err != nil {
					return nil, err
				}
			}
			id, err := store.ParseQueueScope(scope)
			name, ok := names[id]
			if err != nil || !ok {
				continue
			}
			scope = auth.QueueScope(name)
		}
		out = append(out, api.Grant{Role: g.Role, Scope: scope})
	}
	return out, nil
}

func (o *Ops) queueNames(ctx context.Context) (map[int64]string, error) {
	qs, err := o.Store.ListQueues(ctx)
	if err != nil {
		return nil, err
	}
	m := make(map[int64]string, len(qs))
	for _, q := range qs {
		m[q.ID] = q.Name
	}
	return m, nil
}

// StoreScope translates a wire scope to the stored form.
func (o *Ops) StoreScope(ctx context.Context, scope string) (string, error) {
	switch {
	case scope == store.ScopeAll:
		return scope, nil
	case strings.HasPrefix(scope, "queue:"):
		q, err := o.Store.QueueByName(ctx, strings.TrimPrefix(scope, "queue:"))
		if errors.Is(err, store.ErrNotFound) {
			return "", badRequest("unknown queue in scope " + scope)
		}
		if err != nil {
			return "", err
		}
		return store.QueueScope(q.ID), nil
	case strings.HasPrefix(scope, "worker:") && len(scope) > len("worker:"):
		return scope, nil
	}
	return "", badRequest("invalid scope " + strconv.Quote(scope))
}

// StorePrincipal translates a wire principal to the stored form, checking
// that it exists. It also returns the principal's kind.
func (o *Ops) StorePrincipal(ctx context.Context, principal string) (string, auth.PrincipalKind, error) {
	kind, id, ok := strings.Cut(principal, ":")
	if !ok || id == "" {
		return "", "", badRequest("principal must be user:<name>, client:<id> or worker:<id>")
	}
	var err error
	switch auth.PrincipalKind(kind) {
	case auth.KindUser:
		var u *store.User
		if u, err = o.Store.UserByUsername(ctx, id); err == nil {
			return u.Principal(), auth.KindUser, nil
		}
	case auth.KindClient:
		if _, err = o.Store.ClientByID(ctx, id); err == nil {
			return store.ClientPrincipal(id), auth.KindClient, nil
		}
	case auth.KindWorker:
		if _, err = o.Store.WorkerByID(ctx, id); err == nil {
			return store.WorkerPrincipal(id), auth.KindWorker, nil
		}
	default:
		return "", "", badRequest("unknown principal kind " + strconv.Quote(kind))
	}
	if errors.Is(err, store.ErrNotFound) {
		return "", "", newProblem(404, api.ProblemNotFound, "no such principal "+principal)
	}
	return "", "", err
}

// checkGrantFits rejects grants that are well formed but meaningless for
// the principal kind: process and fetch are the only roles a worker can
// use, and only workers can use them. A processing worker takes process, a
// downloader (wk, ignored for non-workers) takes fetch.
func checkGrantFits(kind auth.PrincipalKind, wk api.WorkerKind, g api.Grant) error {
	if err := auth.ValidateGrant(g); err != nil {
		return badRequest(err.Error())
	}
	fits := g.Role != api.RoleProcess && g.Role != api.RoleFetch
	if kind == auth.KindWorker {
		fits = g.Role == workerRole(wk)
	}
	if !fits {
		if kind == auth.KindWorker {
			return badRequest(fmt.Sprintf("role %s cannot be granted to a %s worker", g.Role, cmp.Or(wk, api.WorkerProcessing)))
		}
		return badRequest(fmt.Sprintf("role %s cannot be granted to a %s", g.Role, kind))
	}
	return nil
}

// workerRole is the role a worker of kind holds on the queues it serves.
func workerRole(k api.WorkerKind) api.Role {
	if k == api.WorkerDownloader {
		return api.RoleFetch
	}
	return api.RoleProcess
}

// storeGrant is a grant resolved to its stored scope, with its wire form
// kept for the audit detail.
type storeGrant struct {
	wire  api.Grant
	scope string
}

// resolveGrants checks that each grant fits kind (and wk, for a worker) and
// translates its scope.
func (o *Ops) resolveGrants(ctx context.Context, kind auth.PrincipalKind, wk api.WorkerKind, gs []api.Grant) ([]storeGrant, error) {
	out := make([]storeGrant, len(gs))
	for i, g := range gs {
		if err := checkGrantFits(kind, wk, g); err != nil {
			return nil, err
		}
		scope, err := o.StoreScope(ctx, g.Scope)
		if err != nil {
			return nil, err
		}
		out[i] = storeGrant{wire: g, scope: scope}
	}
	return out, nil
}

// --- grants ---

// Grant gives principal (wire form) role on scope.
func (o *Ops) Grant(ctx context.Context, a Actor, principal string, g api.Grant) error {
	return o.UpdateGrants(ctx, a, principal, []api.Grant{g}, nil)
}

// Revoke removes a grant. ErrNotFound if the principal does not hold it.
func (o *Ops) Revoke(ctx context.Context, a Actor, principal string, g api.Grant) error {
	return o.UpdateGrants(ctx, a, principal, nil, []api.Grant{g})
}

// UpdateGrants adds and removes grants of principal (wire form) in one
// transaction, auditing each as grant.add or grant.remove: either every
// change applies or none does. Grants to add must fit the principal's kind;
// removing a grant the principal does not hold is ErrNotFound.
func (o *Ops) UpdateGrants(ctx context.Context, a Actor, principal string, add, remove []api.Grant) error {
	sp, kind, err := o.StorePrincipal(ctx, principal)
	if err != nil {
		return err
	}
	var wk api.WorkerKind
	if id, ok := strings.CutPrefix(sp, "worker:"); ok && kind == auth.KindWorker {
		w, err := o.Store.WorkerByID(ctx, id)
		if err != nil {
			return err
		}
		wk = w.Kind
	}
	adds, err := o.resolveGrants(ctx, kind, wk, add)
	if err != nil {
		return err
	}
	removes := make([]storeGrant, len(remove))
	for i, g := range remove {
		scope, err := o.StoreScope(ctx, g.Scope)
		if err != nil {
			return err
		}
		removes[i] = storeGrant{wire: g, scope: scope}
	}
	type revoked struct {
		role      api.Role
		cancelled int
	}
	var cancelled []revoked
	var anyCancelled bool
	var failedJobs []string
	err = o.change(ctx, a, func(tx *store.Tx, rec recorder) error {
		cancelled, anyCancelled, failedJobs = cancelled[:0], false, failedJobs[:0]
		for _, g := range adds {
			if err := tx.AddGrant(ctx, sp, g.wire.Role, g.scope, a.Grant); err != nil {
				return err
			}
			if err := rec("grant.add", principal, g.wire); err != nil {
				return err
			}
		}
		for _, g := range removes {
			r, err := tx.RevokeGrant(ctx, sp, g.wire.Role, g.scope)
			if err != nil {
				return err
			}
			if len(r.Cancelled) > 0 {
				cancelled = append(cancelled, revoked{g.wire.Role, len(r.Cancelled)})
			}
			anyCancelled = anyCancelled || len(r.Cancelled) > 0 || len(r.Orphaned.Leases) > 0
			failedJobs = append(failedJobs, r.Orphaned.Jobs...)
			detail := any(g.wire)
			if len(r.Cancelled) > 0 || len(r.Orphaned.Jobs) > 0 {
				d := map[string]any{"role": g.wire.Role, "scope": g.wire.Scope}
				if len(r.Cancelled) > 0 {
					d["cancelled_leases"] = len(r.Cancelled)
				}
				if len(r.Orphaned.Jobs) > 0 {
					d["failed_jobs"] = r.Orphaned.Jobs
				}
				detail = d
			}
			if err := rec("grant.remove", principal, detail); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, c := range cancelled {
		o.Log.Info().Str(obs.FieldPrincipal, principal).Int("cancelled_leases", c.cancelled).
			Msgf("leases cancelled: %s grant revoked", c.role)
	}
	o.jobsFinished(ctx, failedJobs)
	if anyCancelled {
		o.notify() // their tasks are pending again
		return nil
	}
	for _, g := range add {
		if g.Role == api.RoleProcess || g.Role == api.RoleFetch {
			o.notify()
			break
		}
	}
	return nil
}

// --- workers ---

// ApproveWorker makes a pending (or already active) worker active and grants
// it process on each queue, atomically. All queues must exist; otherwise
// nothing changes.
func (o *Ops) ApproveWorker(ctx context.Context, a Actor, workerID string, queues []string) error {
	w, err := o.Store.WorkerByID(ctx, workerID)
	if err != nil {
		return err
	}
	scopes := make([]string, len(queues))
	for i, q := range queues {
		if scopes[i], err = o.StoreScope(ctx, auth.QueueScope(q)); err != nil {
			return err
		}
	}
	role := workerRole(w.Kind)
	err = o.change(ctx, a, func(tx *store.Tx, rec recorder) error {
		if _, err := tx.SetWorkerState(ctx, workerID, api.WorkerActive, a.Name); err != nil {
			return err
		}
		for _, sc := range scopes {
			if err := tx.AddGrant(ctx, w.Principal(), role, sc, a.Grant); err != nil {
				return err
			}
		}
		return rec("worker.approve", w.Principal(), map[string]any{"queues": nonNilStrings(queues), "role": role})
	})
	if err != nil {
		return err
	}
	obs.Info(&o.Log, obs.EventWorkerApproved).Str(obs.FieldWorkerID, w.ID).Str(obs.FieldHostname, w.Facts.Hostname).
		Str(obs.FieldActor, a.Name).Strs(obs.FieldQueues, queues).Msg("worker approved")
	o.notify()
	return nil
}

func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// SetWorkerState pauses, resumes or rejects a worker. Rejection cancels the
// worker's live leases (their tasks requeue without using an attempt).
func (o *Ops) SetWorkerState(ctx context.Context, a Actor, workerID string, state api.WorkerState) error {
	w, err := o.Store.WorkerByID(ctx, workerID)
	if err != nil {
		return err
	}
	var ev obs.Event
	var action string
	switch state {
	case api.WorkerPaused:
		ev, action = obs.EventWorkerPaused, "worker.pause"
	case api.WorkerActive:
		if w.State == api.WorkerPending {
			return conflict("a pending worker is approved, not resumed")
		}
		ev, action = obs.EventWorkerResumed, "worker.resume"
	case api.WorkerRejected:
		ev, action = obs.EventWorkerRejected, "worker.reject"
	default:
		return badRequest("unsupported worker state " + string(state))
	}
	var ch store.WorkerStateChange
	err = o.change(ctx, a, func(tx *store.Tx, rec recorder) error {
		var err error
		if ch, err = tx.ChangeWorkerState(ctx, workerID, state, a.Name); err != nil {
			return err
		}
		detail := map[string]any{"from": w.State, "cancelled_leases": len(ch.Cancelled)}
		if len(ch.Orphaned.Jobs) > 0 {
			detail["failed_jobs"] = ch.Orphaned.Jobs
		}
		return rec(action, w.Principal(), detail)
	})
	if err != nil {
		return err
	}
	obs.Info(&o.Log, ev).Str(obs.FieldWorkerID, w.ID).Str(obs.FieldHostname, w.Facts.Hostname).
		Str(obs.FieldActor, a.Name).Int("cancelled_leases", len(ch.Cancelled)).Msg("worker state changed")
	o.jobsFinished(ctx, ch.Orphaned.Jobs)
	if state != api.WorkerPaused {
		o.notify()
	}
	return nil
}

// DeleteWorker deletes a rejected worker on demand, as the reaper does past
// --rejected-worker-retention: with its grants, the grants scoped to it and
// its counters, audited as worker.delete. Any other state is a conflict, so
// an active or paused worker must be rejected first (cancelling its leases)
// and live leases are never stranded. Afterwards the worker's token is
// unknown (401) rather than rejected (410).
func (o *Ops) DeleteWorker(ctx context.Context, a Actor, workerID string) error {
	return o.change(ctx, a, func(tx *store.Tx, rec recorder) error {
		w, err := tx.WorkerByID(ctx, workerID)
		if err != nil {
			return err
		}
		if w.State != api.WorkerRejected {
			return conflict("only a rejected worker can be deleted; this one is " + string(w.State))
		}
		return deleteRejectedWorker(ctx, tx, rec, w.ID)
	})
}

// DeleteRejectedWorkers deletes each of the given workers that is still
// rejected, in one transaction, each audited as worker.delete; ids that are
// unknown (already deleted) or not rejected are skipped. The caller has
// authorized every id. It returns the ids deleted.
func (o *Ops) DeleteRejectedWorkers(ctx context.Context, a Actor, workerIDs []string) ([]string, error) {
	var deleted []string
	err := o.change(ctx, a, func(tx *store.Tx, rec recorder) error {
		deleted = deleted[:0]
		for _, id := range workerIDs {
			w, err := tx.WorkerByID(ctx, id)
			if errors.Is(err, store.ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if w.State != api.WorkerRejected {
				continue
			}
			if err := deleteRejectedWorker(ctx, tx, rec, w.ID); err != nil {
				return err
			}
			deleted = append(deleted, w.ID)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return deleted, nil
}

// deleteRejectedWorker deletes one rejected worker and records worker.delete.
// Like worker.prune, the entry keeps the final counters of a worker that did
// any work, since they are gone afterwards.
func deleteRejectedWorker(ctx context.Context, tx *store.Tx, rec recorder, id string) error {
	pw, err := tx.DeleteRejectedWorker(ctx, id)
	if err != nil {
		return err
	}
	return rec("worker.delete", store.WorkerPrincipal(pw.ID), prunedDetail(pw))
}

// prunedDetail is the audit detail of a deleted rejected worker: its state,
// and its final counters if it did any work.
func prunedDetail(pw *store.PrunedWorker) map[string]any {
	detail := map[string]any{"state": api.WorkerRejected}
	if pw.Stats.TasksDone()+pw.Stats.Failures+pw.Stats.ExpiredLeases > 0 {
		detail["stats"] = pw.Stats
	}
	return detail
}

// --- clients ---

// CreateClient creates an API client with the given grants and returns it
// with its token (shown once).
func (o *Ops) CreateClient(ctx context.Context, a Actor, name string, grants []api.Grant) (*api.Client, error) {
	if name = strings.TrimSpace(name); name == "" || len(name) > 200 {
		return nil, badRequest("client name must be 1-200 characters")
	}
	resolved, err := o.resolveGrants(ctx, auth.KindClient, "", grants)
	if err != nil {
		return nil, err
	}
	specs := make([]store.GrantSpec, len(resolved))
	for i, g := range resolved {
		specs[i] = store.GrantSpec{Role: g.wire.Role, Scope: g.scope}
	}
	token, err := auth.NewClientToken()
	if err != nil {
		return nil, err
	}
	var c *store.Client
	err = o.change(ctx, a, func(tx *store.Tx, rec recorder) error {
		var err error
		if c, err = tx.CreateClient(ctx, store.NewClient{Name: name, TokenHash: auth.HashToken(token),
			CreatedBy: a.Grant, Grants: specs}); err != nil {
			return err
		}
		return rec("client.create", c.Principal(), map[string]any{"name": name, "grants": nonNilGrants(grants)})
	})
	if err != nil {
		return nil, err
	}
	out, err := o.ClientAPI(ctx, c)
	if err != nil {
		return nil, err
	}
	out.Token = token
	return out, nil
}

func nonNilGrants(gs []api.Grant) []api.Grant {
	if gs == nil {
		return []api.Grant{}
	}
	return gs
}

// RevokeClient revokes a client's token.
func (o *Ops) RevokeClient(ctx context.Context, a Actor, id string) error {
	return o.change(ctx, a, func(tx *store.Tx, rec recorder) error {
		if err := tx.RevokeClient(ctx, id); err != nil {
			return err
		}
		return rec("client.revoke", store.ClientPrincipal(id), nil)
	})
}

// ClientAPI builds the wire form of a client.
func (o *Ops) ClientAPI(ctx context.Context, c *store.Client) (*api.Client, error) {
	gs, err := o.WireGrants(ctx, c.Principal())
	if err != nil {
		return nil, err
	}
	return &api.Client{ID: c.ID, Name: c.Name, Grants: gs, CreatedAt: c.CreatedAt,
		LastUsedAt: c.LastUsedAt, RevokedAt: c.RevokedAt}, nil
}

// --- users ---

// CreateUser creates a UI user with grants, atomically. Without a password
// one is generated, returned once in the result, and must be changed at
// first login.
func (o *Ops) CreateUser(ctx context.Context, a Actor, req api.CreateUserRequest) (*api.User, error) {
	if !store.ValidQueueName(req.Username) { // same conservative grammar as queue names
		return nil, badRequest("username must match [a-z0-9][a-z0-9-]{0,62}")
	}
	grants, err := o.resolveGrants(ctx, auth.KindUser, "", req.Grants)
	if err != nil {
		return nil, err
	}
	password, generated := req.Password, false
	if password == "" {
		if password, err = auth.GeneratePassword(); err != nil {
			return nil, err
		}
		generated = true
	}
	hash, err := o.hashPassword(ctx, password)
	if err != nil {
		return nil, err
	}
	var u *store.User
	target := "user:" + req.Username
	err = o.change(ctx, a, func(tx *store.Tx, rec recorder) error {
		var err error
		if u, err = tx.CreateUser(ctx, req.Username, hash, generated); err != nil {
			return err
		}
		for _, g := range grants {
			if err := tx.AddGrant(ctx, u.Principal(), g.wire.Role, g.scope, a.Grant); err != nil {
				return err
			}
		}
		return rec("user.create", target, map[string]any{"grants": nonNilGrants(req.Grants), "must_change": generated})
	})
	if err != nil {
		return nil, err
	}
	out, err := o.UserAPI(ctx, u)
	if err != nil {
		return nil, err
	}
	if generated {
		out.Password = password
	}
	return out, nil
}

// ResetPassword sets a user's password and ends the user's sessions. An
// empty password generates one, which is returned and must be changed at
// the user's next login.
func (o *Ops) ResetPassword(ctx context.Context, a Actor, username, password string) (string, error) {
	u, err := o.Store.UserByUsername(ctx, username)
	if err != nil {
		return "", err
	}
	generated := password == ""
	if generated {
		if password, err = auth.GeneratePassword(); err != nil {
			return "", err
		}
	}
	hash, err := o.hashPassword(ctx, password)
	if err != nil {
		return "", err
	}
	err = o.change(ctx, a, func(tx *store.Tx, rec recorder) error {
		if err := tx.SetPassword(ctx, u.ID, hash, generated); err != nil {
			return err
		}
		n, err := tx.DeleteUserSessions(ctx, u.ID)
		if err != nil {
			return err
		}
		return rec("user.reset_password", "user:"+u.Username, map[string]any{"must_change": generated, "sessions_ended": n})
	})
	if err != nil {
		return "", err
	}
	return password, nil
}

// DisableUser disables a UI user and ends their sessions. Disabling an
// already-disabled user is not an error.
func (o *Ops) DisableUser(ctx context.Context, a Actor, username string) error {
	u, err := o.Store.UserByUsername(ctx, username)
	if err != nil {
		return err
	}
	return o.change(ctx, a, func(tx *store.Tx, rec recorder) error {
		if err := tx.DisableUser(ctx, u.ID); err != nil {
			return err
		}
		return rec("user.disable", "user:"+u.Username, nil)
	})
}

// UserAPI builds the wire form of a user.
func (o *Ops) UserAPI(ctx context.Context, u *store.User) (*api.User, error) {
	gs, err := o.WireGrants(ctx, u.Principal())
	if err != nil {
		return nil, err
	}
	return &api.User{Username: u.Username, MustChangePassword: u.MustChangePassword,
		Disabled: u.DisabledAt != nil, CreatedAt: u.CreatedAt, Grants: gs}, nil
}

// --- queues ---

// CreateQueue creates a queue.
func (o *Ops) CreateQueue(ctx context.Context, a Actor, q api.Queue) (*store.Queue, error) {
	if err := o.checkQueueCap(q.MaxUploadBytes); err != nil {
		return nil, err
	}
	var created *store.Queue
	err := o.change(ctx, a, func(tx *store.Tx, rec recorder) error {
		var err error
		if created, err = tx.CreateQueue(ctx, store.NewQueue{Name: q.Name, Description: q.Description,
			Weight: q.Weight, MaxUploadBytes: q.MaxUploadBytes}); err != nil {
			return err
		}
		return rec("queue.create", "queue:"+created.Name, created.API())
	})
	return created, err
}

// PatchQueue changes a queue's description, weight or upload cap. The upload
// cap may not exceed the server's (MaxUploadBytes), for any caller.
func (o *Ops) PatchQueue(ctx context.Context, a Actor, q *store.Queue, p api.QueuePatch) (*store.Queue, error) {
	if p.MaxUploadBytes != nil {
		if err := o.checkQueueCap(*p.MaxUploadBytes); err != nil {
			return nil, err
		}
	}
	var out *store.Queue
	err := o.change(ctx, a, func(tx *store.Tx, rec recorder) error {
		var err error
		if out, err = tx.PatchQueue(ctx, q.ID, p); err != nil {
			return err
		}
		return rec("queue.patch", "queue:"+q.Name, p)
	})
	return out, err
}

// RenameQueue renames a queue. Grants follow it (they are stored by id).
func (o *Ops) RenameQueue(ctx context.Context, a Actor, q *store.Queue, name string) error {
	return o.change(ctx, a, func(tx *store.Tx, rec recorder) error {
		if err := tx.RenameQueue(ctx, q.ID, name); err != nil {
			return err
		}
		return rec("queue.rename", "queue:"+name, map[string]any{"from": q.Name})
	})
}

package server

// Admin API. JSON over the same authentication as the client API (bearer
// client token or user session + CSRF header). Every change is audited and
// logged as admin.change; see Ops. Principals on the wire are
// "user:<username>", "client:<id>" or "worker:<id>"; scopes are "*",
// "queue:<name>" or "worker:<id>".
//
//	GET    /admin/workers                  list workers (pending first)          admin, worker_admin, queue_admin
//	GET    /admin/version                  api.ServerVersion (server's own)      admin, worker_admin, queue_admin
//	POST   /admin/workers/{id}/approve     {"queues":[...]} → 204                worker_admin + queue_admin on each queue
//	POST   /admin/workers/{id}/pause       → 204                                 worker_admin
//	POST   /admin/workers/{id}/resume      → 204                                 worker_admin
//	POST   /admin/workers/{id}/reject      → 204 (cancels its leases)            worker_admin
//	DELETE /admin/workers/{id}             → 204; 409 unless rejected            worker_admin
//	GET    /admin/queues                   list queues                           any user or client
//	POST   /admin/queues                   api.Queue → 201                       admin
//	GET    /admin/users                    list users                            admin
//	POST   /admin/users                    api.CreateUserRequest → 201 api.User   admin (+ grants via CanGrantTo)
//	GET    /admin/clients                  list clients                          client_admin
//	GET    /admin/grants?principal=P       P's grants                            admin, or P itself
//	POST   /admin/grants                   api.PrincipalGrant → 204              CanGrantTo
//	DELETE /admin/grants?principal=&role=&scope=  → 204                          CanGrantTo
//	GET    /admin/fetch-policies           []api.FetchPolicy                     admin
//	GET    /admin/fetch-policies/{key}     api.FetchPolicy; 404 if none           admin
//	PUT    /admin/fetch-policies/{key}     api.FetchPolicy → 200 (201 created)   admin; the path key wins
//	DELETE /admin/fetch-policies/{key}     → 204; 409 for "*"                    admin
//	GET    /admin/fetch-cooldowns          []api.FetchCooldown in force          admin
//	POST   /admin/fetch-cooldowns          api.SetCooldownRequest → 201          admin
//	DELETE /admin/fetch-cooldowns/{egress}/{site}  → 204 (egress percent-encoded) admin
//	GET    /admin/audit?before=&limit=&action=&actor=&target=&since=&until=
//	                                       api.AuditList, newest first           admin
//
// Granting and revoking follow auth.CanGrantTo: admin, or queue_admin on X
// for submit/view_all/delete_all/process on queue:X (never read_all, which
// only admin grants), or worker_admin "*" for worker_admin "worker:<id>".
// Holding a role is not enough to hand it on.
//
// Queue changes go through PATCH /queue/{q} and clients through
// POST/GET/DELETE /client (DESIGN.md §6.1).

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/auth"
	"github.com/deweysasser/whisper-network/internal/fetchpolicy"
	"github.com/deweysasser/whisper-network/internal/store"
)

// holdsRole reports whether p holds role on any scope.
func holdsRole(p *Principal, role api.Role) bool {
	for _, g := range p.Grants {
		if g.Role == role {
			return true
		}
	}
	return false
}

// handleAdminVersion reports the server's own version, for comparing with
// the versions in GET /admin/workers. It is open to whoever may list
// workers.
func (s *Server) handleAdminVersion(w http.ResponseWriter, r *http.Request, p *Principal) error {
	if !holdsRole(p, api.RoleQueueAdmin) && !holdsRole(p, api.RoleWorkerAdmin) && !holdsRole(p, api.RoleAdmin) {
		return s.deny(r, p, "worker_admin or queue_admin required")
	}
	writeJSON(w, http.StatusOK, api.ServerVersion{Version: s.cfg.Version})
	return nil
}

func (s *Server) handleAdminListWorkers(w http.ResponseWriter, r *http.Request, p *Principal) error {
	queueAdmin := holdsRole(p, api.RoleQueueAdmin)
	if !queueAdmin && !holdsRole(p, api.RoleWorkerAdmin) && !holdsRole(p, api.RoleAdmin) {
		return s.deny(r, p, "worker_admin or queue_admin required")
	}
	ws, err := s.st.ListWorkers(r.Context())
	if err != nil {
		return err
	}
	out := []api.WorkerSummary{}
	for i := range ws {
		wk := &ws[i]
		if !queueAdmin && !auth.Can(p.Principal, auth.ActViewWorker, auth.Resource{WorkerID: wk.ID}) {
			continue
		}
		doc, err := s.workerAPI(r.Context(), wk)
		if err != nil {
			return err
		}
		out = append(out, api.WorkerSummary{Worker: *doc, RegisteredAt: wk.RegisteredAt,
			DecidedBy: wk.DecidedBy, DecidedAt: wk.DecidedAt, Gauges: gaugesOf(wk.Gauges), Stats: &wk.Stats,
			FailureStreak: wk.FailureStreak, FailureStreakJobs: wk.StreakJobs, PauseReason: wk.PauseReason})
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) handleAdminApprove(w http.ResponseWriter, r *http.Request, p *Principal) error {
	id := r.PathValue("id")
	var req api.ApproveWorkerRequest
	if err := decodeJSON(w, r, &req, true); err != nil {
		return err
	}
	if err := auth.CanApproveWorker(p.Principal, id, req.Queues); err != nil {
		return s.deny(r, p, err.Error())
	}
	if err := s.ops.ApproveWorker(r.Context(), p.Actor(), id, req.Queues); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Server) handleAdminWorkerState(w http.ResponseWriter, r *http.Request, p *Principal) error {
	id := r.PathValue("id")
	verb := r.URL.Path[strings.LastIndexByte(r.URL.Path, '/')+1:]
	var act auth.Action
	var state api.WorkerState
	switch verb {
	case "pause":
		act, state = auth.ActPauseWorker, api.WorkerPaused
	case "resume":
		act, state = auth.ActResumeWorker, api.WorkerActive
	case "reject":
		act, state = auth.ActRejectWorker, api.WorkerRejected
	default:
		return notFound()
	}
	if !auth.Can(p.Principal, act, auth.Resource{WorkerID: id}) {
		return s.deny(r, p, "worker_admin required")
	}
	if err := s.ops.SetWorkerState(r.Context(), p.Actor(), id, state); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// handleAdminDeleteWorker deletes a rejected worker now rather than after
// --rejected-worker-retention (Ops.DeleteWorker). It needs the right that
// rejects the worker; any state but rejected is 409.
func (s *Server) handleAdminDeleteWorker(w http.ResponseWriter, r *http.Request, p *Principal) error {
	id := r.PathValue("id")
	if !auth.Can(p.Principal, auth.ActDeleteWorker, auth.Resource{WorkerID: id}) {
		return s.deny(r, p, "worker_admin required")
	}
	if err := s.ops.DeleteWorker(r.Context(), p.Actor(), id); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Server) handleAdminListQueues(w http.ResponseWriter, r *http.Request, _ *Principal) error {
	qs, err := s.st.ListQueues(r.Context())
	if err != nil {
		return err
	}
	out := make([]api.Queue, len(qs))
	for i := range qs {
		out[i] = qs[i].API()
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) handleAdminCreateQueue(w http.ResponseWriter, r *http.Request, p *Principal) error {
	if !auth.Can(p.Principal, auth.ActCreateQueue, auth.Resource{}) {
		return s.deny(r, p, "admin required")
	}
	var req api.Queue
	if err := decodeJSON(w, r, &req, false); err != nil {
		return err
	}
	q, err := s.ops.CreateQueue(r.Context(), p.Actor(), req)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusCreated, q.API())
	return nil
}

func (s *Server) handleAdminListUsers(w http.ResponseWriter, r *http.Request, p *Principal) error {
	if !auth.Can(p.Principal, auth.ActCreateUser, auth.Resource{}) {
		return s.deny(r, p, "admin required")
	}
	us, err := s.st.ListUsers(r.Context())
	if err != nil {
		return err
	}
	out := make([]api.User, 0, len(us))
	for i := range us {
		u, err := s.ops.UserAPI(r.Context(), &us[i])
		if err != nil {
			return err
		}
		out = append(out, *u)
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) handleAdminCreateUser(w http.ResponseWriter, r *http.Request, p *Principal) error {
	if !auth.Can(p.Principal, auth.ActCreateUser, auth.Resource{}) {
		return s.deny(r, p, "admin required")
	}
	var req api.CreateUserRequest
	if err := decodeJSON(w, r, &req, false); err != nil {
		return err
	}
	if err := auth.CanGrantTo(p.Principal, req.Grants); err != nil {
		return s.deny(r, p, err.Error())
	}
	u, err := s.ops.CreateUser(r.Context(), p.Actor(), req)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusCreated, u)
	return nil
}

func (s *Server) handleAdminListClients(w http.ResponseWriter, r *http.Request, p *Principal) error {
	if !auth.Can(p.Principal, auth.ActCreateClient, auth.Resource{}) {
		return s.deny(r, p, "client_admin required")
	}
	cs, err := s.st.ListClients(r.Context())
	if err != nil {
		return err
	}
	out := make([]api.Client, 0, len(cs))
	for i := range cs {
		c, err := s.ops.ClientAPI(r.Context(), &cs[i])
		if err != nil {
			return err
		}
		out = append(out, *c)
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

// handleAdminListGrants authorizes before resolving the principal, so an
// unauthorized caller gets 403 whether or not the principal exists.
func (s *Server) handleAdminListGrants(w http.ResponseWriter, r *http.Request, p *Principal) error {
	principal := r.URL.Query().Get("principal")
	if principal != p.Name && !auth.Can(p.Principal, auth.ActGrantRoles, auth.Resource{}) {
		return s.deny(r, p, "admin required")
	}
	sp, _, err := s.ops.StorePrincipal(r.Context(), principal)
	if err != nil {
		return err
	}
	gs, err := s.ops.WireGrants(r.Context(), sp)
	if err != nil {
		return err
	}
	out := make([]api.PrincipalGrant, len(gs))
	for i, g := range gs {
		out[i] = api.PrincipalGrant{Principal: principal, Role: g.Role, Scope: g.Scope}
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

// handleAdminGrant gives an existing principal a grant (auth.CanGrantTo).
func (s *Server) handleAdminGrant(w http.ResponseWriter, r *http.Request, p *Principal) error {
	var req api.PrincipalGrant
	if err := decodeJSON(w, r, &req, false); err != nil {
		return err
	}
	g := api.Grant{Role: req.Role, Scope: req.Scope}
	if err := auth.CanGrantTo(p.Principal, []api.Grant{g}); err != nil {
		return s.deny(r, p, err.Error())
	}
	if err := s.ops.Grant(r.Context(), p.Actor(), req.Principal, g); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// handleAdminRevoke removes a grant. Revoking needs exactly the authority
// granting would (auth.CanGrantTo).
func (s *Server) handleAdminRevoke(w http.ResponseWriter, r *http.Request, p *Principal) error {
	q := r.URL.Query()
	g := api.Grant{Role: api.Role(q.Get("role")), Scope: q.Get("scope")}
	if err := auth.CanGrantTo(p.Principal, []api.Grant{g}); err != nil {
		return s.deny(r, p, err.Error())
	}
	if err := s.ops.Revoke(r.Context(), p.Actor(), q.Get("principal"), g); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Server) handleAdminAudit(w http.ResponseWriter, r *http.Request, p *Principal) error {
	if !auth.Can(p.Principal, auth.ActViewAudit, auth.Resource{}) {
		return s.deny(r, p, "admin required")
	}
	f, err := ParseAuditFilter(r.URL.Query())
	if err != nil {
		return err
	}
	limit := f.Limit
	es, err := s.st.QueryAudit(r.Context(), f)
	if err != nil {
		return err
	}
	out := api.AuditList{Entries: make([]api.AuditEntry, len(es))}
	for i, e := range es {
		out.Entries[i] = api.AuditEntry{ID: e.ID, TS: e.TS, Actor: e.Actor, Action: e.Action, Target: e.Target, Detail: e.Detail}
	}
	if len(es) == limit {
		out.NextBefore = es[len(es)-1].ID
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

// ParseAuditFilter reads the audit query parameters shared by
// GET /admin/audit and the UI audit page: before (entry id), limit
// (1..1000, default 100), action (prefix, e.g. "grant." or "auth."), actor
// and target (exact), since and until (RFC 3339 times; since <= ts < until).
func ParseAuditFilter(q url.Values) (store.AuditFilter, error) {
	f := store.AuditFilter{ActionPrefix: q.Get("action"), Actor: q.Get("actor"), Target: q.Get("target")}
	if v := q.Get("before"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			return f, badRequest("before must be an audit entry id")
		}
		f.Before = n
	}
	f.Limit, _ = strconv.Atoi(q.Get("limit"))
	if f.Limit <= 0 || f.Limit > 1000 {
		f.Limit = 100
	}
	for _, t := range []struct {
		name string
		dst  *time.Time
	}{{"since", &f.Since}, {"until", &f.Until}} {
		if v := q.Get(t.name); v != "" {
			ts, err := time.Parse(time.RFC3339, v)
			if err != nil {
				return f, badRequest(t.name + " must be an RFC 3339 time")
			}
			*t.dst = ts
		}
	}
	return f, nil
}

// handleAdminListFetchPolicies lists every stored fetch policy in key order,
// the invalid ones too (marked, with the reason, so they can be repaired).
func (s *Server) handleAdminListFetchPolicies(w http.ResponseWriter, r *http.Request, p *Principal) error {
	if !auth.Can(p.Principal, auth.ActManageFetchPolicy, auth.Resource{}) {
		return s.deny(r, p, "admin required")
	}
	rows, err := s.st.FetchPolicyRows(r.Context())
	if err != nil {
		return err
	}
	out := make([]api.FetchPolicy, len(rows))
	for i, row := range rows {
		out[i] = fetchPolicyAPI(row)
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

// handleAdminGetFetchPolicy is GET /admin/fetch-policies/{key}: one stored
// policy, valid or not.
func (s *Server) handleAdminGetFetchPolicy(w http.ResponseWriter, r *http.Request, p *Principal) error {
	if !auth.Can(p.Principal, auth.ActManageFetchPolicy, auth.Resource{}) {
		return s.deny(r, p, "admin required")
	}
	row, err := s.st.FetchPolicyRow(r.Context(), r.PathValue("key"))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, fetchPolicyAPI(row))
	return nil
}

// fetchPolicyAPI is the wire form of a stored fetch policy row, durations in
// compact form ("10m").
func fetchPolicyAPI(r store.FetchPolicyRow) api.FetchPolicy {
	rules := r.Rules
	if rules == nil {
		rules = []string{}
	}
	return api.FetchPolicy{Key: r.Key, Site: r.Site, EgressKey: r.EgressKey, Rules: rules,
		Concurrency:    r.Concurrency,
		BackoffInitial: fetchpolicy.CompactDuration(time.Duration(r.BackoffInitialS) * time.Second),
		BackoffCap:     fetchpolicy.CompactDuration(time.Duration(r.BackoffCapS) * time.Second),
		UpdatedBy:      r.UpdatedBy, UpdatedAt: r.UpdatedAt, Invalid: r.Invalid != "", InvalidReason: r.Invalid}
}

// policyFromRequest builds the store policy for PUT /admin/fetch-policies/{key}.
// Site and egress come from the key; a body that names others is a 400.
func policyFromRequest(key string, req api.FetchPolicy) (fetchpolicy.Policy, error) {
	site, egress, _ := strings.Cut(key, "@")
	if req.Key != "" && req.Key != key {
		return fetchpolicy.Policy{}, badRequest("body key " + strconv.Quote(req.Key) + " contradicts the path key")
	}
	if req.Site != "" && req.Site != site {
		return fetchpolicy.Policy{}, badRequest("body site " + strconv.Quote(req.Site) + " contradicts the key")
	}
	if req.EgressKey != "" && req.EgressKey != egress {
		return fetchpolicy.Policy{}, badRequest("body egress_key " + strconv.Quote(req.EgressKey) + " contradicts the key")
	}
	return ParseFetchPolicy(key, req.Rules, req.Concurrency, req.BackoffInitial, req.BackoffCap)
}

// ParseFetchPolicy builds a store policy from its wire fields: rules as
// "N/DUR" strings, durations as Go duration strings; site and egress come
// from key (split on the first '@'). Errors are 400s whose detail is safe to
// show; the policy's own invariants are checked when it is stored.
func ParseFetchPolicy(key string, rules []string, concurrency int, backoffInitial, backoffCap string) (fetchpolicy.Policy, error) {
	site, egress, _ := strings.Cut(key, "@")
	fp := fetchpolicy.Policy{Key: key, Site: site, EgressKey: egress, Concurrency: concurrency}
	for _, rs := range rules {
		ru, err := fetchpolicy.ParseRule(rs)
		if err != nil {
			return fp, badRequest("rule " + strconv.Quote(rs) + ": " + err.Error())
		}
		fp.Rules = append(fp.Rules, ru)
	}
	var err error
	if fp.BackoffInitial, err = time.ParseDuration(backoffInitial); err != nil {
		return fp, badRequest("backoff_initial: " + err.Error())
	}
	if fp.BackoffCap, err = time.ParseDuration(backoffCap); err != nil {
		return fp, badRequest("backoff_cap: " + err.Error())
	}
	return fp, nil
}

func (s *Server) handleAdminPutFetchPolicy(w http.ResponseWriter, r *http.Request, p *Principal) error {
	if !auth.Can(p.Principal, auth.ActManageFetchPolicy, auth.Resource{}) {
		return s.deny(r, p, "admin required")
	}
	var req api.FetchPolicy
	if err := decodeJSON(w, r, &req, false); err != nil {
		return err
	}
	key := r.PathValue("key")
	fp, err := policyFromRequest(key, req)
	if err != nil {
		return err
	}
	created, err := s.ops.PutFetchPolicy(r.Context(), p.Actor(), fp)
	if err != nil {
		return err
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	// Echo the stored policy, with its editor and time.
	row, err := s.st.FetchPolicyRow(r.Context(), key)
	if err != nil {
		return err
	}
	writeJSON(w, status, fetchPolicyAPI(row))
	return nil
}

func (s *Server) handleAdminDeleteFetchPolicy(w http.ResponseWriter, r *http.Request, p *Principal) error {
	if !auth.Can(p.Principal, auth.ActManageFetchPolicy, auth.Resource{}) {
		return s.deny(r, p, "admin required")
	}
	if err := s.ops.DeleteFetchPolicy(r.Context(), p.Actor(), r.PathValue("key")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Server) handleAdminListCooldowns(w http.ResponseWriter, r *http.Request, p *Principal) error {
	if !auth.Can(p.Principal, auth.ActManageFetchPolicy, auth.Resource{}) {
		return s.deny(r, p, "admin required")
	}
	cs, err := s.st.FetchCooldowns(r.Context(), s.now())
	if err != nil {
		return err
	}
	if cs == nil {
		cs = []api.FetchCooldown{}
	}
	writeJSON(w, http.StatusOK, cs)
	return nil
}

// maxCooldown caps a manual cooldown (the longest rate window).
const maxCooldown = 168 * time.Hour

// ParseCooldownDuration parses a manual cooldown's Go duration: positive and
// at most maxCooldown. Errors are 400s.
func ParseCooldownDuration(s string) (time.Duration, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, badRequest("duration: " + err.Error())
	}
	if d <= 0 || d > maxCooldown {
		return 0, badRequest("duration must be positive and at most " + maxCooldown.String())
	}
	return d, nil
}

func (s *Server) handleAdminSetCooldown(w http.ResponseWriter, r *http.Request, p *Principal) error {
	if !auth.Can(p.Principal, auth.ActManageFetchPolicy, auth.Resource{}) {
		return s.deny(r, p, "admin required")
	}
	var req api.SetCooldownRequest
	if err := decodeJSON(w, r, &req, false); err != nil {
		return err
	}
	d, err := ParseCooldownDuration(req.Duration)
	if err != nil {
		return err
	}
	until := s.now().Add(d)
	if err := s.ops.SetCooldown(r.Context(), p.Actor(), req.EgressKey, req.Site, until, req.Reason); err != nil {
		return err
	}
	// Echo the stored row: an automatic cooldown it replaced keeps its level.
	cd, err := s.st.FetchCooldown(r.Context(), req.EgressKey, req.Site)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusCreated, cd)
	return nil
}

// handleAdminClearCooldown clears a cooldown. The egress key (e.g.
// "label:home", "ip:2001:db8::1") may be percent-encoded; PathValue returns
// it decoded.
func (s *Server) handleAdminClearCooldown(w http.ResponseWriter, r *http.Request, p *Principal) error {
	if !auth.Can(p.Principal, auth.ActManageFetchPolicy, auth.Resource{}) {
		return s.deny(r, p, "admin required")
	}
	if err := s.ops.ClearCooldown(r.Context(), p.Actor(), r.PathValue("egress"), r.PathValue("site")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

package server

import (
	"database/sql"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/store"
)

// newClient creates an API client and returns its id and token.
func (h *harness) newClient(grants ...api.Grant) (string, string) {
	h.t.Helper()
	c, err := h.srv.Ops().CreateClient(h.ctx, CLIActor(), "test", grants)
	require.NoError(h.t, err)
	return c.ID, c.Token
}

// wireGrants returns a principal's grants in wire form.
func (h *harness) wireGrants(principal string) []api.Grant {
	h.t.Helper()
	sp, _, err := h.srv.Ops().StorePrincipal(h.ctx, principal)
	require.NoError(h.t, err)
	gs, err := h.srv.Ops().WireGrants(h.ctx, sp)
	require.NoError(h.t, err)
	return gs
}

func revokePath(principal string, g api.Grant) string {
	return "/admin/grants?principal=" + principal + "&role=" + string(g.Role) + "&scope=" + g.Scope
}

var (
	submitPublic = api.Grant{Role: api.RoleSubmit, Scope: "queue:public"}
	viewPublic   = api.Grant{Role: api.RoleViewAll, Scope: "queue:public"}
	qaPublic     = api.Grant{Role: api.RoleQueueAdmin, Scope: "queue:public"}
	clientAdmin  = api.Grant{Role: api.RoleClientAdmin, Scope: "*"}
)

// TestExploitSubmitHolderRevokesOthersGrant: a submit-only client used to
// DELETE another client's submit grant (204). It is now 403 and the grant
// survives.
func TestExploitSubmitHolderRevokesOthersGrant(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	_, attacker := h.newClient(submitPublic)
	victim, _ := h.newClient(submitPublic)

	resp, b := h.call("DELETE", revokePath("client:"+victim, submitPublic), attacker, nil)
	h.expectProblem(resp, b, http.StatusForbidden, api.ProblemForbidden)
	assert.Equal(t, []api.Grant{submitPublic}, h.wireGrants("client:"+victim))
}

// TestExploitHolderGrantsHeldRole: any role holder used to POST that role
// to any user or client. Holding a role no longer authorizes handing it on.
func TestExploitHolderGrantsHeldRole(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.expect("POST", "/admin/users", h.admin, api.CreateUserRequest{Username: "bob"}, http.StatusCreated, nil)
	target, _ := h.newClient()
	cases := []struct {
		name  string
		held  []api.Grant
		grant api.PrincipalGrant
	}{
		{"submit to a client", []api.Grant{submitPublic},
			api.PrincipalGrant{Principal: "client:" + target, Role: api.RoleSubmit, Scope: "queue:public"}},
		{"view_all to a user", []api.Grant{viewPublic},
			api.PrincipalGrant{Principal: "user:bob", Role: api.RoleViewAll, Scope: "queue:public"}},
		{"client_admin to a client", []api.Grant{clientAdmin},
			api.PrincipalGrant{Principal: "client:" + target, Role: api.RoleClientAdmin, Scope: "*"}},
		{"held grant by a client_admin", []api.Grant{clientAdmin, submitPublic},
			api.PrincipalGrant{Principal: "client:" + target, Role: api.RoleSubmit, Scope: "queue:public"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, tok := h.newClient(c.held...)
			resp, b := h.call("POST", "/admin/grants", tok, c.grant)
			h.expectProblem(resp, b, http.StatusForbidden, api.ProblemForbidden)
		})
	}
	assert.Empty(t, h.wireGrants("client:"+target))
	assert.Empty(t, h.wireGrants("user:bob"))
}

// TestExploitQueueAdminMintsQueueAdmin: a queue_admin used to grant
// queue_admin to others. It may manage submit/view_all/delete_all on its
// queue, but neither create nor remove queue admins.
func TestExploitQueueAdminMintsQueueAdmin(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	_, qa := h.newClient(qaPublic)
	other, _ := h.newClient()
	peer, _ := h.newClient(qaPublic)

	resp, b := h.call("POST", "/admin/grants", qa, api.PrincipalGrant{Principal: "client:" + other,
		Role: api.RoleQueueAdmin, Scope: "queue:public"})
	h.expectProblem(resp, b, http.StatusForbidden, api.ProblemForbidden)
	resp, b = h.call("DELETE", revokePath("client:"+peer, qaPublic), qa, nil)
	h.expectProblem(resp, b, http.StatusForbidden, api.ProblemForbidden)
	assert.Equal(t, []api.Grant{qaPublic}, h.wireGrants("client:"+peer))
	assert.Empty(t, h.wireGrants("client:"+other))

	// What a queue admin may do on its own queue still works, both ways.
	h.expect("POST", "/admin/grants", qa, api.PrincipalGrant{Principal: "client:" + other,
		Role: api.RoleSubmit, Scope: "queue:public"}, http.StatusNoContent, nil)
	h.expect("DELETE", revokePath("client:"+other, submitPublic), qa, nil, http.StatusNoContent, nil)
	h.expect("POST", "/admin/queues", h.admin, api.Queue{Name: "private"}, http.StatusCreated, nil)
	resp, b = h.call("POST", "/admin/grants", qa, api.PrincipalGrant{Principal: "client:" + other,
		Role: api.RoleSubmit, Scope: "queue:private"})
	h.expectProblem(resp, b, http.StatusForbidden, api.ProblemForbidden)
}

// TestRevokeClientNeedsCreatorAuthority: client_admin may revoke only
// clients it could have created; admin may revoke any.
func TestRevokeClientNeedsCreatorAuthority(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	_, ca := h.newClient(clientAdmin, submitPublic)
	small, _ := h.newClient(submitPublic)
	big, _ := h.newClient(submitPublic, viewPublic)
	adminClient, _ := h.newClient(api.Grant{Role: api.RoleAdmin, Scope: "*"})

	for _, id := range []string{big, adminClient} {
		resp, b := h.call("DELETE", "/client/"+id, ca, nil)
		h.expectProblem(resp, b, http.StatusForbidden, api.ProblemForbidden)
		c, err := h.st.ClientByID(h.ctx, id)
		require.NoError(t, err)
		assert.Nil(t, c.RevokedAt)
	}
	h.expect("DELETE", "/client/"+small, ca, nil, http.StatusNoContent, nil)
	h.expect("DELETE", "/client/"+big, h.admin, nil, http.StatusNoContent, nil)
	_, noRights := h.newClient(submitPublic)
	resp, b := h.call("DELETE", "/client/"+small, noRights, nil)
	h.expectProblem(resp, b, http.StatusForbidden, api.ProblemForbidden)
	resp, b = h.call("DELETE", "/client/cli_nonexistent", h.admin, nil)
	h.expectProblem(resp, b, http.StatusNotFound, api.ProblemNotFound)
}

// TestCreateClientKeepsRedelegation: POST /client still lets a client_admin
// hand on grants it holds (and only those, or what it administers).
func TestCreateClientKeepsRedelegation(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	_, ca := h.newClient(clientAdmin, submitPublic)
	h.expect("POST", "/client", ca, api.CreateClientRequest{Name: "x", Grants: []api.Grant{submitPublic, clientAdmin}},
		http.StatusCreated, nil)
	resp, b := h.call("POST", "/client", ca, api.CreateClientRequest{Name: "y", Grants: []api.Grant{viewPublic}})
	h.expectProblem(resp, b, http.StatusForbidden, api.ProblemForbidden)
	_, caqa := h.newClient(clientAdmin, qaPublic)
	h.expect("POST", "/client", caqa, api.CreateClientRequest{Name: "z", Grants: []api.Grant{viewPublic}},
		http.StatusCreated, nil)
}

// TestListGrantsDoesNotLeakExistence: GET /admin/grants authorizes before
// resolving the principal, so a non-admin gets 403 for a missing principal
// exactly as for an existing one, while still seeing its own grants.
func TestListGrantsDoesNotLeakExistence(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	self, tok := h.newClient(submitPublic)
	other, _ := h.newClient()
	for _, p := range []string{"client:" + other, "client:cli_missing", "user:admin", "user:nobody", "bogus"} {
		resp, b := h.call("GET", "/admin/grants?principal="+p, tok, nil)
		h.expectProblem(resp, b, http.StatusForbidden, api.ProblemForbidden)
	}
	var gs []api.PrincipalGrant
	h.expect("GET", "/admin/grants?principal=client:"+self, tok, nil, http.StatusOK, &gs)
	assert.Equal(t, []api.PrincipalGrant{{Principal: "client:" + self, Role: api.RoleSubmit, Scope: "queue:public"}}, gs)
	resp, b := h.call("GET", "/admin/grants?principal=user:nobody", h.admin, nil)
	h.expectProblem(resp, b, http.StatusNotFound, api.ProblemNotFound)
}

// TestWorkerAdminGrantsScopedWorkerAdmin: the only worker-scoped grant is
// worker_admin on worker:<id>; worker_admin "*" administers it.
func TestWorkerAdminGrantsScopedWorkerAdmin(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	_, wa := h.newClient(api.Grant{Role: api.RoleWorkerAdmin, Scope: "*"})
	h.expect("POST", "/admin/users", h.admin, api.CreateUserRequest{Username: "op"}, http.StatusCreated, nil)
	scoped := api.PrincipalGrant{Principal: "user:op", Role: api.RoleWorkerAdmin, Scope: "worker:w1"}
	h.expect("POST", "/admin/grants", wa, scoped, http.StatusNoContent, nil)
	h.expect("DELETE", revokePath("user:op", api.Grant{Role: api.RoleWorkerAdmin, Scope: "worker:w1"}), wa, nil,
		http.StatusNoContent, nil)
	resp, b := h.call("POST", "/admin/grants", wa, api.PrincipalGrant{Principal: "user:op",
		Role: api.RoleWorkerAdmin, Scope: "*"})
	h.expectProblem(resp, b, http.StatusForbidden, api.ProblemForbidden)
}

// TestOnlyAdminGrantsReadAll: read_all is granted and revoked by admin
// only. A queue admin, which does not itself read transcripts, may neither
// grant it, revoke it, nor mint a client holding it; a client_admin may hand
// on a read_all it holds when creating a client (re-delegation, no
// escalation).
func TestOnlyAdminGrantsReadAll(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	readPublic := api.Grant{Role: api.RoleReadAll, Scope: "queue:public"}
	_, qa := h.newClient(qaPublic, clientAdmin)
	target, _ := h.newClient()
	add := api.PrincipalGrant{Principal: "client:" + target, Role: api.RoleReadAll, Scope: "queue:public"}

	resp, b := h.call("POST", "/admin/grants", qa, add)
	h.expectProblem(resp, b, http.StatusForbidden, api.ProblemForbidden)
	resp, b = h.call("POST", "/client", qa, api.CreateClientRequest{Name: "r", Grants: []api.Grant{readPublic}})
	h.expectProblem(resp, b, http.StatusForbidden, api.ProblemForbidden)
	assert.Empty(t, h.wireGrants("client:"+target))

	h.expect("POST", "/admin/grants", h.admin, add, http.StatusNoContent, nil)
	assert.Equal(t, []api.Grant{readPublic}, h.wireGrants("client:"+target))
	resp, b = h.call("DELETE", revokePath("client:"+target, readPublic), qa, nil)
	h.expectProblem(resp, b, http.StatusForbidden, api.ProblemForbidden)
	h.expect("DELETE", revokePath("client:"+target, readPublic), h.admin, nil, http.StatusNoContent, nil)
	assert.Empty(t, h.wireGrants("client:"+target))

	_, holder := h.newClient(clientAdmin, readPublic)
	h.expect("POST", "/client", holder, api.CreateClientRequest{Name: "delegate", Grants: []api.Grant{readPublic}},
		http.StatusCreated, nil)
}

func TestGrantFitsWorkerKind(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	proc, _ := h.activeWorker(api.WorkerFacts{}, "public")
	dl, _ := h.worker(api.WorkerFacts{Kind: api.WorkerDownloader, Kinds: []api.TaskKind{}})
	ops, a := h.srv.Ops(), CLIActor()
	fetch := api.Grant{Role: api.RoleFetch, Scope: "queue:public"}
	process := api.Grant{Role: api.RoleProcess, Scope: "queue:public"}

	var pe *problem
	err := ops.Grant(h.ctx, a, "worker:"+proc, fetch)
	require.ErrorAs(t, err, &pe)
	assert.Equal(t, http.StatusBadRequest, pe.status)
	err = ops.Grant(h.ctx, a, "worker:"+dl, process)
	require.ErrorAs(t, err, &pe)
	assert.Equal(t, http.StatusBadRequest, pe.status)
	require.NoError(t, ops.Grant(h.ctx, a, "worker:"+dl, fetch))
	require.NoError(t, ops.Grant(h.ctx, a, "worker:"+proc, process))
	cid, _ := h.newClient()
	require.Error(t, ops.Grant(h.ctx, a, "client:"+cid, fetch))
}

func TestApproveDownloaderGrantsFetch(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	id, _ := h.activeWorker(api.WorkerFacts{Kind: api.WorkerDownloader, Kinds: []api.TaskKind{}}, "public")
	assert.Equal(t, []api.Grant{{Role: api.RoleFetch, Scope: "queue:public"}}, h.wireGrants("worker:"+id))
	pid, _ := h.activeWorker(api.WorkerFacts{}, "public")
	assert.Equal(t, []api.Grant{{Role: api.RoleProcess, Scope: "queue:public"}}, h.wireGrants("worker:"+pid))
}

// fetchingJob inserts a fetching job with a pending fetch task on the public
// queue through a second connection (nothing creates one over the API yet).
func (h *harness) fetchingJob(id string) {
	h.t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(h.dir, "wn.db")+"?_pragma=busy_timeout(5000)")
	require.NoError(h.t, err)
	defer db.Close()
	_, err = db.ExecContext(h.ctx, `INSERT INTO jobs (id, queue_id, client_id, state, model, language, created_at, updated_at,
		fetch_url, fetch_site) VALUES (?, (SELECT id FROM queues WHERE name = 'public'), 'c', 'fetching', 'm', 'en', 1, 1,
		'https://x/y', 'x')`, id)
	require.NoError(h.t, err)
	_, err = db.ExecContext(h.ctx, `INSERT INTO tasks (job_id, seq, kind, cost, state) VALUES (?, -1, 'fetch', 1, 'pending')`, id)
	require.NoError(h.t, err)
}

func (h *harness) auditDetails(action string) []string {
	h.t.Helper()
	es, err := h.st.QueryAudit(h.ctx, store.AuditFilter{Limit: 1000})
	require.NoError(h.t, err)
	var out []string
	for _, e := range es {
		if e.Action == action {
			out = append(out, e.Detail)
		}
	}
	return out
}

func (h *harness) assertJobFailedReported(job string) {
	h.t.Helper()
	assert.Equal(h.t, api.JobFailed, h.job(job).State)
	jf := h.logs.events(h.t, obs.EventJobFailed)
	require.Len(h.t, jf, 1)
	assert.Equal(h.t, job, jf[0][obs.FieldJobID])
	mb := &syncBuffer{}
	h.metrics.WritePrometheus(mb)
	assert.Contains(h.t, string(mb.Bytes()), `whisper_jobs_finished_total{queue="public",outcome="failed"} 1`)
}

var dlFacts = api.WorkerFacts{Kind: api.WorkerDownloader, Kinds: []api.TaskKind{}}

func TestRevokeLastFetchReportsFailedJob(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	dl, _ := h.activeWorker(dlFacts, "public")
	h.fetchingJob("jf")
	require.NoError(t, h.srv.Ops().Revoke(h.ctx, CLIActor(), "worker:"+dl, api.Grant{Role: api.RoleFetch, Scope: "queue:public"}))
	h.assertJobFailedReported("jf")
	d := h.auditDetails("grant.remove")
	require.Len(t, d, 1)
	assert.Contains(t, d[0], `"failed_jobs":["jf"]`)
	assert.NotContains(t, d[0], "cancelled_leases", "the revoked worker held no lease")
}

func TestRejectLastDownloaderReportsFailedJob(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	dl, _ := h.activeWorker(dlFacts, "public")
	h.fetchingJob("jf")
	require.NoError(t, h.srv.Ops().SetWorkerState(h.ctx, CLIActor(), dl, api.WorkerRejected))
	h.assertJobFailedReported("jf")
	d := h.auditDetails("worker.reject")
	require.Len(t, d, 1)
	assert.Contains(t, d[0], `"failed_jobs":["jf"]`)
	assert.Contains(t, d[0], `"cancelled_leases":0`)
}

func TestFactsBelowFloorReportsFailedJob(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	f := dlFacts
	f.SandboxLevel = api.SandboxFull
	dl, _ := h.activeWorker(f, "public")
	h.fetchingJob("jf")
	f.SandboxLevel = api.SandboxFSOnly
	require.NoError(t, h.srv.Ops().UpdateWorkerFacts(h.ctx, Actor{Name: "worker:" + dl, Grant: "worker:" + dl}, dl, f))
	h.assertJobFailedReported("jf")
}

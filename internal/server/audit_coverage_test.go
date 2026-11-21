package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/media"
	"github.com/deweysasser/whisper-network/internal/obs/obstest"
	"github.com/deweysasser/whisper-network/internal/store"
)

// auditMark returns the id of the newest audit entry.
func (h *harness) auditMark() int64 {
	h.t.Helper()
	es, err := h.st.QueryAudit(h.ctx, store.AuditFilter{Limit: 1})
	require.NoError(h.t, err)
	if len(es) == 0 {
		return 0
	}
	return es[0].ID
}

// later marks an actor or target that is not known until the step has run;
// the caller checks it on the returned entry.
const later = "\x00later"

// expectOneAudit asserts that exactly one audit entry was written after
// mark, with the given action, actor and target.
func (h *harness) expectOneAudit(mark int64, action, actor, target string) store.AuditEntry {
	h.t.Helper()
	es, err := h.st.QueryAudit(h.ctx, store.AuditFilter{Limit: 1000})
	require.NoError(h.t, err)
	var fresh []store.AuditEntry
	for _, e := range es {
		if e.ID > mark {
			fresh = append(fresh, e)
		}
	}
	require.Len(h.t, fresh, 1, "%s: exactly one audit entry, got %+v", action, fresh)
	e := fresh[0]
	assert.Equal(h.t, action, e.Action)
	if actor != later {
		assert.Equal(h.t, actor, e.Actor, action)
	}
	if target != later {
		assert.Equal(h.t, target, e.Target, action)
	}
	return e
}

// TestAuditCoverage drives every audited mutation through the HTTP API (or,
// for call sites outside the admin API, through the Ops method they use) and
// checks it writes exactly one audit entry with the right action, actor and
// target. It then checks that no audit detail holds a secret.
func TestAuditCoverage(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ops := h.srv.Ops()
	adminID, admin := h.newClient(api.Grant{Role: api.RoleAdmin, Scope: "*"})
	adminName := "client:" + adminID
	var secrets []string
	secrets = append(secrets, admin)

	step := func(action, actor, target string, do func()) store.AuditEntry {
		t.Helper()
		mark := h.auditMark()
		do()
		return h.expectOneAudit(mark, action, actor, target)
	}
	httpOK := func(method, path string, body any, status int, out any) func() {
		return func() { h.expect(method, path, admin, body, status, out) }
	}

	// Queues.
	step("queue.create", adminName, "queue:q1", httpOK("POST", "/admin/queues", api.Queue{Name: "q1"}, 201, nil))
	desc, weight := "first", 200
	step("queue.patch", adminName, "queue:q1",
		httpOK("PATCH", "/queue/q1", api.QueuePatch{Description: &desc, Weight: &weight}, 200, nil))
	q1, err := h.st.QueueByName(h.ctx, "q1")
	require.NoError(t, err)
	step("queue.rename", adminName, "queue:q2", func() {
		require.NoError(t, ops.RenameQueue(h.ctx, Actor{Name: adminName, Grant: adminName}, q1, "q2"))
	})

	// Users and grants.
	var alice api.User
	step("user.create", adminName, "user:alice", httpOK("POST", "/admin/users",
		api.CreateUserRequest{Username: "alice", Grants: []api.Grant{viewPublic}}, 201, &alice))
	secrets = append(secrets, alice.Password)
	step("grant.add", adminName, "user:alice", httpOK("POST", "/admin/grants",
		api.PrincipalGrant{Principal: "user:alice", Role: api.RoleSubmit, Scope: "queue:public"}, 204, nil))
	step("grant.remove", adminName, "user:alice",
		httpOK("DELETE", revokePath("user:alice", submitPublic), nil, 204, nil))
	var pw string
	step("user.reset_password", adminName, "user:alice", func() {
		pw, err = ops.ResetPassword(h.ctx, Actor{Name: adminName, Grant: adminName}, "alice", "")
		require.NoError(t, err)
	})
	secrets = append(secrets, pw)
	u, err := h.st.UserByUsername(h.ctx, "alice")
	require.NoError(t, err)
	step("user.change_password", "user:alice", "user:alice", func() {
		require.NoError(t, ops.ChangePassword(h.ctx, u, u.PasswordHash, "alice-own-password"))
	})
	secrets = append(secrets, "alice-own-password")

	// Sign-in events (called from the UI seam).
	step("auth.login", "user:alice", "user:alice", func() {
		require.NoError(t, ops.CreateSession(h.ctx, u, store.Session{ID: "sid-1", UserID: u.ID, CSRF: "csrf-1",
			ExpiresAt: h.clk.Now().Add(time.Hour)}, "", "192.0.2.1"))
	})
	step("auth.logout", "user:alice", "user:alice", func() { require.NoError(t, ops.Logout(h.ctx, "sid-1")) })
	step("auth.login_failed", "anonymous", "user:alice", func() {
		require.NoError(t, ops.LoginFailed(h.ctx, "alice", "192.0.2.1", "bad_credentials"))
	})
	step("auth.login_failed", "anonymous", "", func() {
		require.NoError(t, ops.LoginFailed(h.ctx, "hunter2-typed-as-name", "192.0.2.1", "bad_credentials"))
	})
	secrets = append(secrets, "hunter2-typed-as-name")
	require.NoError(t, ops.CreateSession(h.ctx, u, store.Session{ID: "sid-2", UserID: u.ID, CSRF: "csrf-2",
		ExpiresAt: h.clk.Now().Add(time.Minute)}, "", ""))
	h.clk.Advance(2 * time.Minute)
	step("session.expire", store.SystemActor, "", func() {
		n, err := ops.DeleteExpiredSessions(h.ctx)
		require.NoError(t, err)
		assert.EqualValues(t, 1, n)
	})
	step("user.disable", adminName, "user:alice", func() {
		require.NoError(t, ops.DisableUser(h.ctx, Actor{Name: adminName, Grant: adminName}, "alice"))
	})

	// Clients.
	var c api.Client
	e := step("client.create", adminName, later, httpOK("POST", "/client",
		api.CreateClientRequest{Name: "svc", Grants: []api.Grant{submitPublic}}, 201, &c))
	assert.Equal(t, "client:"+c.ID, e.Target)
	secrets = append(secrets, c.Token)
	step("client.revoke", adminName, "client:"+c.ID, httpOK("DELETE", "/client/"+c.ID, nil, 204, nil))

	// Workers.
	var wk *store.Worker
	e = step("worker.register", later, later, func() {
		wk, err = ops.RegisterWorker(h.ctx, "hash-of-a-token", api.WorkerFacts{Hostname: "nuc"}, store.PendingLimits{})
		require.NoError(t, err)
	})
	wname := "worker:" + wk.ID
	assert.Equal(t, wname, e.Actor, "a worker registers itself")
	assert.Equal(t, wname, e.Target)
	step("worker.update_facts", wname, wname, func() {
		require.NoError(t, ops.UpdateWorkerFacts(h.ctx, Actor{Name: wname, Grant: wname}, wk.ID,
			api.WorkerFacts{Hostname: "nuc", Slots: 2}))
	})
	step("worker.approve", adminName, wname, httpOK("POST", "/admin/workers/"+wk.ID+"/approve",
		api.ApproveWorkerRequest{Queues: []string{"public"}}, 204, nil))
	step("worker.pause", adminName, wname, httpOK("POST", "/admin/workers/"+wk.ID+"/pause", nil, 204, nil))
	step("worker.resume", adminName, wname, httpOK("POST", "/admin/workers/"+wk.ID+"/resume", nil, 204, nil))
	step("worker.reject", adminName, wname, httpOK("POST", "/admin/workers/"+wk.ID+"/reject", nil, 204, nil))
	step("worker.delete", adminName, wname, httpOK("DELETE", "/admin/workers/"+wk.ID, nil, 204, nil))
	pending, err := ops.RegisterWorker(h.ctx, "hash-2", api.WorkerFacts{Hostname: "old"}, store.PendingLimits{})
	require.NoError(t, err)
	h.clk.Advance(48 * time.Hour)
	step("worker.prune", store.SystemActor, "worker:"+pending.ID, func() {
		_, err := ops.PrunePendingWorkers(h.ctx, h.clk.Now().Add(-time.Hour))
		require.NoError(t, err)
	})

	// Jobs.
	ownerID, owner := h.newClient(submitPublic)
	pub, err := h.st.QueueByName(h.ctx, "public")
	require.NoError(t, err)
	const prompt = "the confidential prompt"
	var job *store.Job
	e = step("job.create", "client:"+ownerID, later, func() {
		job, _, err = ops.CreateJob(h.ctx, Actor{Name: "client:" + ownerID, Grant: "client:" + ownerID}, "public",
			store.NewJob{QueueID: pub.ID, ClientID: ownerID, Params: api.JobParams{Model: "small.en", Prompt: prompt,
				Metadata: json.RawMessage(`{"case":"secret-metadata"}`)},
				Original: store.Media{Bytes: 10, SHA256: strings.Repeat("0", 64), Type: "audio/wav"}})
		require.NoError(t, err)
	})
	assert.Equal(t, "job:"+job.ID, e.Target)
	secrets = append(secrets, prompt, "secret-metadata")
	step("job.purge_media", store.SystemActor, "job:"+job.ID, func() {
		require.NoError(t, ops.MediaPurged(h.ctx, job.ID, MediaOriginal))
	})
	step("job.delete", store.SystemActor, "job:"+job.ID, func() {
		_, err := ops.DeleteJob(h.ctx, SystemActor(), job.ID)
		require.NoError(t, err)
	})
	viaAPI := h.submit(owner, "public", api.JobParams{Model: "small.en"}, canonicalWAV(speech(1, media.SampleRate)))
	mark := h.auditMark()
	h.expect("DELETE", "/job/"+viaAPI.ID, owner, nil, http.StatusNoContent, nil)
	es, err := h.st.QueryAudit(h.ctx, store.AuditFilter{Target: "job:" + viaAPI.ID, ActionPrefix: "job.delete"})
	require.NoError(t, err)
	require.Len(t, es, 1, "DELETE /job audits once")
	assert.Greater(t, es[0].ID, mark)
	assert.Equal(t, "client:"+ownerID, es[0].Actor)

	// Fetch policies and cooldowns.
	step("fetch_policy.put", adminName, "fetch_policy:example.com", httpOK("PUT", "/admin/fetch-policies/example.com",
		api.FetchPolicy{Rules: []string{"1/1m"}, Concurrency: 1, BackoffInitial: "1m", BackoffCap: "1h"}, 201, nil))
	step("fetch_policy.delete", adminName, "fetch_policy:example.com",
		httpOK("DELETE", "/admin/fetch-policies/example.com", nil, 204, nil))
	step("fetch_cooldown.set", adminName, "fetch_cooldown:label:home/example.com", httpOK("POST", "/admin/fetch-cooldowns",
		api.SetCooldownRequest{EgressKey: "label:home", Site: "example.com", Duration: "1h", Reason: "test"}, 201, nil))
	step("fetch_cooldown.clear", adminName, "fetch_cooldown:label:home/example.com",
		httpOK("DELETE", "/admin/fetch-cooldowns/label%3Ahome/example.com", nil, 204, nil))

	assertAuditClean(t, h, secrets)
}

// assertAuditClean checks every audit entry against the never-log rules
// (forbidden field names at any depth, token-looking values) and that none
// of the given secrets appears anywhere in an entry.
func assertAuditClean(t *testing.T, h *harness, secrets []string) {
	t.Helper()
	es, err := h.st.QueryAudit(h.ctx, store.AuditFilter{Limit: 1000})
	require.NoError(t, err)
	var jsonl strings.Builder
	for _, e := range es {
		line := map[string]any{"actor": e.Actor, "action": e.Action, "target": e.Target}
		if e.Detail != "" {
			var d any
			if err := json.Unmarshal([]byte(e.Detail), &d); err != nil {
				d = e.Detail
			}
			line["detail"] = d
		}
		b, err := json.Marshal(line)
		require.NoError(t, err)
		jsonl.Write(append(b, '\n'))
		for _, s := range secrets {
			if s == "" {
				continue
			}
			for _, field := range []string{e.Actor, e.Target, e.Detail} {
				assert.NotContains(t, field, s, "audit entry %d (%s) leaks a secret", e.ID, e.Action)
			}
		}
	}
	obstest.AssertClean(t, []byte(jsonl.String()))
}

// TestAdminAuditFilters: GET /admin/audit filters by action prefix, actor,
// target and time range, and rejects malformed filters.
func TestAdminAuditFilters(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	adminID, admin := h.newClient(api.Grant{Role: api.RoleAdmin, Scope: "*"})
	start := h.clk.Now()
	h.expect("POST", "/admin/queues", admin, api.Queue{Name: "q1"}, 201, nil)
	h.clk.Advance(time.Hour)
	h.expect("POST", "/admin/users", admin, api.CreateUserRequest{Username: "bob"}, 201, nil)
	h.expect("POST", "/admin/grants", admin, api.PrincipalGrant{Principal: "user:bob", Role: api.RoleSubmit,
		Scope: "queue:q1"}, 204, nil)

	list := func(query string) []string {
		t.Helper()
		var out api.AuditList
		h.expect("GET", "/admin/audit?"+query, admin, nil, http.StatusOK, &out)
		var got []string
		for _, e := range out.Entries {
			got = append(got, e.Action+" "+e.Target)
		}
		return got
	}
	actor := "client:" + adminID
	assert.Equal(t, []string{"grant.add user:bob", "user.create user:bob", "queue.create queue:q1"},
		list("actor="+actor))
	assert.Equal(t, []string{"grant.add user:bob"}, list("action=grant."))
	assert.Equal(t, []string{"grant.add user:bob", "user.create user:bob"}, list("target=user:bob"))
	assert.Equal(t, []string{"queue.create queue:q1"},
		list("actor="+actor+"&until="+start.Add(time.Minute).Format(time.RFC3339)))
	assert.Equal(t, []string{"grant.add user:bob", "user.create user:bob"},
		list("actor="+actor+"&since="+start.Add(time.Minute).Format(time.RFC3339)))
	for _, bad := range []string{"since=yesterday", "until=1", "before=x"} {
		resp, b := h.call("GET", "/admin/audit?"+bad, admin, nil)
		h.expectProblem(resp, b, http.StatusBadRequest, api.ProblemBadRequest)
	}
}

package server

import (
	"database/sql"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/store"
)

func goodPolicy() api.FetchPolicy {
	return api.FetchPolicy{Rules: []string{"1/3m", "5/1h"}, Concurrency: 3, BackoffInitial: "10m", BackoffCap: "2h"}
}

func TestFetchPolicyAPIAdminOnly(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	qa := h.client(api.Grant{Role: api.RoleQueueAdmin, Scope: "queue:public"})
	for _, c := range []struct {
		method, path string
		body         any
	}{
		{"GET", "/admin/fetch-policies", nil},
		{"PUT", "/admin/fetch-policies/example.com", goodPolicy()},
		{"DELETE", "/admin/fetch-policies/example.com", nil},
		{"GET", "/admin/fetch-cooldowns", nil},
		{"POST", "/admin/fetch-cooldowns", api.SetCooldownRequest{EgressKey: "label:home", Site: "example.com", Duration: "1h"}},
		{"DELETE", "/admin/fetch-cooldowns/label:home/example.com", nil},
	} {
		resp, b := h.call(c.method, c.path, qa, c.body)
		h.expectProblem(resp, b, http.StatusForbidden, api.ProblemForbidden)
	}
}

func TestFetchPolicyPutValidates(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	bad := func(mut func(*api.FetchPolicy), key string) {
		t.Helper()
		p := goodPolicy()
		mut(&p)
		resp, b := h.call("PUT", "/admin/fetch-policies/"+key, h.admin, p)
		h.expectProblem(resp, b, http.StatusBadRequest, api.ProblemBadRequest)
	}
	bad(func(p *api.FetchPolicy) { p.Rules = []string{"0/1m"} }, "example.com")
	bad(func(p *api.FetchPolicy) { p.Rules = []string{"nonsense"} }, "example.com")
	bad(func(p *api.FetchPolicy) { p.BackoffInitial = "soon" }, "example.com")
	bad(func(p *api.FetchPolicy) { p.BackoffCap = "1m" }, "example.com")
	bad(func(p *api.FetchPolicy) { p.Concurrency = 0 }, "example.com")
	bad(func(p *api.FetchPolicy) {}, "Not%20A%20Site")
	bad(func(p *api.FetchPolicy) { p.Site = "other.com" }, "example.com")
	bad(func(p *api.FetchPolicy) { p.EgressKey = "label:work" }, "example.com@label:home")
	bad(func(p *api.FetchPolicy) { p.Key = "other.com" }, "example.com")
	var ps []api.FetchPolicy
	h.expect("GET", "/admin/fetch-policies", h.admin, nil, 200, &ps)
	assert.Len(t, ps, 2, "only the seeded policies remain")
}

func TestFetchPolicyRoundTrip(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	var seeded []api.FetchPolicy
	h.expect("GET", "/admin/fetch-policies", h.admin, nil, 200, &seeded)
	require.Len(t, seeded, 2)
	assert.Equal(t, "*", seeded[0].Key)
	assert.Equal(t, "youtube.com", seeded[1].Key)
	assert.Equal(t, []string{"1/3m0s"}, seeded[1].Rules)
	assert.Equal(t, "12h0m0s", seeded[1].BackoffInitial)

	// The path key wins; site and egress derive from it.
	mark := h.auditMark()
	var got api.FetchPolicy
	h.expect("PUT", "/admin/fetch-policies/example.com@label:home", h.admin, goodPolicy(), http.StatusCreated, &got)
	assert.Equal(t, "example.com", got.Site)
	assert.Equal(t, "label:home", got.EgressKey)
	assert.NotEmpty(t, got.UpdatedBy)
	e := h.expectOneAudit(mark, "fetch_policy.put", later, "fetch_policy:example.com@label:home")
	assert.Contains(t, e.Detail, `"concurrency":3`)
	assert.NotContains(t, e.Detail, "http")

	p := goodPolicy()
	p.Concurrency = 5
	h.expect("PUT", "/admin/fetch-policies/example.com@label:home", h.admin, p, http.StatusOK, &got)
	assert.Equal(t, 5, got.Concurrency)

	var ps []api.FetchPolicy
	h.expect("GET", "/admin/fetch-policies", h.admin, nil, 200, &ps)
	require.Len(t, ps, 3)
	assert.Equal(t, "example.com@label:home", ps[1].Key)
	assert.Equal(t, 5, ps[1].Concurrency)
	assert.Equal(t, "label:home", ps[1].EgressKey)
	assert.False(t, ps[1].UpdatedAt.IsZero())
	assert.NotEmpty(t, ps[1].UpdatedBy)

	mark = h.auditMark()
	h.expect("DELETE", "/admin/fetch-policies/example.com@label:home", h.admin, nil, http.StatusNoContent, nil)
	h.expectOneAudit(mark, "fetch_policy.delete", later, "fetch_policy:example.com@label:home")
	resp, b := h.call("DELETE", "/admin/fetch-policies/example.com@label:home", h.admin, nil)
	h.expectProblem(resp, b, http.StatusNotFound, api.ProblemNotFound)
}

func TestDeleteDefaultPolicy409(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	resp, b := h.call("DELETE", "/admin/fetch-policies/*", h.admin, nil)
	h.expectProblem(resp, b, http.StatusConflict, api.ProblemConflict)
}

func TestCooldownSetAndClear(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	for _, egress := range []string{"label:home", "ip:2001:db8::1"} {
		mark := h.auditMark()
		var cd api.FetchCooldown
		h.expect("POST", "/admin/fetch-cooldowns", h.admin, api.SetCooldownRequest{EgressKey: egress,
			Site: "example.com", Duration: "90m", Reason: "blocked"}, http.StatusCreated, &cd)
		assert.True(t, cd.Manual)
		e := h.expectOneAudit(mark, "fetch_cooldown.set", later, "fetch_cooldown:"+egress+"/example.com")
		assert.Contains(t, e.Detail, "blocked")

		var cds []api.FetchCooldown
		h.expect("GET", "/admin/fetch-cooldowns", h.admin, nil, 200, &cds)
		require.Len(t, cds, 1)
		assert.Equal(t, egress, cds[0].EgressKey)
		assert.Equal(t, h.clk.Now().Add(90*time.Minute).Unix(), cds[0].Until.Unix())

		mark = h.auditMark()
		h.expect("DELETE", "/admin/fetch-cooldowns/"+escapeSeg(egress)+"/example.com", h.admin, nil,
			http.StatusNoContent, nil)
		h.expectOneAudit(mark, "fetch_cooldown.clear", later, "fetch_cooldown:"+egress+"/example.com")
		h.expect("GET", "/admin/fetch-cooldowns", h.admin, nil, 200, &cds)
		assert.Empty(t, cds)
	}
	for _, bad := range []api.SetCooldownRequest{
		{EgressKey: "label:home", Site: "example.com", Duration: "never"},
		{EgressKey: "label:home", Site: "example.com", Duration: "-1h"},
		{EgressKey: "", Site: "example.com", Duration: "1h"},
		{EgressKey: "label:home", Site: "", Duration: "1h"},
	} {
		resp, b := h.call("POST", "/admin/fetch-cooldowns", h.admin, bad)
		h.expectProblem(resp, b, http.StatusBadRequest, api.ProblemBadRequest)
	}
}

// escapeSeg percent-encodes ':' (and everything else unreserved-unsafe) in a
// path segment.
func escapeSeg(s string) string {
	var out []byte
	for _, c := range []byte(s) {
		if c == ':' {
			out = append(out, '%', '3', 'A')
		} else {
			out = append(out, c)
		}
	}
	return string(out)
}

func TestReaperFailsOrphanedFetchJobs(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	client := h.client(submitURLOn("public"))
	did, _ := h.downloader("", "", "public")
	j := h.fetchSubmit(client, "public", exURL)

	// Remove the downloader behind the store's back, as a raised sandbox
	// floor would.
	db, err := sql.Open("sqlite", "file:"+filepath.Join(h.dir, "wn.db")+"?_pragma=busy_timeout(5000)")
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(`UPDATE workers SET state = 'rejected' WHERE id = ?`, did)
	require.NoError(t, err)

	h.srv.reapOnce(h.ctx)
	got, err := h.st.Job(h.ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, api.JobFailed, got.State)
	assert.Contains(t, got.Error, "no downloader")
	_ = store.SystemActor
}

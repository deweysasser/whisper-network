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
	"github.com/deweysasser/whisper-network/internal/media"
	"github.com/deweysasser/whisper-network/internal/testscale"
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
		{"GET", "/admin/fetch-policies/youtube.com", nil},
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
	assert.Equal(t, []string{"1/3m"}, seeded[1].Rules)
	assert.Equal(t, "12h", seeded[1].BackoffInitial, "durations in compact form")

	// The path key wins; site and egress derive from it.
	mark := h.auditMark()
	var got api.FetchPolicy
	h.expect("PUT", "/admin/fetch-policies/example.com@label:home", h.admin, goodPolicy(), http.StatusCreated, &got)
	assert.Equal(t, "example.com", got.Site)
	assert.Equal(t, "label:home", got.EgressKey)
	assert.NotEmpty(t, got.UpdatedBy)
	assert.Equal(t, "10m", got.BackoffInitial)
	e := h.expectOneAudit(mark, "fetch_policy.put", later, "fetch_policy:example.com@label:home")
	assert.Contains(t, e.Detail, `"concurrency":3`)
	assert.Contains(t, e.Detail, `"backoff_cap":"2h"`)
	assert.NotContains(t, e.Detail, "http")

	// The echo is the stored policy, not the request: a client's invalid
	// flag is not repeated, durations come back compact.
	p := goodPolicy()
	p.Concurrency = 5
	p.BackoffInitial = "600s"
	p.Invalid, p.InvalidReason = true, "client says so"
	h.expect("PUT", "/admin/fetch-policies/example.com@label:home", h.admin, p, http.StatusOK, &got)
	assert.Equal(t, 5, got.Concurrency)
	assert.Equal(t, "10m", got.BackoffInitial)
	assert.False(t, got.Invalid)
	assert.Empty(t, got.InvalidReason)

	var one api.FetchPolicy
	h.expect("GET", "/admin/fetch-policies/example.com@label:home", h.admin, nil, http.StatusOK, &one)
	assert.Equal(t, got, one, "GET of one key is the PUT's echo")
	resp, b := h.call("GET", "/admin/fetch-policies/nosuch.example", h.admin, nil)
	h.expectProblem(resp, b, http.StatusNotFound, api.ProblemNotFound)

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
	resp, b = h.call("DELETE", "/admin/fetch-policies/example.com@label:home", h.admin, nil)
	h.expectProblem(resp, b, http.StatusNotFound, api.ProblemNotFound)
}

// TestGetInvalidFetchPolicy: an invalid stored row reads back raw, with why.
func TestGetInvalidFetchPolicy(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.corruptPolicy("youtube.com", `rules = '["nonsense"]'`)
	var got api.FetchPolicy
	h.expect("GET", "/admin/fetch-policies/youtube.com", h.admin, nil, http.StatusOK, &got)
	assert.True(t, got.Invalid)
	assert.NotEmpty(t, got.InvalidReason)
	assert.Equal(t, []string{"nonsense"}, got.Rules)
}

// TestSetCooldownEchoesStoredRow: a manual cooldown over an automatic one
// keeps its level, and the response says so.
func TestSetCooldownEchoesStoredRow(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.execDB(`INSERT INTO fetch_cooldowns (egress_key, site, until, level, manual, set_by, reason)
		VALUES ('label:home', 'example.com', 1, 3, 0, 'system', 'rate limited')`)
	var cd api.FetchCooldown
	h.expect("POST", "/admin/fetch-cooldowns", h.admin, api.SetCooldownRequest{EgressKey: "label:home",
		Site: "example.com", Duration: "1h", Reason: "blocked"}, http.StatusCreated, &cd)
	assert.Equal(t, 3, cd.Level, "the stored level")
	assert.True(t, cd.Manual)
	assert.Equal(t, "blocked", cd.Reason)
	assert.Equal(t, h.clk.Now().Add(time.Hour).Unix(), cd.Until.Unix())
	assert.NotEmpty(t, cd.SetBy)
}

func TestPutSeededPolicies(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	for _, key := range []string{"*", "youtube.com"} {
		mark := h.auditMark()
		p := goodPolicy()
		p.Concurrency = 4
		var got api.FetchPolicy
		h.expect("PUT", "/admin/fetch-policies/"+key, h.admin, p, http.StatusOK, &got)
		assert.Equal(t, 4, got.Concurrency)
		h.expectOneAudit(mark, "fetch_policy.put", later, "fetch_policy:"+key)
	}
}

func (h *harness) corruptPolicy(key, set string) {
	h.t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(h.dir, "wn.db")+"?_pragma=busy_timeout(5000)")
	require.NoError(h.t, err)
	defer db.Close()
	_, err = db.Exec(`UPDATE fetch_policies SET `+set+` WHERE key = ?`, key)
	require.NoError(h.t, err)
}

// execDB runs a statement on the server's database behind its back.
func (h *harness) execDB(stmt string) {
	h.t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(h.dir, "wn.db")+"?_pragma=busy_timeout(5000)")
	require.NoError(h.t, err)
	defer db.Close()
	_, err = db.Exec(stmt)
	require.NoError(h.t, err)
}

func (h *harness) policies() map[string]api.FetchPolicy {
	h.t.Helper()
	var ps []api.FetchPolicy
	h.expect("GET", "/admin/fetch-policies", h.admin, nil, 200, &ps)
	m := map[string]api.FetchPolicy{}
	for _, p := range ps {
		m[p.Key] = p
	}
	return m
}

// TestInvalidFetchPolicyVisibleAndRepairable: a hand-corrupted row blocks
// its site, shows in GET as invalid, and a PUT repairs it (a DELETE removes
// it).
func TestInvalidFetchPolicyVisibleAndRepairable(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	client := h.client(submitURLOn("public"))
	did, dtok := h.downloader("", "", "public")
	h.fetchSubmit(client, "public", exURL)

	h.corruptPolicy("*", "rules = 'garbage'")
	h.srv.FetchPacingChanged()
	assert.Nil(t, h.claim(did, dtok, "", nil), "an invalid default blocks every site")
	p := h.policies()["*"]
	assert.True(t, p.Invalid)
	assert.NotEmpty(t, p.InvalidReason)
	assert.Equal(t, 2, p.Concurrency)
	assert.False(t, h.policies()["youtube.com"].Invalid)

	h.expect("PUT", "/admin/fetch-policies/*", h.admin, api.FetchPolicy{Concurrency: 2, BackoffInitial: "5m",
		BackoffCap: "6h"}, http.StatusOK, nil)
	assert.False(t, h.policies()["*"].Invalid)
	assert.NotNil(t, h.claim(did, dtok, "", nil), "repaired")

	h.corruptPolicy("youtube.com", "concurrency = 0, backoff_cap_s = 1")
	yt := h.policies()["youtube.com"]
	assert.True(t, yt.Invalid)
	assert.Equal(t, "1s", yt.BackoffCap)
	h.expect("DELETE", "/admin/fetch-policies/youtube.com", h.admin, nil, http.StatusNoContent, nil)
	_, ok := h.policies()["youtube.com"]
	assert.False(t, ok)
}

// TestPolicyAndCooldownChangesWakePacing: a claim blocked by a cooldown or a
// rate rule is woken by the admin change itself, with no other event.
func TestPolicyAndCooldownChangesWakePacing(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	client := h.client(submitURLOn("public"))
	did, dtok := h.downloader("a", "", "public")
	wait := testscale.D(8 * time.Second)
	waitClaim := func() chan *api.Lease {
		got := make(chan *api.Lease, 1)
		go func() { got <- h.claim(did, dtok, wait.String(), nil) }()
		require.Eventually(t, func() bool { return len(h.srv.waiters.keys()) > 0 },
			testscale.D(5*time.Second), 5*time.Millisecond, "the claim is waiting")
		return got
	}
	woken := func(got chan *api.Lease, what string) {
		t.Helper()
		select {
		case l := <-got:
			require.NotNil(t, l, what)
		case <-time.After(wait):
			t.Fatal("claim not woken: " + what)
		}
	}

	h.expect("POST", "/admin/fetch-cooldowns", h.admin, api.SetCooldownRequest{EgressKey: "label:a",
		Site: "example.com", Duration: "1h"}, http.StatusCreated, nil)
	var cds []api.FetchCooldown
	h.expect("GET", "/admin/fetch-cooldowns", h.admin, nil, 200, &cds)
	require.Len(t, cds, 1)
	h.fetchSubmit(client, "public", exURL)
	require.Nil(t, h.claim(did, dtok, "", nil))
	got := waitClaim()
	h.expect("DELETE", "/admin/fetch-cooldowns/"+escapeSeg(cds[0].EgressKey)+"/example.com", h.admin, nil,
		http.StatusNoContent, nil)
	woken(got, "cleared cooldown")

	// A rate rule: the second youtube fetch waits until the rule is loosened.
	h.fetchSubmit(client, "public", ytURL)
	l := h.claim(did, dtok, "", nil)
	require.NotNil(t, l)
	resp, b := h.putFetch(l, dtok, canonicalFLAC(t, speech(1, media.SampleRate)))
	require.Equal(t, http.StatusOK, resp.StatusCode, string(b))
	h.fetchSubmit(client, "public", ytURL+"x")
	require.Nil(t, h.claim(did, dtok, "", nil))
	got = waitClaim()
	h.expect("PUT", "/admin/fetch-policies/youtube.com", h.admin, api.FetchPolicy{Concurrency: 1,
		BackoffInitial: "1h", BackoffCap: "2h"}, http.StatusOK, nil)
	woken(got, "loosened policy")
}

func TestClearMissingCooldown404(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	mark := h.auditMark()
	resp, b := h.call("DELETE", "/admin/fetch-cooldowns/label%3Ahome/example.com", h.admin, nil)
	h.expectProblem(resp, b, http.StatusNotFound, api.ProblemNotFound)
	assert.Equal(t, mark, h.auditMark())
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
		{EgressKey: "label:home", Site: "example.com", Duration: "169h"},
		{EgressKey: "label:home", Site: "example.com", Duration: "9999999h"},
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
}

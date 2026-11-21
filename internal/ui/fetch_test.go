package ui

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/auth"
	"github.com/deweysasser/whisper-network/internal/store"
)

// downloader registers a downloader worker in egress group group.
func (e *env) downloader(host, group string) string {
	e.t.Helper()
	status, b := e.api("POST", "/worker", "", api.WorkerFacts{Hostname: host, OS: "linux", Arch: "amd64", Cores: 2,
		SandboxLevel: api.SandboxFull, Kind: api.WorkerDownloader, Kinds: []api.TaskKind{api.KindFetch},
		Slots: 1, Models: []string{}, EgressGroup: group, YtDlpVersion: "2026.03.17"})
	require.Equal(e.t, http.StatusCreated, status, string(b))
	id := jsonField(e.t, b, "id")
	require.NoError(e.t, e.st.SetWorkerEgressKey(e.ctx, id, "label:"+group))
	return id
}

func jsonField(t *testing.T, b []byte, name string) string {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m))
	s, _ := m[name].(string)
	return s
}

func (e *env) auditActions(prefix string) []store.AuditEntry {
	e.t.Helper()
	es, err := e.st.QueryAudit(e.ctx, store.AuditFilter{ActionPrefix: prefix})
	require.NoError(e.t, err)
	return es
}

func TestFetchPageAdminOnly(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.user("root", api.Grant{Role: api.RoleAdmin, Scope: "*"})
	e.user("qa", api.Grant{Role: api.RoleQueueAdmin, Scope: auth.QueueScope("public")})
	b := e.login("qa")
	resp, _ := b.get("/ui/fetch")
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	_, body := b.get("/ui/")
	assert.NotContains(t, body, `href="/ui/fetch"`)
	for _, p := range []string{"/ui/fetch/policies", "/ui/fetch/cooldowns", "/ui/fetch/cooldowns/clear", "/ui/fetch/policies/x/delete"} {
		resp, _ = b.post(p, url.Values{"key": {"x.com"}})
		assert.Equal(t, http.StatusForbidden, resp.StatusCode, p)
	}

	a := e.login("root")
	resp, body = a.get("/ui/fetch")
	require.Equal(t, http.StatusOK, resp.StatusCode, body)
	assert.Contains(t, body, `href="/ui/fetch"`)
	assert.Contains(t, body, "youtube.com")
	assert.Contains(t, body, "1/3m")
	assert.NotContains(t, body, "1/3m0s")
}

func TestFetchPolicyEditViaUI(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.user("root", api.Grant{Role: api.RoleAdmin, Scope: "*"})
	b := e.login("root")
	resp, body := b.post("/ui/fetch/policies", url.Values{"key": {"example.com"}, "rules": {"1/3m 20/1h"},
		"concurrency": {"2"}, "backoff_initial": {"10m"}, "backoff_cap": {"2h"}})
	require.Equal(t, http.StatusSeeOther, resp.StatusCode, body)
	assert.Equal(t, "/ui/fetch", resp.Header.Get("Location"))
	ps, err := e.st.FetchPolicies(e.ctx)
	require.NoError(t, err)
	var found bool
	for _, p := range ps {
		if p.Key == "example.com" {
			found = true
			require.Len(t, p.Rules, 2)
			assert.Equal(t, "20/1h", p.Rules[1].String())
			assert.Equal(t, 2, p.Concurrency)
		}
	}
	assert.True(t, found)
	require.Len(t, e.auditActions("fetch_policy.put"), 1)

	_, body = b.get("/ui/fetch")
	assert.Contains(t, body, "example.com")
	assert.Contains(t, body, "20/1h")

	// Egress-scoped key, then delete both.
	resp, _ = b.post("/ui/fetch/policies", url.Values{"key": {"example.com@label:home"}, "rules": {""},
		"concurrency": {"1"}, "backoff_initial": {"1m"}, "backoff_cap": {"1h"}})
	require.Equal(t, http.StatusSeeOther, resp.StatusCode)
	resp, _ = b.post("/ui/fetch/policies/example.com/delete", nil)
	require.Equal(t, http.StatusSeeOther, resp.StatusCode)
	require.Len(t, e.auditActions("fetch_policy.delete"), 1)
	resp, _ = b.post("/ui/fetch/policies/"+url.PathEscape("example.com@label:home")+"/delete", nil)
	require.Equal(t, http.StatusSeeOther, resp.StatusCode)

	// The default cannot be deleted, and has no delete button.
	resp, _ = b.post("/ui/fetch/policies/"+url.PathEscape("*")+"/delete", nil)
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	_, body = b.get("/ui/fetch")
	assert.NotContains(t, body, `action="/ui/fetch/policies/%2a/delete"`)
	assert.NotContains(t, body, `action="/ui/fetch/policies/*/delete"`)
}

func TestFetchPolicyBadRuleShowsError(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.user("root", api.Grant{Role: api.RoleAdmin, Scope: "*"})
	b := e.login("root")
	resp, body := b.post("/ui/fetch/policies", url.Values{"key": {"example.com"}, "rules": {"1/3m bogus"},
		"concurrency": {"2"}, "backoff_initial": {"10m"}, "backoff_cap": {"2h"}})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, body, "bogus")
	assert.Contains(t, body, `name="rules"`, "the page is re-rendered with the form")
	assert.Contains(t, body, "1/3m bogus", "the submitted rules are kept")
	assert.Empty(t, e.auditActions("fetch_policy.put"))

	for _, f := range []url.Values{
		{"key": {"example.com"}, "rules": {"1/3m"}, "concurrency": {"x"}, "backoff_initial": {"1m"}, "backoff_cap": {"1h"}},
		{"key": {"example.com"}, "rules": {"1/3m"}, "concurrency": {"1"}, "backoff_initial": {"zz"}, "backoff_cap": {"1h"}},
		{"key": {"example.com"}, "rules": {"1/3m"}, "concurrency": {"0"}, "backoff_initial": {"1m"}, "backoff_cap": {"1h"}},
		{"key": {"Not A Site!"}, "rules": {"1/3m"}, "concurrency": {"1"}, "backoff_initial": {"1m"}, "backoff_cap": {"1h"}},
	} {
		resp, _ = b.post("/ui/fetch/policies", f)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode, f.Encode())
	}
}

func TestFetchInvalidPolicyShownAndRepairable(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.user("root", api.Grant{Role: api.RoleAdmin, Scope: "*"})
	db, err := sql.Open("sqlite", e.dbPath)
	require.NoError(t, err)
	defer db.Close()
	_, err = db.ExecContext(e.ctx, `INSERT INTO fetch_policies(key, site, egress_key, rules, concurrency,
		backoff_initial_s, backoff_cap_s, updated_by, updated_at) VALUES ('broken.com','broken.com',NULL,'["junk"]',1,60,3600,'hand',0)`)
	require.NoError(t, err)
	b := e.login("root")
	_, body := b.get("/ui/fetch")
	assert.Contains(t, body, "broken.com")
	assert.Contains(t, body, "invalid")
	assert.Contains(t, body, "junk")
	assert.Contains(t, body, "/ui/fetch/policies/broken.com/delete")
	resp, _ := b.post("/ui/fetch/policies", url.Values{"key": {"broken.com"}, "rules": {"1/1m"},
		"concurrency": {"1"}, "backoff_initial": {"1m"}, "backoff_cap": {"1h"}})
	require.Equal(t, http.StatusSeeOther, resp.StatusCode)
	_, body = b.get("/ui/fetch")
	assert.NotContains(t, body, "junk")
}

func TestCooldownSetClearViaUI(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.user("root", api.Grant{Role: api.RoleAdmin, Scope: "*"})
	e.downloader("dl-host", "home")
	b := e.login("root")
	_, body := b.get("/ui/fetch")
	assert.Contains(t, body, "label:home", "egress choices come from downloaders")

	resp, body := b.post("/ui/fetch/cooldowns", url.Values{"egress_key": {"label:home"}, "site": {"youtube.com"},
		"duration": {"2h"}, "reason": {"429s"}})
	require.Equal(t, http.StatusSeeOther, resp.StatusCode, body)
	cs, err := e.st.FetchCooldowns(e.ctx, e.clk.Now())
	require.NoError(t, err)
	require.Len(t, cs, 1)
	assert.Equal(t, e.clk.Now().Add(2*60*60*1e9), cs[0].Until)
	require.Len(t, e.auditActions("fetch_cooldown.set"), 1)

	_, body = b.get("/ui/fetch")
	assert.Contains(t, body, "youtube.com")
	assert.Contains(t, body, "429s")

	resp, _ = b.post("/ui/fetch/cooldowns/clear", url.Values{"egress_key": {"label:home"}, "site": {"youtube.com"}})
	require.Equal(t, http.StatusSeeOther, resp.StatusCode)
	cs, _ = e.st.FetchCooldowns(e.ctx, e.clk.Now())
	assert.Empty(t, cs)
	require.Len(t, e.auditActions("fetch_cooldown.clear"), 1)

	for _, d := range []string{"0s", "-1h", "169h", "soon"} {
		resp, _ = b.post("/ui/fetch/cooldowns", url.Values{"egress_key": {"label:home"}, "site": {"youtube.com"}, "duration": {d}})
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode, d)
	}
	resp, _ = b.post("/ui/fetch/cooldowns", url.Values{"egress_key": {""}, "site": {"youtube.com"}, "duration": {"1h"}})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestWorkerPageShowsDownloaderFacts(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.user("root", api.Grant{Role: api.RoleAdmin, Scope: "*"})
	id := e.downloader("dl-host", "home")
	b := e.login("root")
	_, body := b.get("/ui/workers/" + id)
	for _, s := range []string{"downloader", "label:home", "2026.03.17", "Approve to download for queues"} {
		assert.Contains(t, body, s)
	}
	_, body = b.get("/ui/workers")
	for _, s := range []string{"downloader", "label:home", "2026.03.17"} {
		assert.Contains(t, body, s)
	}
}

func TestApproveDownloaderViaUI(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.user("root", api.Grant{Role: api.RoleAdmin, Scope: "*"})
	id := e.downloader("dl-host", "home")
	b := e.login("root")
	resp, body := b.post("/ui/workers/"+id+"/approve", url.Values{"queue": {"public"}})
	require.Equal(t, http.StatusSeeOther, resp.StatusCode, body)
	g, err := e.srv.Ops().WireGrants(e.ctx, "worker:"+id)
	require.NoError(t, err)
	assert.Contains(t, g, api.Grant{Role: api.RoleFetch, Scope: "queue:public"})
	_, body = b.get("/ui/workers/" + id)
	assert.Contains(t, body, "public", "the worker page lists its fetch grants as queues")
	_, body = b.get("/ui/workers")
	assert.Contains(t, body, `<span class="chip">public</span>`)
}

func TestGrantSubmitURLViaUI(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.user("root", api.Grant{Role: api.RoleAdmin, Scope: "*"})
	e.user("alice")
	b := e.login("root")
	_, body := b.get("/ui/users")
	assert.Contains(t, body, "<option>submit_url</option>")
	_, body = b.get("/ui/queues/public")
	assert.Contains(t, body, "<option>submit_url</option>")
	resp, body := b.post("/ui/grants", url.Values{"principal": {"user:alice"}, "role": {"submit_url"},
		"scope": {"queue:public"}, "back": {"/ui/queues/public"}})
	require.Equal(t, http.StatusSeeOther, resp.StatusCode, body)
	_, body = b.get("/ui/queues/public")
	assert.Contains(t, body, "submit_url")
	_, body = b.get("/ui/clients")
	assert.Contains(t, body, "Submit URL")
}

func TestQueueFilterFetching(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.user("root", api.Grant{Role: api.RoleAdmin, Scope: "*"})
	b := e.login("root")
	resp, body := b.get("/ui/queues/public?state=fetching")
	require.Equal(t, http.StatusOK, resp.StatusCode, body)
	assert.Contains(t, body, `<option value="fetching" selected>`)
	_, body = b.get("/ui/")
	assert.Contains(t, body, "Fetching")
}

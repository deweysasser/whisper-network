package ui

import (
	"fmt"
	"html"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/auth"
	"github.com/deweysasser/whisper-network/internal/store"
)

func roles(ris []roleInfo) []api.Role {
	out := make([]api.Role, len(ris))
	for i, r := range ris {
		out[i] = r.Role
	}
	return out
}

// TestGrantMatrixShape: a row per queue, a column per role, offered roles
// depend on who receives them, and cells the granter can't give are disabled.
func TestGrantMatrixShape(t *testing.T) {
	queues := []store.Queue{{Name: "public"}, {Name: "meetings"}, {Name: "secret"}}
	admin := auth.Principal{Kind: auth.KindUser, ID: "1", Grants: []api.Grant{{Role: api.RoleAdmin, Scope: "*"}}}

	t.Run("admin granting to a client", func(t *testing.T) {
		m := newGrantMatrix(admin, auth.KindClient, queues, auth.CanDelegateAtCreation)
		assert.Equal(t, []api.Role{api.RoleSubmit, api.RoleSubmitURL, api.RoleViewAll, api.RoleReadAll, api.RoleDeleteAll}, roles(m.Columns),
			"clients are not offered queue_admin")
		require.Len(t, m.Global, 1)
		assert.Equal(t, api.RoleClientAdmin, m.Global[0].Role, "clients are offered only client_admin globally")
		require.Len(t, m.Rows, 3)
		assert.Equal(t, "meetings", m.Rows[1].Queue)
		for _, r := range m.Rows {
			require.Len(t, r.Cells, 5)
			for _, c := range r.Cells {
				assert.True(t, c.Allowed)
			}
		}
		assert.Equal(t, "view_all queue:meetings", m.Rows[1].Cells[2].Value)
		assert.Equal(t, "read_all queue:meetings", m.Rows[1].Cells[3].Value)
		assert.True(t, m.Any())
	})

	t.Run("admin granting to a user", func(t *testing.T) {
		m := newGrantMatrix(admin, auth.KindUser, queues, auth.CanGrantTo)
		assert.Equal(t, []api.Role{api.RoleQueueAdmin, api.RoleSubmit, api.RoleSubmitURL, api.RoleViewAll, api.RoleReadAll, api.RoleDeleteAll}, roles(m.Columns))
		assert.Equal(t, []api.Role{api.RoleAdmin, api.RoleWorkerAdmin, api.RoleClientAdmin}, roles(globalsOf(m)))
	})

	t.Run("queue admin sees only its queue, nothing global", func(t *testing.T) {
		qa := auth.Principal{Kind: auth.KindUser, ID: "2", Grants: []api.Grant{
			{Role: api.RoleQueueAdmin, Scope: auth.QueueScope("meetings")},
		}}
		m := newGrantMatrix(qa, auth.KindClient, queues, auth.CanDelegateAtCreation)
		assert.Empty(t, m.Global)
		require.Len(t, m.Rows, 1, "queues with nothing grantable are left out")
		assert.Equal(t, "meetings", m.Rows[0].Queue)
		assert.Equal(t, []api.Role{api.RoleSubmit, api.RoleSubmitURL, api.RoleViewAll, api.RoleDeleteAll}, roles(m.Columns),
			"a queue admin cannot grant read_all, so the column is not shown")
	})

	t.Run("read_all column shown when held, even if the editor cannot change it", func(t *testing.T) {
		qa := auth.Principal{Kind: auth.KindUser, ID: "2", Grants: []api.Grant{
			{Role: api.RoleQueueAdmin, Scope: auth.QueueScope("meetings")},
		}}
		held := []api.Grant{{Role: api.RoleReadAll, Scope: auth.QueueScope("meetings")}}
		m := editGrantMatrix(qa, auth.KindClient, queues, held)
		assert.Equal(t, []api.Role{api.RoleSubmit, api.RoleSubmitURL, api.RoleViewAll, api.RoleReadAll, api.RoleDeleteAll}, roles(m.Columns))
		require.Len(t, m.Rows, 1)
		assert.Equal(t, []bool{true, true, true, false, true}, allowed(m.Rows[0].Cells))
		assert.True(t, m.Rows[0].Cells[3].Checked)
	})

	t.Run("client_admin holding read_all may re-delegate it at creation", func(t *testing.T) {
		holder := auth.Principal{Kind: auth.KindClient, ID: "c", Grants: []api.Grant{
			{Role: api.RoleClientAdmin, Scope: "*"},
			{Role: api.RoleReadAll, Scope: auth.QueueScope("public")},
		}}
		m := newGrantMatrix(holder, auth.KindClient, queues, auth.CanDelegateAtCreation)
		assert.Equal(t, []api.Role{api.RoleSubmit, api.RoleSubmitURL, api.RoleViewAll, api.RoleReadAll, api.RoleDeleteAll}, roles(m.Columns))
		require.Len(t, m.Rows, 1)
		assert.Equal(t, []bool{false, false, false, true, false}, allowed(m.Rows[0].Cells))
	})

	ca := auth.Principal{Kind: auth.KindClient, ID: "c", Grants: []api.Grant{
		{Role: api.RoleClientAdmin, Scope: "*"},
		{Role: api.RoleSubmit, Scope: auth.QueueScope("public")},
	}}
	t.Run("creating a client: held grants may be re-delegated", func(t *testing.T) {
		m := newGrantMatrix(ca, auth.KindClient, queues, auth.CanDelegateAtCreation)
		require.Len(t, m.Rows, 1)
		assert.Equal(t, []bool{true, false, false, false}, allowed(m.Rows[0].Cells))
		require.Len(t, m.Global, 1, "client_admin may hand on client_admin at creation")
	})

	t.Run("editing an existing client: holding is not enough", func(t *testing.T) {
		held := []api.Grant{{Role: api.RoleSubmit, Scope: auth.QueueScope("public")}}
		m := editGrantMatrix(ca, auth.KindClient, queues, held)
		assert.Empty(t, m.Global, "client_admin cannot grant client_admin to an existing client")
		require.Len(t, m.Rows, 1, "the held grant stays visible")
		assert.Equal(t, []bool{false, false, false, false}, allowed(m.Rows[0].Cells))
		assert.True(t, m.Rows[0].Cells[0].Checked)
	})

	t.Run("queue admin editing a user cannot touch queue_admin", func(t *testing.T) {
		qa := auth.Principal{Kind: auth.KindUser, ID: "2", Grants: []api.Grant{
			{Role: api.RoleQueueAdmin, Scope: auth.QueueScope("meetings")},
		}}
		m := editGrantMatrix(qa, auth.KindUser, queues, nil)
		require.Len(t, m.Rows, 1)
		assert.Equal(t, []bool{false, true, true, true, true}, allowed(m.Rows[0].Cells),
			"queue_admin column is not grantable by a queue admin")
	})

	t.Run("nothing grantable", func(t *testing.T) {
		m := newGrantMatrix(auth.Principal{Kind: auth.KindUser, ID: "3"}, auth.KindClient, queues, auth.CanDelegateAtCreation)
		assert.False(t, m.Any())
	})
}

func globalsOf(m grantMatrix) []roleInfo {
	out := make([]roleInfo, len(m.Global))
	for i, g := range m.Global {
		out[i] = g.roleInfo
	}
	return out
}

func allowed(cs []grantCell) []bool {
	out := make([]bool, len(cs))
	for i, c := range cs {
		out[i] = c.Allowed
	}
	return out
}

// TestClientFormIsAMatrix: the clients page renders the matrix (one row per
// queue, labelled checkboxes), and ticking cells across two queues mints a
// client with exactly those grants.
func TestClientFormIsAMatrix(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.queue("meetings")
	e.user("root", api.Grant{Role: api.RoleAdmin, Scope: "*"})
	b := e.login("root")

	_, body := b.get("/ui/clients")
	assert.Contains(t, body, `class="data grant-matrix"`)
	assert.Regexp(t, regexp.MustCompile(`<th scope="row">meetings</th>`), body)
	assert.Contains(t, body, `aria-label="View all on meetings"`)
	assert.NotContains(t, body, `value="queue_admin queue:public"`, "clients are not offered queue_admin")

	resp, body := b.post("/ui/clients", url.Values{"name": {"matrix"},
		"grant": {"submit queue:public", "view_all queue:meetings", "delete_all queue:meetings"}})
	require.Equal(t, http.StatusCreated, resp.StatusCode, body)

	cs, err := e.st.ListClients(e.ctx)
	require.NoError(t, err)
	var id string
	for _, c := range cs {
		if c.Name == "matrix" {
			id = c.ID
		}
	}
	require.NotEmpty(t, id)
	gs, err := e.st.GrantsFor(e.ctx, store.ClientPrincipal(id))
	require.NoError(t, err)
	assert.Len(t, gs, 3)
}

// TestGrantChanges: removals are limited to what the editor could give;
// additions are everything newly ticked (authorized separately).
func TestGrantChanges(t *testing.T) {
	qa := auth.Principal{Kind: auth.KindUser, ID: "1", Grants: []api.Grant{
		{Role: api.RoleQueueAdmin, Scope: auth.QueueScope("meetings")},
	}}
	submitM := api.Grant{Role: api.RoleSubmit, Scope: auth.QueueScope("meetings")}
	viewM := api.Grant{Role: api.RoleViewAll, Scope: auth.QueueScope("meetings")}
	submitP := api.Grant{Role: api.RoleSubmit, Scope: auth.QueueScope("public")}

	add, remove := grantChanges(qa, []api.Grant{submitM, submitP}, []api.Grant{viewM})
	assert.Equal(t, []api.Grant{viewM}, add)
	assert.Equal(t, []api.Grant{submitM}, remove, "submit on public is outside the editor's authority and is kept")

	add, remove = grantChanges(qa, []api.Grant{submitM}, []api.Grant{submitM, submitM})
	assert.Empty(t, add, "duplicates and already-held grants add nothing")
	assert.Empty(t, remove)

	// A mere holder can no longer strip what it holds from others.
	holder := auth.Principal{Kind: auth.KindClient, ID: "c", Grants: []api.Grant{
		{Role: api.RoleClientAdmin, Scope: "*"}, submitP,
	}}
	_, remove = grantChanges(holder, []api.Grant{submitP}, nil)
	assert.Empty(t, remove, "holding submit on public does not authorize revoking it")
}

// TestEditClientNeedsAdministrativeRight: a client_admin that merely holds
// a grant can create clients with it, but cannot add it to or remove it
// from an existing client, nor revoke a client more privileged than itself.
func TestEditClientNeedsAdministrativeRight(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.queue("meetings")
	e.user("root", api.Grant{Role: api.RoleAdmin, Scope: "*"})
	e.user("ca", api.Grant{Role: api.RoleClientAdmin, Scope: "*"}, api.Grant{Role: api.RoleSubmit, Scope: "queue:public"})
	root, ca := e.login("root"), e.login("ca")

	resp, body := ca.post("/ui/clients", url.Values{"name": {"mine"}, "grant": {"submit queue:public"}})
	require.Equal(t, http.StatusCreated, resp.StatusCode, body)
	mine := clientID(t, e, "mine")
	resp, body = root.post("/ui/clients", url.Values{"name": {"bigger"},
		"grant": {"submit queue:public", "view_all queue:meetings"}})
	require.Equal(t, http.StatusCreated, resp.StatusCode, body)
	bigger := clientID(t, e, "bigger")

	// The edit page shows the held grant ticked but not changeable.
	resp, body = ca.get("/ui/clients/" + mine)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NotContains(t, body, `name="grant" value="submit queue:public"`)
	assert.Contains(t, body, `(held; you cannot change it)`)

	// Removing it by submitting an empty form changes nothing...
	resp, body = ca.post("/ui/clients/"+mine, url.Values{})
	require.Equal(t, http.StatusOK, resp.StatusCode, body)
	assert.Equal(t, []string{"submit queue:public"}, grantSet(t, e, mine))
	// ...and adding a held grant to another client is refused.
	resp, _ = ca.post("/ui/clients/"+bigger, url.Values{"grant": {"submit queue:public", "view_all queue:meetings"}})
	assert.Equal(t, http.StatusOK, resp.StatusCode, "no change requested for held grants")
	resp, _ = ca.post("/ui/grants", url.Values{"principal": {"client:" + bigger}, "role": {"submit"},
		"scope": {"queue:public"}})
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	resp, _ = ca.post("/ui/grants/revoke", url.Values{"principal": {"client:" + bigger}, "role": {"submit"},
		"scope": {"queue:public"}})
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.ElementsMatch(t, []string{"submit queue:public", "view_all queue:meetings"}, grantSet(t, e, bigger))

	// Revoking: only the client ca could have created.
	_, body = ca.get("/ui/clients")
	assert.Contains(t, body, `action="/ui/clients/`+mine+`/revoke"`)
	assert.NotContains(t, body, `action="/ui/clients/`+bigger+`/revoke"`)
	resp, _ = ca.post("/ui/clients/"+bigger+"/revoke", nil)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	resp, _ = ca.post("/ui/clients/"+mine+"/revoke", nil)
	assert.Equal(t, http.StatusSeeOther, resp.StatusCode)
}

// TestEditClientAuditedOnce: one client edit records exactly one audit entry
// per grant change, by the editor.
func TestEditClientAuditedOnce(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.queue("meetings")
	e.user("root", api.Grant{Role: api.RoleAdmin, Scope: "*"})
	b := e.login("root")
	resp, body := b.post("/ui/clients", url.Values{"name": {"svc"}, "grant": {"submit queue:public"}})
	require.Equal(t, http.StatusCreated, resp.StatusCode, body)
	id := clientID(t, e, "svc")
	resp, body = b.post("/ui/clients/"+id, url.Values{"grant": {"view_all queue:meetings"}})
	require.Equal(t, http.StatusOK, resp.StatusCode, body)
	es, err := e.st.QueryAudit(e.ctx, store.AuditFilter{Target: "client:" + id, ActionPrefix: "grant."})
	require.NoError(t, err)
	var got []string
	for _, a := range es {
		assert.Equal(t, "user:root", a.Actor)
		got = append(got, a.Action+" "+a.Detail)
	}
	assert.ElementsMatch(t, []string{
		`grant.add {"role":"view_all","scope":"queue:meetings"}`,
		`grant.remove {"role":"submit","scope":"queue:public"}`,
	}, got)
}

func clientID(t *testing.T, e *env, name string) string {
	t.Helper()
	cs, err := e.st.ListClients(e.ctx)
	require.NoError(t, err)
	for _, c := range cs {
		if c.Name == name {
			return c.ID
		}
	}
	t.Fatalf("no client %q", name)
	return ""
}

func grantSet(t *testing.T, e *env, id string) []string {
	t.Helper()
	c, err := e.st.ClientByID(e.ctx, id)
	require.NoError(t, err)
	ac, err := e.srv.Ops().ClientAPI(e.ctx, c) // wire form: scopes by queue name
	require.NoError(t, err)
	var out []string
	for _, g := range ac.Grants {
		out = append(out, string(g.Role)+" "+g.Scope)
	}
	return out
}

// TestEditClientPermissions: an admin changes an existing client's grants;
// the token keeps working and immediately carries the new permissions; the
// change is audited.
func TestEditClientPermissions(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.queue("meetings")
	e.user("root", api.Grant{Role: api.RoleAdmin, Scope: "*"})
	b := e.login("root")

	resp, body := b.post("/ui/clients", url.Values{"name": {"svc"}, "grant": {"submit queue:public"}})
	require.Equal(t, http.StatusCreated, resp.StatusCode, body)
	token := tokenRE.FindString(body)
	id := clientID(t, e, "svc")

	_, body = b.get("/ui/clients")
	assert.Contains(t, body, `href="/ui/clients/`+id+`"`)
	resp, body = b.get("/ui/clients/" + id)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Regexp(t, regexp.MustCompile(`value="submit queue:public" checked`), body, "current grants are pre-ticked")

	status, _ := e.api("GET", "/queue/meetings", token, nil)
	require.Equal(t, http.StatusForbidden, status, "no access to meetings yet")

	resp, body = b.post("/ui/clients/"+id, url.Values{"grant": {"view_all queue:meetings", "submit queue:meetings"}})
	require.Equal(t, http.StatusOK, resp.StatusCode, body)
	assert.Contains(t, body, "2 added, 1 removed")
	assert.ElementsMatch(t, []string{"view_all queue:meetings", "submit queue:meetings"}, grantSet(t, e, id))

	status, _ = e.api("GET", "/queue/meetings", token, nil)
	assert.Equal(t, http.StatusOK, status, "same token, new permission")
	status, _ = e.api("GET", "/queue/public", token, nil)
	assert.Equal(t, http.StatusForbidden, status, "removed permission no longer works")

	entries, err := e.st.ListAudit(e.ctx, 0, 50)
	require.NoError(t, err)
	var adds, removes int
	for _, a := range entries {
		switch a.Action {
		case "grant.add":
			adds++
		case "grant.remove":
			removes++
		}
	}
	assert.GreaterOrEqual(t, adds, 2)
	assert.GreaterOrEqual(t, removes, 1)
}

// TestAuditPageFilters: the audit page filters with the API's parameters
// and keeps the filter when paging.
func TestAuditPageFilters(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.user("root", api.Grant{Role: api.RoleAdmin, Scope: "*"})
	e.user("bob")
	for i := range auditPageSize + 2 {
		e.queue(fmt.Sprintf("q%02d", i))
	}
	b := e.login("root")
	resp, _ := b.post("/ui/grants", url.Values{"principal": {"user:bob"}, "role": {"submit"}, "scope": {"queue:q00"}})
	require.Equal(t, http.StatusSeeOther, resp.StatusCode)

	_, body := b.get("/ui/audit?action=grant.&actor=user:root")
	assert.Equal(t, 1, strings.Count(body, "<tr><td>"), body)
	assert.Contains(t, body, "grant.add")
	assert.Contains(t, body, `value="grant."`, "the filter is echoed into the form")

	_, body = b.get("/ui/audit?action=queue.")
	m := regexp.MustCompile(`href="(/ui/audit\?[^"]+)"`).FindAllStringSubmatch(body, -1)
	require.NotEmpty(t, m)
	next := html.UnescapeString(m[len(m)-1][1])
	assert.Contains(t, next, "action=queue.")
	_, body = b.get(next)
	// 52 queues plus the bootstrap's public queue, 50 per page.
	assert.Equal(t, 3, strings.Count(body, "<tr><td>"))
	assert.NotContains(t, body, "grant.add")

	resp, _ = b.get("/ui/audit?since=yesterday")
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

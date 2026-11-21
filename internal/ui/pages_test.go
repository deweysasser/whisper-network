package ui

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/auth"
)

// fleet is a populated server: two queues, two pending workers, a job on
// public, and four users of decreasing privilege.
type fleet struct {
	*env
	w1, w2   string // worker ids
	job      api.Job
	submitTo string // client token with submit on public
}

func newFleet(t *testing.T) *fleet {
	t.Helper()
	f := &fleet{env: newEnv(t)}
	f.queue("other")
	f.w1, _ = f.worker("w1-host", "first worker")
	f.w2, _ = f.worker("w2-host", "second worker")
	f.user("root", api.Grant{Role: api.RoleAdmin, Scope: "*"})
	f.user("qa", api.Grant{Role: api.RoleQueueAdmin, Scope: auth.QueueScope("other")})
	f.user("wa", api.Grant{Role: api.RoleWorkerAdmin, Scope: auth.WorkerScope(f.w1)})
	f.user("plain")
	f.submitTo = f.client(api.Grant{Role: api.RoleSubmit, Scope: auth.QueueScope("public")})
	f.job = f.submit(f.submitTo, "public", "board-meeting")
	return f
}

type pageCase struct {
	status     int
	has, lacks []string
}

// TestPagesByPrincipal renders every page as each kind of principal: the
// admin sees everything; less privileged users get 403 for whole pages
// they may not open, 404 for jobs they may not see, and pages without the
// controls they may not use.
func TestPagesByPrincipal(t *testing.T) {
	t.Parallel()
	f := newFleet(t)
	jobPage := "/ui/jobs/" + f.job.ID
	w1Page, w2Page := "/ui/workers/"+f.w1, "/ui/workers/"+f.w2
	pages := map[string]map[string]pageCase{
		"root": {
			"/ui/":                {200, []string{"audio-hours", "Fleet share", `href="/ui/users"`, `href="/ui/audit"`}, nil},
			"/ui/dashboard/stats": {200, []string{`id="stats"`}, []string{"<html"}},
			"/ui/workers": {200, []string{"first worker", "w1-host", "second worker", `href="` + w1Page + `"`,
				`href="` + w1Page + `#approve"`, "Approve"}, []string{`name="queue"`, "Reject"}},
			w1Page:                       {200, []string{"first worker", "w1-host", `value="public"`, `value="other"`, "Reject"}, nil},
			"/ui/workers/no-such-worker": {404, nil, nil},
			"/ui/queues":                 {200, []string{"Create a queue", "public", "other"}, nil},
			"/ui/queues/public":          {200, []string{`name="weight"`, `name="description"`, "board-meeting", "Access"}, nil},
			jobPage:                      {200, []string{"board-meeting", "Delete job"}, []string{"Download transcript"}},
			"/ui/users":                  {200, []string{"Create a user", "qa", "plain"}, nil},
			"/ui/clients":                {200, []string{"Mint a client token"}, nil},
			"/ui/audit":                  {200, []string{"user.create"}, nil},
			"/ui/fetch":                  {200, []string{"Policies", "Set a cooldown", "youtube.com", `name="rules"`, `href="/ui/fetch"`}, []string{"1/3m0s"}},
			"/ui/password":               {200, []string{"Change password"}, nil},
			"/ui/jobs/no-such-job":       {404, nil, nil},
			"/ui/queues/no-such-q":       {404, nil, nil},
			"/ui/no/such/page/here":      {404, nil, nil},
		},
		"qa": { // queue_admin on "other" only
			"/ui/":              {200, []string{"audio-hours", `href="/ui/workers"`}, []string{`href="/ui/users"`, `href="/ui/audit"`}},
			"/ui/workers":       {200, []string{"w1-host", "w2-host", `href="` + w1Page + `"`}, []string{"Approve", "Pause", "Reject"}},
			w1Page:              {200, []string{"w1-host"}, []string{"Approve", "Pause", "Reject", `name="queue"`}},
			"/ui/queues":        {200, []string{"public", "other"}, []string{"Create a queue"}},
			"/ui/queues/public": {200, []string{"no access to this queue"}, []string{`name="weight"`, `name="description"`, "board-meeting", "Access"}},
			"/ui/queues/other":  {200, []string{`name="description"`, "only an admin can change it", "Access"}, []string{`name="weight"`}},
			jobPage:             {404, nil, []string{"board-meeting"}},
			"/ui/users":         {403, nil, nil},
			"/ui/clients":       {403, nil, nil},
			"/ui/audit":         {403, nil, nil},
			"/ui/fetch":         {403, nil, nil},
		},
		"wa": { // worker_admin on w1 only
			"/ui/":              {200, []string{"audio-hours"}, nil},
			"/ui/workers":       {200, []string{"w1-host", "Approve"}, []string{"w2-host", `name="queue"`}},
			w1Page:              {200, []string{"w1-host", "Approve", "Reject"}, []string{`name="queue"`}},
			w2Page:              {404, nil, []string{"w2-host"}},
			"/ui/queues/public": {200, nil, []string{`name="description"`, "board-meeting"}},
			jobPage:             {404, nil, nil},
			"/ui/users":         {403, nil, nil},
			"/ui/clients":       {403, nil, nil},
			"/ui/audit":         {403, nil, nil},
			"/ui/fetch":         {403, nil, nil},
		},
		"plain": { // no roles at all
			"/ui/":                {200, []string{"no access to any queue"}, []string{"audio-hours", `href="/ui/workers"`}},
			"/ui/dashboard/stats": {403, nil, nil},
			"/ui/workers":         {403, nil, nil},
			w1Page:                {404, nil, []string{"w1-host"}},
			"/ui/queues":          {200, []string{"public"}, []string{"Create a queue", "view jobs"}},
			"/ui/queues/public":   {200, []string{"no access to this queue"}, []string{"board-meeting", `name="description"`, "Access"}},
			jobPage:               {404, nil, nil},
			"/ui/users":           {403, nil, nil},
			"/ui/clients":         {403, nil, nil},
			"/ui/audit":           {403, nil, nil},
			"/ui/fetch":           {403, nil, nil},
		},
	}
	for user, cases := range pages {
		b := f.login(user)
		for path, c := range cases {
			resp, body := b.get(path)
			require.Equal(t, c.status, resp.StatusCode, "%s %s: %s", user, path, body)
			for _, s := range c.has {
				assert.Contains(t, body, s, "%s %s", user, path)
			}
			for _, s := range c.lacks {
				assert.NotContains(t, body, s, "%s %s", user, path)
			}
		}
	}
}

// TestHiddenActionsAreRejected checks that the controls missing from the
// pages above are refused server-side too.
func TestHiddenActionsAreRejected(t *testing.T) {
	t.Parallel()
	f := newFleet(t)
	type post struct {
		path   string
		form   map[string]string
		status int
	}
	jobDelete := "/ui/jobs/" + f.job.ID + "/delete"
	cases := map[string][]post{
		"plain": {
			{"/ui/queues", map[string]string{"name": "mine"}, 403},
			{"/ui/queues/public", map[string]string{"description": "x"}, 403},
			{jobDelete, nil, 404},
			{"/ui/workers/" + f.w1 + "/approve", nil, 403},
			{"/ui/workers/" + f.w1 + "/reject", nil, 403},
			{"/ui/workers/" + f.w1 + "/delete", nil, 403},
			{"/ui/workers/delete-rejected", nil, 403},
			{"/ui/users", map[string]string{"username": "eve"}, 403},
			{"/ui/users/qa/disable", nil, 403},
			{"/ui/clients", map[string]string{"name": "x"}, 403},
			{"/ui/grants", map[string]string{"principal": "user:plain", "role": "admin", "scope": "*"}, 403},
		},
		"qa": {
			{"/ui/queues/public", map[string]string{"description": "x"}, 403},
			{"/ui/queues/other", map[string]string{"weight": "500"}, 403},
			{jobDelete, nil, 404},
			{"/ui/workers/" + f.w1 + "/pause", nil, 403},
			{"/ui/workers/" + f.w1 + "/delete", nil, 403},
			{"/ui/workers/delete-rejected", nil, 403},
			{"/ui/grants", map[string]string{"principal": "user:plain", "role": "submit", "scope": "queue:public"}, 403},
			{"/ui/grants", map[string]string{"principal": "user:qa", "role": "queue_admin", "scope": "queue:public"}, 403},
		},
		"wa": {
			{"/ui/workers/" + f.w2 + "/approve", nil, 403},
			{"/ui/workers/" + f.w2 + "/reject", nil, 403},
			{"/ui/workers/" + f.w2 + "/delete", nil, 403},
			{"/ui/workers/" + f.w1 + "/approve", map[string]string{"queue": "public"}, 403},
		},
	}
	for user, posts := range cases {
		b := f.login(user)
		for _, p := range posts {
			form := map[string][]string{}
			for k, v := range p.form {
				form[k] = []string{v}
			}
			resp, body := b.post(p.path, form)
			assert.Equal(t, p.status, resp.StatusCode, "%s POST %s: %s", user, p.path, body)
		}
	}
	// Nothing changed.
	q, err := f.st.QueueByName(f.ctx, "other")
	require.NoError(t, err)
	assert.Equal(t, 100, q.Weight)
	_, err = f.st.Job(f.ctx, f.job.ID)
	assert.NoError(t, err)
	for _, id := range []string{f.w1, f.w2} {
		wk, err := f.st.WorkerByID(f.ctx, id)
		require.NoError(t, err)
		assert.Equal(t, api.WorkerPending, wk.State)
	}
}

// TestXSSEscaped: labels and descriptions are attacker-controlled (any
// client or registering worker sets them) and must render as text.
func TestXSSEscaped(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.user("root", api.Grant{Role: api.RoleAdmin, Scope: "*"})
	const evil = `<script>alert("x")</script><img src=x onerror=alert(1)>`
	tok := e.client(api.Grant{Role: api.RoleSubmit, Scope: "queue:public"})
	j := e.submit(tok, "public", evil)
	b := e.login("root")
	wid, _ := e.worker(evil, evil)
	for _, p := range []string{"/ui/workers", "/ui/workers/" + wid, "/ui/queues/public", "/ui/jobs/" + j.ID} {
		resp, body := b.get(p)
		require.Equal(t, http.StatusOK, resp.StatusCode, p)
		assert.NotContains(t, body, `<script>alert`, p)
		assert.NotContains(t, body, `<img src=x`, p)
		assert.Contains(t, body, `&lt;script&gt;alert(&#34;x&#34;)&lt;/script&gt;`, p)
	}
}

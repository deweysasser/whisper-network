package auth

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
)

func TestValidateGrant(t *testing.T) {
	ok := []api.Grant{admin, caAll, waAll, waW1, qaA, submitA, viewA, readA, deleteA, processA}
	for _, gr := range ok {
		assert.NoError(t, ValidateGrant(gr), "%v", gr)
	}
	bad := []api.Grant{
		g(api.RoleAdmin, "queue:A"), g(api.RoleClientAdmin, "queue:A"),
		g(api.RoleSubmit, "*"), g(api.RoleQueueAdmin, "*"), g(api.RoleReadAll, "*"), g(api.RoleProcess, "worker:w1"),
		g(api.RoleWorkerAdmin, "queue:A"), g(api.RoleWorkerAdmin, "worker:"),
		g(api.RoleSubmit, "queue:"), g(api.RoleSubmit, ""), g("root", "*"), g("", ""),
	}
	for _, gr := range bad {
		err := ValidateGrant(gr)
		require.Error(t, err, "%v", gr)
		assert.ErrorIs(t, err, ErrForbidden)
	}
}

// grantCase is one row of the grant-rule matrices.
type grantCase struct {
	name  string
	actor Principal
	req   []api.Grant
	ok    bool
}

func runGrantCases(t *testing.T, check func(Principal, []api.Grant) error, tests []grantCase) {
	t.Helper()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := check(tc.actor, tc.req)
			if tc.ok {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrForbidden)
		})
	}
}

// commonGrantCases hold under both rules: CanDelegateAtCreation only adds
// re-delegation of held grants.
var commonGrantCases = []grantCase{
	{"admin anything", user(admin), []api.Grant{admin, qaA, waAll, caAll, submitA, processA}, true},
	{"admin grants read_all", user(admin), []api.Grant{readA}, true},
	{"queue_admin cannot grant read_all on its queue", user(qaA), []api.Grant{readA}, false},
	{"client_admin cannot hand out unheld read_all", client(caAll), []api.Grant{readA}, false},
	{"worker_admin cannot grant read_all", user(waAll), []api.Grant{readA}, false},
	{"admin empty request", user(admin), nil, true},
	{"admin malformed", user(admin), []api.Grant{g(api.RoleSubmit, "*")}, false},
	{"client_admin cannot grant admin", client(caAll), []api.Grant{admin}, false},
	{"client_admin cannot hand out unheld", client(caAll, submitA), []api.Grant{viewA}, false},
	{"holder of submit cannot widen queue", client(caAll, submitA), []api.Grant{g(api.RoleSubmit, "queue:B")}, false},
	{"one bad grant fails the set", client(caAll, submitA), []api.Grant{submitA, viewA}, false},
	{"queue_admin submit", user(qaA), []api.Grant{submitA}, true},
	{"queue_admin view_all delete_all process", user(qaA), []api.Grant{viewA, deleteA, processA}, true},
	{"queue_admin on other queue denied", user(qaA), []api.Grant{g(api.RoleSubmit, "queue:B")}, false},
	{"queue_admin cannot mint queue_admin elsewhere", user(qaA), []api.Grant{g(api.RoleQueueAdmin, "queue:B")}, false},
	{"queue_admin cannot mint admin", user(qaA), []api.Grant{admin}, false},
	{"queue_admin cannot mint worker_admin", user(qaA), []api.Grant{waAll}, false},
	{"queue_admin cannot mint client_admin", user(qaA), []api.Grant{caAll}, false},
	{"worker_admin * covers worker scope", user(waAll), []api.Grant{waW1}, true},
	{"worker_admin W1 cannot widen to *", user(waW1), []api.Grant{waAll}, false},
	{"worker_admin W1 cannot grant w2", user(waW1), []api.Grant{g(api.RoleWorkerAdmin, "worker:w2")}, false},
	{"worker_admin cannot grant process", user(waAll), []api.Grant{processA}, false},
	{"no grants", client(), []api.Grant{submitA}, false},
	{"no grants empty request", client(), nil, true},
	{"worker cannot grant", worker(admin, qaA), []api.Grant{submitA}, false},
	{"unknown kind cannot grant", Principal{ID: "x", Grants: []api.Grant{admin}}, []api.Grant{submitA}, false},
	{"malformed held grant gives nothing", user(g(api.RoleQueueAdmin, "*")), []api.Grant{submitA}, false},
}

// TestCanGrantTo is the administrative rule for existing principals,
// including the confirmed exploits of the old holds()-based rule as
// explicit negative rows.
func TestCanGrantTo(t *testing.T) {
	runGrantCases(t, CanGrantTo, append(append([]grantCase{}, commonGrantCases...),
		// Exploit: a submit-only client revoked another client's submit grant.
		grantCase{"exploit: submit holder cannot grant or revoke submit", client(submitA), []api.Grant{submitA}, false},
		// Exploit: any role holder could grant that role to any principal.
		grantCase{"exploit: view_all holder cannot hand it on", user(viewA), []api.Grant{viewA}, false},
		grantCase{"read_all holder cannot hand it on", user(readA), []api.Grant{readA}, false},
		grantCase{"queue_admin + read_all holder still cannot grant read_all", user(qaA, readA), []api.Grant{readA}, false},
		grantCase{"exploit: delete_all holder cannot hand it on", client(deleteA), []api.Grant{deleteA}, false},
		grantCase{"exploit: client_admin cannot re-grant client_admin", client(caAll), []api.Grant{caAll}, false},
		grantCase{"exploit: client_admin cannot hand out held grants", client(caAll, submitA), []api.Grant{submitA}, false},
		grantCase{"exploit: worker_admin * cannot mint worker_admin *", user(waAll), []api.Grant{waAll}, false},
		grantCase{"exploit: worker_admin W1 cannot re-grant W1", user(waW1), []api.Grant{waW1}, false},
		// Exploit: a queue_admin granted queue_admin to others.
		grantCase{"exploit: queue_admin cannot grant queue_admin on its queue", user(qaA), []api.Grant{qaA}, false},
	))
}

// TestCanDelegateAtCreation is the POST /client rule: the administrative
// rule plus re-delegation of held grants.
func TestCanDelegateAtCreation(t *testing.T) {
	runGrantCases(t, CanDelegateAtCreation, append(append([]grantCase{}, commonGrantCases...),
		grantCase{"client_admin hands out held grants", client(caAll, submitA), []api.Grant{submitA}, true},
		grantCase{"client_admin may re-grant client_admin", client(caAll), []api.Grant{caAll}, true},
		grantCase{"client_admin re-delegates a held read_all (no escalation)", client(caAll, readA), []api.Grant{readA}, true},
		grantCase{"queue_admin may re-delegate its own queue_admin", user(qaA), []api.Grant{qaA}, true},
		grantCase{"held plus queue_admin rule combine", user(qaA, g(api.RoleSubmit, "queue:B")),
			[]api.Grant{viewA, g(api.RoleSubmit, "queue:B")}, true},
	))
}

func TestCanRevokeClient(t *testing.T) {
	tests := []struct {
		name  string
		actor Principal
		held  []api.Grant
		ok    bool
	}{
		{"admin any client", user(admin), []api.Grant{admin, submitA}, true},
		{"admin client with malformed grant", user(admin), []api.Grant{g(api.RoleSubmit, "*")}, true},
		{"client_admin revokes subset of itself", client(caAll, submitA), []api.Grant{submitA}, true},
		{"client_admin revokes grantless client", client(caAll), nil, true},
		{"client_admin cannot revoke more privileged client", client(caAll, submitA), []api.Grant{submitA, viewA}, false},
		{"client_admin cannot revoke an admin client", client(caAll), []api.Grant{admin}, false},
		{"client_admin with queue_admin revokes queue client", user(caAll, qaA), []api.Grant{submitA, viewA}, true},
		{"queue_admin without client_admin cannot revoke", user(qaA), []api.Grant{submitA}, false},
		{"submit holder cannot revoke", client(submitA), []api.Grant{submitA}, false},
		{"worker cannot revoke", worker(admin), nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := CanRevokeClient(tc.actor, "c9", tc.held)
			if tc.ok {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrForbidden)
		})
	}
}

func TestCanApproveWorker(t *testing.T) {
	qaB := g(api.RoleQueueAdmin, "queue:B")
	tests := []struct {
		name   string
		actor  Principal
		worker string
		queues []string
		ok     bool
	}{
		{"admin", user(admin), "w1", []string{"A", "B"}, true},
		{"wa + qa on all", user(waAll, qaA, qaB), "w1", []string{"A", "B"}, true},
		{"wa only, no queues", user(waAll), "w1", nil, true},
		{"wa only, with queue", user(waAll), "w1", []string{"A"}, false},
		{"qa only, no worker right", user(qaA), "w1", []string{"A"}, false},
		{"qa only, no queues still needs wa", user(qaA), "w1", nil, false},
		{"wa + qa missing one queue", user(waAll, qaA), "w1", []string{"A", "B"}, false},
		{"scoped wa right worker", user(waW1, qaA), "w1", []string{"A"}, true},
		{"scoped wa wrong worker", user(waW1, qaA), "w2", []string{"A"}, false},
		{"wa + qa wrong queue", user(waAll, qaB), "w1", []string{"A"}, false},
		{"admin worker_admin-less user", user(), "w1", nil, false},
		{"worker self-approval", worker(waAll, qaA), "w1", []string{"A"}, false},
		{"client with both rights", client(waAll, qaA), "w1", []string{"A"}, true},
		{"empty queue name", user(waAll, qaA), "w1", []string{""}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := CanApproveWorker(tc.actor, tc.worker, tc.queues)
			if tc.ok {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrForbidden)
		})
	}
}

func TestValidateGrantFetchRoles(t *testing.T) {
	for _, role := range []api.Role{api.RoleFetch, api.RoleSubmitURL} {
		assert.NoError(t, ValidateGrant(api.Grant{Role: role, Scope: "queue:A"}))
		assert.Error(t, ValidateGrant(api.Grant{Role: role, Scope: "*"}))
		assert.Error(t, ValidateGrant(api.Grant{Role: role, Scope: "queue:"}))
	}
}

func TestQueueAdminMayGrantFetchRoles(t *testing.T) {
	qa := user(g(api.RoleQueueAdmin, "queue:A"))
	for _, role := range []api.Role{api.RoleFetch, api.RoleSubmitURL} {
		assert.NoError(t, CanGrantTo(qa, []api.Grant{g(role, "queue:A")}))
		assert.Error(t, CanGrantTo(qa, []api.Grant{g(role, "queue:B")}))
	}
}

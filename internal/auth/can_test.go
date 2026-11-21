package auth

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/deweysasser/whisper-network/internal/api"
)

func g(role api.Role, scope string) api.Grant { return api.Grant{Role: role, Scope: scope} }

func user(grants ...api.Grant) Principal { return Principal{Kind: KindUser, ID: "u1", Grants: grants} }
func client(grants ...api.Grant) Principal {
	return Principal{Kind: KindClient, ID: "c1", Grants: grants}
}
func worker(grants ...api.Grant) Principal {
	return Principal{Kind: KindWorker, ID: "w1", Grants: grants}
}

var (
	admin    = g(api.RoleAdmin, "*")
	qaA      = g(api.RoleQueueAdmin, "queue:A")
	waAll    = g(api.RoleWorkerAdmin, "*")
	waW1     = g(api.RoleWorkerAdmin, "worker:w1")
	caAll    = g(api.RoleClientAdmin, "*")
	submitA  = g(api.RoleSubmit, "queue:A")
	viewA    = g(api.RoleViewAll, "queue:A")
	readA    = g(api.RoleReadAll, "queue:A")
	deleteA  = g(api.RoleDeleteAll, "queue:A")
	processA = g(api.RoleProcess, "queue:A")
)

func TestCanMatrix(t *testing.T) {
	qA := Resource{Queue: "A"}
	qB := Resource{Queue: "B"}
	jobA := func(owner string) Resource { return Resource{Queue: "A", OwnerID: owner} }
	jobB := Resource{Queue: "B", OwnerID: "someone"}
	w1 := Resource{WorkerID: "w1"}
	w2 := Resource{WorkerID: "w2"}

	tests := []struct {
		name string
		p    Principal
		a    Action
		r    Resource
		want bool
	}{
		// --- admin can do everything (as user and as client) ---
		{"admin submit", user(admin), ActSubmitJob, qA, true},
		{"admin list", user(admin), ActListQueue, qB, true},
		{"admin view foreign job", user(admin), ActViewJob, jobA("x"), true},
		{"admin download", user(admin), ActDownloadTranscript, jobB, true},
		{"admin content", user(admin), ActViewJobContent, jobB, true},
		{"admin delete", user(admin), ActDeleteJob, jobB, true},
		{"admin patch desc", user(admin), ActPatchQueueDescription, qA, true},
		{"admin patch max upload", user(admin), ActPatchQueueMaxUpload, qA, true},
		{"admin set weight", user(admin), ActSetQueueWeight, qA, true},
		{"admin create queue", user(admin), ActCreateQueue, Resource{}, true},
		{"admin approve worker", user(admin), ActApproveWorker, w2, true},
		{"admin pause worker", user(admin), ActPauseWorker, w2, true},
		{"admin resume worker", user(admin), ActResumeWorker, w2, true},
		{"admin reject worker", user(admin), ActRejectWorker, w2, true},
		{"admin delete worker", user(admin), ActDeleteWorker, w2, true},
		{"admin view worker", user(admin), ActViewWorker, w2, true},
		{"admin grant process", user(admin), ActGrantProcess, qB, true},
		{"admin create client", user(admin), ActCreateClient, Resource{}, true},
		{"admin revoke client", user(admin), ActRevokeClient, Resource{ClientID: "c9"}, true},
		{"admin view client", user(admin), ActViewClient, Resource{ClientID: "c9"}, true},
		{"admin grant queue role", user(admin), ActGrantQueueRole, qB, true},
		{"admin grant roles", user(admin), ActGrantRoles, Resource{}, true},
		{"admin create user", user(admin), ActCreateUser, Resource{}, true},
		{"admin audit", user(admin), ActViewAudit, Resource{}, true},
		{"admin metrics", user(admin), ActViewMetricsUI, Resource{}, true},
		{"admin client kind submit", client(admin), ActSubmitJob, qA, true},
		{"admin does not lease (not a worker)", user(admin), ActLeaseTask, qA, false},
		{"admin does not update worker facts", user(admin), ActUpdateWorker, w1, false},
		{"admin cannot fetch model", user(admin), ActFetchModel, Resource{}, false},

		// --- queue_admin on A ---
		{"qa list A", user(qaA), ActListQueue, qA, true},
		{"qa list B denied", user(qaA), ActListQueue, qB, false},
		{"qa view foreign job on A", user(qaA), ActViewJob, jobA("x"), true},
		{"qa does NOT download foreign job on A", user(qaA), ActDownloadTranscript, jobA("x"), false},
		{"qa sees foreign job metadata on A", user(qaA), ActViewJobContent, jobA("x"), true},
		{"qa+read_all downloads foreign job on A", user(qaA, readA), ActDownloadTranscript, jobA("x"), true},
		{"qa view job on B denied", user(qaA), ActViewJob, jobB, false},
		{"qa delete job on A", user(qaA), ActDeleteJob, jobA("x"), true},
		{"qa delete job on B denied", user(qaA), ActDeleteJob, jobB, false},
		{"qa patch description A", user(qaA), ActPatchQueueDescription, qA, true},
		{"qa patch max upload A", user(qaA), ActPatchQueueMaxUpload, qA, true},
		{"qa patch description B denied", user(qaA), ActPatchQueueDescription, qB, false},
		{"qa CANNOT set weight on own queue", user(qaA), ActSetQueueWeight, qA, false},
		{"qa cannot create queue", user(qaA), ActCreateQueue, qA, false},
		{"qa grant process A", user(qaA), ActGrantProcess, qA, true},
		{"qa grant process B denied", user(qaA), ActGrantProcess, qB, false},
		{"qa grant queue role A", user(qaA), ActGrantQueueRole, qA, true},
		{"qa grant queue role B denied", user(qaA), ActGrantQueueRole, qB, false},
		{"qa cannot grant arbitrary roles", user(qaA), ActGrantRoles, Resource{}, false},
		{"qa cannot submit without submit grant", user(qaA), ActSubmitJob, qA, false},
		{"qa cannot approve worker lifecycle", user(qaA), ActApproveWorker, w1, false},
		{"qa cannot pause", user(qaA), ActPauseWorker, w1, false},
		{"qa cannot delete worker", user(qaA), ActDeleteWorker, w1, false},
		{"qa cannot create client", user(qaA), ActCreateClient, Resource{}, false},
		{"qa cannot create user", user(qaA), ActCreateUser, Resource{}, false},
		{"qa cannot see audit", user(qaA), ActViewAudit, Resource{}, false},
		{"qa sees metrics", user(qaA), ActViewMetricsUI, Resource{}, true},
		{"qa client kind has no UI metrics", client(qaA), ActViewMetricsUI, Resource{}, false},

		// --- worker_admin ---
		{"wa* approve any worker", user(waAll), ActApproveWorker, w2, true},
		{"wa* pause", user(waAll), ActPauseWorker, w2, true},
		{"wa* resume", user(waAll), ActResumeWorker, w2, true},
		{"wa* reject", user(waAll), ActRejectWorker, w2, true},
		{"wa* delete", user(waAll), ActDeleteWorker, w2, true},
		{"wa* view worker", user(waAll), ActViewWorker, w2, true},
		{"waW1 approve w1", user(waW1), ActApproveWorker, w1, true},
		{"waW1 pause w1", user(waW1), ActPauseWorker, w1, true},
		{"waW1 approve w2 denied", user(waW1), ActApproveWorker, w2, false},
		{"waW1 reject w2 denied", user(waW1), ActRejectWorker, w2, false},
		{"waW1 delete w1", user(waW1), ActDeleteWorker, w1, true},
		{"waW1 delete w2 denied", user(waW1), ActDeleteWorker, w2, false},
		{"waW1 empty worker id denied", user(waW1), ActPauseWorker, Resource{}, false},
		{"wa cannot grant process", user(waAll), ActGrantProcess, qA, false},
		{"wa cannot set weight", user(waAll), ActSetQueueWeight, qA, false},
		{"wa cannot see jobs", user(waAll), ActViewJob, jobA("x"), false},
		{"wa cannot list", user(waAll), ActListQueue, qA, false},
		{"wa sees metrics", user(waAll), ActViewMetricsUI, Resource{}, true},
		{"wa+qa grant process A", user(waAll, qaA), ActGrantProcess, qA, true},
		{"wa+qa(A) grant process B denied", user(waAll, qaA), ActGrantProcess, qB, false},

		// --- clients ---
		{"submit A", client(submitA), ActSubmitJob, qA, true},
		{"submit B denied", client(submitA), ActSubmitJob, qB, false},
		{"submit list A", client(submitA), ActListQueue, qA, true},
		{"submit list B denied", client(submitA), ActListQueue, qB, false},
		{"submit view own", client(submitA), ActViewJob, jobA("c1"), true},
		{"submit view other's denied", client(submitA), ActViewJob, jobA("c2"), false},
		{"submit download own", client(submitA), ActDownloadTranscript, jobA("c1"), true},
		{"submit download other's denied", client(submitA), ActDownloadTranscript, jobA("c2"), false},
		{"submit delete own", client(submitA), ActDeleteJob, jobA("c1"), true},
		{"submit delete other's denied", client(submitA), ActDeleteJob, jobA("c2"), false},
		{"submit cannot patch queue", client(submitA), ActPatchQueueDescription, qA, false},
		{"submit cannot set weight", client(submitA), ActSetQueueWeight, qA, false},
		{"submit cannot create client", client(submitA), ActCreateClient, Resource{}, false},
		{"viewall view other's A", client(viewA), ActViewJob, jobA("c2"), true},
		{"viewall does NOT download other's A", client(viewA), ActDownloadTranscript, jobA("c2"), false},
		{"viewall sees other's metadata A", client(viewA), ActViewJobContent, jobA("c2"), true},
		{"readall list A", client(readA), ActListQueue, qA, true},
		{"readall view other's A", client(readA), ActViewJob, jobA("c2"), true},
		{"readall content of other's A", client(readA), ActViewJobContent, jobA("c2"), true},
		{"readall download other's A", client(readA), ActDownloadTranscript, jobA("c2"), true},
		{"readall B denied", client(readA), ActDownloadTranscript, jobB, false},
		{"readall view B denied", client(readA), ActViewJob, jobB, false},
		{"readall cannot submit", client(readA), ActSubmitJob, qA, false},
		{"readall cannot delete other's", client(readA), ActDeleteJob, jobA("c2"), false},
		{"submit content own", client(submitA), ActViewJobContent, jobA("c1"), true},
		{"submit content other's denied", client(submitA), ActViewJobContent, jobA("c2"), false},
		{"viewall B denied", client(viewA), ActViewJob, jobB, false},
		{"viewall list A", client(viewA), ActListQueue, qA, true},
		{"viewall cannot submit", client(viewA), ActSubmitJob, qA, false},
		{"viewall cannot delete other's", client(viewA), ActDeleteJob, jobA("c2"), false},
		{"deleteall delete other's A", client(deleteA), ActDeleteJob, jobA("c2"), true},
		{"deleteall delete B denied", client(deleteA), ActDeleteJob, jobB, false},
		{"admin retry", user(admin), ActRetryJob, jobB, true},
		{"qa retry job on A", user(qaA), ActRetryJob, jobA("x"), true},
		{"qa retry job on B denied", user(qaA), ActRetryJob, jobB, false},
		{"owner retry", client(submitA), ActRetryJob, jobA("c1"), true},
		{"submit retry other's denied", client(submitA), ActRetryJob, jobA("c2"), false},
		{"viewall retry other's denied", client(viewA), ActRetryJob, jobA("c2"), false},
		{"readall retry other's denied", client(readA), ActRetryJob, jobA("c2"), false},
		{"deleteall retry other's denied", client(deleteA), ActRetryJob, jobA("c2"), false},
		{"deleteall cannot view other's", client(deleteA), ActViewJob, jobA("c2"), false},
		{"deleteall cannot list", client(deleteA), ActListQueue, qA, false},
		{"owner view with no grants", client(), ActViewJob, jobA("c1"), true},
		{"owner content with no grants", client(), ActViewJobContent, jobA("c1"), true},
		{"owner download with no grants", client(), ActDownloadTranscript, jobA("c1"), true},
		{"owner delete with no grants", client(), ActDeleteJob, jobA("c1"), true},
		{"empty id never owns empty owner", Principal{Kind: KindClient}, ActViewJob, Resource{Queue: "A"}, false},
		{"empty grants cannot submit", client(), ActSubmitJob, qA, false},
		{"empty grants cannot list", client(), ActListQueue, qA, false},
		{"empty queue name never matches", client(g(api.RoleSubmit, "queue:")), ActSubmitJob, Resource{}, false},
		{"client_admin create client", client(caAll), ActCreateClient, Resource{}, true},
		{"client_admin revoke client", client(caAll), ActRevokeClient, Resource{ClientID: "c9"}, true},
		{"client_admin view client", client(caAll), ActViewClient, Resource{ClientID: "c9"}, true},
		{"client views self", client(), ActViewClient, Resource{ClientID: "c1"}, true},
		{"client views other denied", client(), ActViewClient, Resource{ClientID: "c9"}, false},
		{"client cannot revoke self", client(), ActRevokeClient, Resource{ClientID: "c1"}, false},
		{"client_admin cannot grant roles", client(caAll), ActGrantRoles, Resource{}, false},
		{"client_admin cannot create user", user(caAll), ActCreateUser, Resource{}, false},
		{"user id equal to client id is not 'self' client", user(), ActViewClient, Resource{ClientID: "u1"}, false},

		// --- malformed grants never widen access ---
		{"admin with wrong scope ignored", user(g(api.RoleAdmin, "queue:A")), ActCreateQueue, Resource{}, false},
		{"queue_admin with * scope ignored", user(g(api.RoleQueueAdmin, "*")), ActPatchQueueDescription, qA, false},
		{"submit with * scope ignored", client(g(api.RoleSubmit, "*")), ActSubmitJob, qA, false},
		{"unknown role ignored", user(g("root", "*")), ActCreateQueue, Resource{}, false},
		{"queue prefix collision", client(g(api.RoleSubmit, "queue:AB")), ActSubmitJob, qA, false},
		{"read_all with * scope ignored", client(g(api.RoleReadAll, "*")), ActDownloadTranscript, jobA("c2"), false},

		// --- workers ---
		{"worker lease A", worker(processA), ActLeaseTask, qA, true},
		{"worker lease B denied", worker(processA), ActLeaseTask, qB, false},
		{"worker no grants lease denied", worker(), ActLeaseTask, qA, false},
		{"worker empty queue denied", worker(processA), ActLeaseTask, Resource{}, false},
		{"worker view self", worker(), ActViewWorker, w1, true},
		{"worker view other denied", worker(), ActViewWorker, w2, false},
		{"worker update self", worker(), ActUpdateWorker, w1, true},
		{"worker update other denied", worker(), ActUpdateWorker, w2, false},
		{"worker fetch model", worker(), ActFetchModel, Resource{}, true},
		{"worker cannot submit", worker(processA), ActSubmitJob, qA, false},
		{"worker cannot list", worker(processA), ActListQueue, qA, false},
		{"worker cannot view job", worker(processA), ActViewJob, jobA("w1"), false},
		{"worker cannot download", worker(processA), ActDownloadTranscript, jobA("w1"), false},
		{"worker with read_all cannot download", worker(readA), ActDownloadTranscript, jobA("x"), false},
		{"worker cannot delete job", worker(processA), ActDeleteJob, jobA("w1"), false},
		{"worker cannot approve self", worker(waAll), ActApproveWorker, w1, false},
		{"worker cannot grant process", worker(qaA), ActGrantProcess, qA, false},
		{"worker with admin grant still denied", worker(admin), ActCreateQueue, Resource{}, false},
		{"worker with admin grant cannot weight", worker(admin), ActSetQueueWeight, qA, false},
		{"worker with admin grant cannot audit", worker(admin), ActViewAudit, Resource{}, false},
		{"worker cannot create client", worker(caAll), ActCreateClient, Resource{}, false},
		{"worker cannot view metrics UI", worker(admin), ActViewMetricsUI, Resource{}, false},
		{"worker cannot delete a worker", worker(admin), ActDeleteWorker, Resource{WorkerID: "w1"}, false},
		{"worker cannot see job workers", worker(admin), ActViewJobWorkers, Resource{Queue: "A", WorkerID: "w1"}, false},

		// --- who may see which worker runs a job ---
		{"admin sees job workers", user(admin), ActViewJobWorkers, Resource{Queue: "B", WorkerID: "w2"}, true},
		{"qa sees job workers on A", user(qaA), ActViewJobWorkers, Resource{Queue: "A", WorkerID: "w2"}, true},
		{"qa(A) job workers on B denied", user(qaA), ActViewJobWorkers, Resource{Queue: "B", WorkerID: "w2"}, false},
		{"wa* sees job workers", user(waAll), ActViewJobWorkers, Resource{Queue: "B", WorkerID: "w2"}, true},
		{"waW1 sees w1 on a job", user(waW1), ActViewJobWorkers, Resource{Queue: "B", WorkerID: "w1"}, true},
		{"waW1 w2 on a job denied", user(waW1), ActViewJobWorkers, Resource{Queue: "B", WorkerID: "w2"}, false},
		{"submit job workers denied", client(submitA), ActViewJobWorkers, Resource{Queue: "A", WorkerID: "w1"}, false},
		{"viewall job workers denied", client(viewA), ActViewJobWorkers, Resource{Queue: "A", WorkerID: "w1"}, false},
		{"process (as user) job workers denied", user(processA), ActViewJobWorkers, Resource{Queue: "A", WorkerID: "w1"}, false},

		// --- clients/users cannot do worker actions ---
		{"client cannot lease", client(processA), ActLeaseTask, qA, false},
		{"user cannot lease", user(processA), ActLeaseTask, qA, false},
		{"client cannot update worker", client(waAll), ActUpdateWorker, w1, false},
		{"client cannot fetch model", client(submitA), ActFetchModel, Resource{}, false},
		{"client with worker_admin may pause", client(waAll), ActPauseWorker, w1, true},

		// --- invalid inputs ---
		{"zero action denied", user(admin), 0, qA, false},
		{"unknown action denied", user(admin), Action(9999), qA, false},
		{"unknown kind denied", Principal{Kind: "", ID: "x", Grants: []api.Grant{admin}}, ActCreateQueue, Resource{}, false},
		{"no grants no admin ops", user(), ActCreateQueue, Resource{}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, Can(tc.p, tc.a, tc.r))
		})
	}
}

func TestVisibilityOn(t *testing.T) {
	tests := []struct {
		name  string
		p     Principal
		queue string
		want  Visibility
	}{
		{"admin", user(admin), "A", SeesAll},
		{"queue_admin", client(qaA), "A", SeesAll},
		{"queue_admin other queue", client(qaA), "B", SeesNone},
		{"view_all", client(viewA), "A", SeesAll},
		{"read_all implies view_all", client(readA), "A", SeesAll},
		{"read_all other queue", client(readA), "B", SeesNone},
		{"submit only", client(submitA), "A", SeesOwn},
		{"submit + view_all", client(submitA, viewA), "A", SeesAll},
		{"submit other queue", client(submitA), "B", SeesNone},
		{"delete_all only", client(deleteA), "A", SeesNone},
		{"no grants", client(), "A", SeesNone},
		{"worker with grants", worker(processA, admin), "A", SeesNone},
		{"empty queue", client(submitA), "", SeesNone},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, VisibilityOn(tc.p, tc.queue))
		})
	}
}

func TestVisibilityMatchesListQueue(t *testing.T) {
	ps := []Principal{user(admin), client(qaA), client(viewA), client(readA), client(submitA), client(deleteA), worker(processA), client()}
	for _, p := range ps {
		for _, q := range []string{"A", "B", ""} {
			assert.Equal(t, VisibilityOn(p, q) != SeesNone, Can(p, ActListQueue, Resource{Queue: q}))
		}
	}
}

// TestJobAccessMatrix pins down the split between seeing a job (status,
// prompt, metadata, submitter) and reading its transcript (issue #38): only
// the transcript needs read_all. Every combination of the roles involved,
// on someone else's job on queue A and on the principal's own job.
func TestJobAccessMatrix(t *testing.T) {
	type want struct{ list, view, content, download bool }
	all, statusOnly, none := want{true, true, true, true}, want{true, true, true, false}, want{}
	foreign := Resource{Queue: "A", OwnerID: "someone-else"}
	own := Resource{Queue: "A", OwnerID: "c1"}
	tests := []struct {
		name string
		p    Principal
		res  Resource
		want want
	}{
		{"owner (submit) own job", client(submitA), own, all},
		{"owner without grants own job", client(), own, want{false, true, true, true}},
		{"owner (submit) foreign job", client(submitA), foreign, want{list: true}},
		{"view_all", client(viewA), foreign, statusOnly},
		{"read_all", client(readA), foreign, all},
		{"view_all + read_all", client(viewA, readA), foreign, all},
		{"queue_admin", user(qaA), foreign, statusOnly},
		{"queue_admin + read_all", user(qaA, readA), foreign, all},
		{"admin", user(admin), foreign, all},
		{"delete_all", client(deleteA), foreign, none},
		{"no grants", client(), foreign, none},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := want{
				list:     Can(tc.p, ActListQueue, Resource{Queue: tc.res.Queue}),
				view:     Can(tc.p, ActViewJob, tc.res),
				content:  Can(tc.p, ActViewJobContent, tc.res),
				download: Can(tc.p, ActDownloadTranscript, tc.res),
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestCanSubmitURL(t *testing.T) {
	sub := g(api.RoleSubmitURL, "queue:A")
	r := Resource{Queue: "A"}
	assert.True(t, Can(user(sub), ActSubmitURL, r))
	assert.True(t, Can(client(sub), ActSubmitURL, r))
	assert.True(t, Can(user(admin), ActSubmitURL, r))
	assert.False(t, Can(user(submitA), ActSubmitURL, r))
	assert.False(t, Can(user(sub), ActSubmitURL, Resource{Queue: "B"}))
	assert.False(t, Can(worker(sub), ActSubmitURL, r))
}

func TestSubmitURLOwnJobRights(t *testing.T) {
	p := Principal{Kind: KindClient, ID: "c1", Grants: []api.Grant{g(api.RoleSubmitURL, "queue:A")}}
	own := Resource{Queue: "A", OwnerID: "c1"}
	other := Resource{Queue: "A", OwnerID: "c2"}
	for _, a := range []Action{ActViewJob, ActViewJobContent, ActDownloadTranscript, ActDeleteJob, ActRetryJob} {
		assert.True(t, Can(p, a, own), "action %d own", a)
		assert.False(t, Can(p, a, other), "action %d other", a)
	}
	assert.Equal(t, SeesOwn, VisibilityOn(p, "A"))
	assert.True(t, Can(p, ActListQueue, Resource{Queue: "A"}))
}

func TestCanManageFetchPolicy(t *testing.T) {
	assert.True(t, Can(user(admin), ActManageFetchPolicy, Resource{}))
	assert.False(t, Can(user(qaA), ActManageFetchPolicy, Resource{Queue: "A"}))
	assert.False(t, Can(user(waAll), ActManageFetchPolicy, Resource{}))
	assert.False(t, Can(worker(), ActManageFetchPolicy, Resource{}))
}

func TestLeaseTaskFetchOrProcess(t *testing.T) {
	r := Resource{Queue: "A"}
	assert.True(t, Can(worker(g(api.RoleFetch, "queue:A")), ActLeaseTask, r))
	assert.True(t, Can(worker(g(api.RoleProcess, "queue:A")), ActLeaseTask, r))
	assert.False(t, Can(worker(g(api.RoleFetch, "queue:B")), ActLeaseTask, r))
	assert.False(t, Can(worker(), ActLeaseTask, r))
}

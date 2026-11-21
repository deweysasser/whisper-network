package auth

import "github.com/deweysasser/whisper-network/internal/api"

// Can reports whether p may perform a on r, implementing DESIGN.md §5.2.
// It is a pure function and denies anything it does not recognise.
//
// Workers act only through the worker actions (lease, self view/update,
// model fetch) and their grants are consulted for nothing else; users and
// clients never hold worker powers.
func Can(p Principal, a Action, r Resource) bool {
	if p.Kind == KindWorker {
		return canWorker(p, a, r)
	}
	if p.Kind != KindUser && p.Kind != KindClient {
		return false
	}
	admin := p.isAdmin()
	switch a {
	case ActSubmitJob:
		return admin || p.onQueue(api.RoleSubmit, r.Queue)
	case ActSubmitURL:
		return admin || p.onQueue(api.RoleSubmitURL, r.Queue)
	case ActListQueue:
		return VisibilityOn(p, r.Queue) != SeesNone
	case ActViewJob, ActViewJobContent:
		// Everything describing a job (status, prompt, metadata, submitter)
		// goes to anyone who may see the job: a manager watching the queue
		// needs who submitted what and why.
		return admin || isOwner(p, r) || VisibilityOn(p, r.Queue) == SeesAll
	case ActDownloadTranscript:
		// The transcript is engine output from untrusted media and may carry
		// prompt injection, so reading it needs read_all (or ownership).
		return admin || isOwner(p, r) || p.onQueue(api.RoleReadAll, r.Queue)
	case ActDeleteJob:
		return admin || isOwner(p, r) ||
			p.onQueue(api.RoleDeleteAll, r.Queue) || p.onQueue(api.RoleQueueAdmin, r.Queue)
	case ActRetryJob:
		return admin || isOwner(p, r) || p.onQueue(api.RoleQueueAdmin, r.Queue)
	case ActPatchQueueDescription, ActPatchQueueMaxUpload,
		ActGrantProcess, ActGrantQueueRole:
		return admin || p.onQueue(api.RoleQueueAdmin, r.Queue)
	case ActApproveWorker, ActPauseWorker, ActResumeWorker, ActRejectWorker,
		ActDeleteWorker, ActViewWorker:
		return admin || p.workerAdminFor(r.WorkerID)
	case ActCreateClient, ActRevokeClient:
		return admin || p.holds(api.RoleClientAdmin, scopeAll)
	case ActViewClient:
		return admin || p.holds(api.RoleClientAdmin, scopeAll) ||
			(p.Kind == KindClient && p.ID != "" && p.ID == r.ClientID)
	case ActSetQueueWeight, ActCreateQueue, ActGrantRoles, ActCreateUser, ActViewAudit,
		ActManageFetchPolicy:
		return admin
	case ActViewJobWorkers:
		return admin || p.onQueue(api.RoleQueueAdmin, r.Queue) || p.workerAdminFor(r.WorkerID)
	case ActViewMetricsUI:
		return p.Kind == KindUser && (admin ||
			p.holdsAny(api.RoleQueueAdmin) || p.holdsAny(api.RoleWorkerAdmin))
	}
	return false
}

func canWorker(p Principal, a Action, r Resource) bool {
	switch a {
	case ActLeaseTask:
		return p.onQueue(api.RoleProcess, r.Queue)
	case ActViewWorker, ActUpdateWorker:
		return p.ID != "" && p.ID == r.WorkerID
	case ActFetchModel:
		return true
	}
	return false
}

// isOwner reports whether p created the job described by r. Workers never
// own jobs. Ids are UUIDs, so user and client id spaces do not collide.
func isOwner(p Principal, r Resource) bool {
	return p.ID != "" && p.ID == r.OwnerID
}

// Visibility says how much of a queue's jobs a principal may list.
type Visibility int

const (
	// SeesNone means the principal may not list the queue.
	SeesNone Visibility = iota
	// SeesOwn means only jobs whose client id is the principal's.
	SeesOwn
	// SeesAll means every job on the queue.
	SeesAll
)

// VisibilityOn returns which jobs of queue p may list (DESIGN.md §5.3): all
// for admin, queue_admin, view_all or read_all (a reader may see what it may
// read); its own for submit; otherwise none. Workers never see jobs.
func VisibilityOn(p Principal, queue string) Visibility {
	if p.Kind != KindUser && p.Kind != KindClient {
		return SeesNone
	}
	if p.isAdmin() || p.onQueue(api.RoleQueueAdmin, queue) ||
		p.onQueue(api.RoleViewAll, queue) || p.onQueue(api.RoleReadAll, queue) {
		return SeesAll
	}
	if p.onQueue(api.RoleSubmit, queue) || p.onQueue(api.RoleSubmitURL, queue) {
		return SeesOwn
	}
	return SeesNone
}

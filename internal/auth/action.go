package auth

// Action is an operation that Can can authorize (DESIGN.md §6).
type Action int

// The actions. Zero is deliberately invalid so an unset Action is denied.
const (
	_ Action = iota // zero is reserved as invalid

	// ActSubmitJob creates a job on Resource.Queue.
	ActSubmitJob
	// ActListQueue lists jobs on Resource.Queue; see Visibility for which.
	ActListQueue
	// ActViewJob reads a job's status document; needs Queue and OwnerID.
	ActViewJob
	// ActViewJobContent reads the client-supplied fields of a job's status
	// document (prompt, metadata, submitter). Same rule as ActViewJob: only
	// the transcript (ActDownloadTranscript) needs read_all. Needs Queue and
	// OwnerID.
	ActViewJobContent
	// ActDownloadTranscript reads a job's transcript or result JSON; needs
	// Queue and OwnerID.
	ActDownloadTranscript
	// ActDeleteJob deletes a job; needs Queue and OwnerID.
	ActDeleteJob
	// ActRetryJob retries a failed job (POST /job/{id}/retry): its owner,
	// queue_admin on its queue, or admin; needs Queue and OwnerID.
	ActRetryJob
	// ActPatchQueueDescription changes a queue's description.
	ActPatchQueueDescription
	// ActPatchQueueMaxUpload changes a queue's max_upload_bytes.
	ActPatchQueueMaxUpload
	// ActSetQueueWeight changes a queue's fleet share. Admin only.
	ActSetQueueWeight
	// ActCreateQueue creates a queue. Admin only.
	ActCreateQueue

	// ActApproveWorker is the lifecycle half of approving Resource.WorkerID.
	// The data-access half is ActGrantProcess; see CanApproveWorker.
	ActApproveWorker
	// ActPauseWorker pauses Resource.WorkerID.
	ActPauseWorker
	// ActResumeWorker resumes Resource.WorkerID.
	ActResumeWorker
	// ActRejectWorker rejects Resource.WorkerID.
	ActRejectWorker
	// ActDeleteWorker deletes Resource.WorkerID once it is rejected (the
	// caller checks the state): the same right as rejecting it.
	ActDeleteWorker
	// ActViewWorker reads a worker's state: the worker itself or a worker admin.
	ActViewWorker
	// ActUpdateWorker updates a worker's facts. The worker itself only.
	ActUpdateWorker
	// ActGrantProcess grants a worker process on Resource.Queue.
	ActGrantProcess
	// ActLeaseTask claims a task from Resource.Queue. Workers only.
	ActLeaseTask
	// ActFetchModel downloads a model file. Workers only (state checked by caller).
	ActFetchModel

	// ActCreateClient creates a client; CanDelegateAtCreation bounds its grants.
	ActCreateClient
	// ActRevokeClient revokes a client; CanRevokeClient adds the check that
	// the actor could have created it.
	ActRevokeClient
	// ActViewClient inspects Resource.ClientID: a client admin, or the client itself.
	ActViewClient
	// ActGrantQueueRole hands out queue-scoped roles on Resource.Queue to
	// clients and users; CanGrantTo bounds which.
	ActGrantQueueRole
	// ActGrantRoles hands out any role. Admin only; CanGrantTo still applies.
	ActGrantRoles
	// ActCreateUser creates a UI user. Admin only.
	ActCreateUser

	// ActViewAudit reads the audit log. Admin only.
	ActViewAudit
	// ActViewMetricsUI views the dashboard metrics. Users holding admin,
	// queue_admin or worker_admin.
	ActViewMetricsUI
	// ActViewJobWorkers sees which worker (Resource.WorkerID) is processing,
	// or processed, a job on Resource.Queue: admin, queue_admin on the
	// queue, or worker_admin over the worker. The caller must also hold
	// ActViewJob on the job; without this, the job shows only that it is
	// running (DESIGN.md §5.3).
	ActViewJobWorkers

	// ActSubmitURL creates a fetch job (a URL to download) on Resource.Queue.
	ActSubmitURL
	// ActManageFetchPolicy edits fetch policies and cooldowns. Admin only.
	ActManageFetchPolicy
)

// Resource identifies what an Action targets. Fields irrelevant to the
// action are ignored.
type Resource struct {
	// Queue is the queue name the action concerns, or the queue of a job.
	Queue string
	// OwnerID is the id of the principal that created the job.
	OwnerID string
	// WorkerID is the target worker.
	WorkerID string
	// ClientID is the target client.
	ClientID string
}

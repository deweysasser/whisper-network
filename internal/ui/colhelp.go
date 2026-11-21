package ui

import (
	"fmt"
	"strings"
)

// Every table column, definition-list term and dashboard tile explains
// itself (#79, DESIGN.md §6.3): its label carries the help text as a hover
// tooltip and as an ⓘ button that shows the same text, for keyboards, touch
// screens and screen readers. The "th", "dt" and "tile-label" templates
// render such a label from a column (all share the "help" partial); columns
// is the one source of the text, so a page cannot ship a label without help
// (the lookup fails the render, and tests walk every page and template).

// column is one labelled field (a table header, a <dt> term or a tile
// caption) and its explanation.
type column struct {
	ID    string // DOM id of the help text, unique per page
	Label string
	Help  string
	Class string // the cell class, e.g. "n" for numbers
	More  string // link to the fuller explanation, if any
}

// designDoc is where "more…" links point: the design document in the
// project repository.
const designDoc = "https://github.com/deweysasser/whisper-network/blob/main/DESIGN.md"

// columns maps a key ("page.column", "page.field") to its label and help.
var columns = map[string]column{
	// Dashboard: queues.
	"dash.queue":     {Label: "Queue", Help: "The queue; open it for its jobs and pending detail."},
	"dash.fetching":  {Label: "Fetching", Help: "URL jobs whose media a downloader worker is fetching."},
	"dash.preparing": {Label: "Preparing", Help: "Jobs whose upload is being converted to canonical audio (a prepare task)."},
	"dash.queued":    {Label: "Queued", Help: "Jobs ready to transcribe that no worker has started yet."},
	"dash.running":   {Label: "Running", Help: "Jobs with at least one task started."},
	"dash.done":      {Label: "Done", Help: "Jobs with a finished transcript."},
	"dash.failed":    {Label: "Failed", Help: "Jobs that failed for good; they can be retried while their media is kept."},
	"dash.pending":   {Label: "Pending tasks", Help: "Tasks waiting for a worker."},
	"dash.leased":    {Label: "Leased", Help: "Tasks a worker is running now."},
	"dash.oldest": {Label: "Oldest pending", Help: "How long the queue's oldest job with a pending task has existed.",
		More: designDoc + "#102-metrics-victoriametrics-and-prometheus"},
	"dash.eligible": {Label: "Eligible workers", Help: "Active workers seen in the last few minutes that may process this queue. Zero while tasks are pending means nothing can take them."},
	"dash.slots": {Label: "Eligible slots", Help: "Slots in use / total on those workers, from their last heartbeat. All slots in use: the queue is waiting for capacity; slots free: it is waiting its turn or for a matching worker.",
		More: designDoc + "#63-ui-ui"},
	// Dashboard: fleet share.
	"share.queue":  {Label: "Queue", Help: "The queue."},
	"share.weight": {Label: "Weight", Help: "The queue's share of the fleet relative to other queues; only an admin sets it.", More: designDoc + "#82-scheduling-weighted-fair-queueing"},
	"share.configured": {Label: "Configured", Help: "The queue's weight over the total weight of queues that have work: the share the scheduler aims for.",
		More: designDoc + "#82-scheduling-weighted-fair-queueing"},
	"share.actual": {Label: "Actual", Help: "The share of scheduling cost (model-weighted audio seconds) the queue was actually served recently."},

	// Queues list.
	"queues.name":   {Label: "Name", Help: "The queue's name, used in API paths."},
	"queues.desc":   {Label: "Description", Help: "What the queue is for, set by its queue admin."},
	"queues.weight": {Label: "Weight", Help: "The queue's share of the fleet relative to other queues (default 100).", More: designDoc + "#82-scheduling-weighted-fair-queueing"},
	"queues.cap":    {Label: "Upload cap", Help: "The largest upload the queue accepts; default is the server's limit."},
	"queues.jobs":   {Label: "Jobs", Help: "Whether you may see the queue's jobs."},

	// Queue page: jobs.
	"queue.select":  {Label: "Select", Help: "Choose jobs for a bulk retry or delete."},
	"queue.job":     {Label: "Job", Help: "The job's label (else its source, else its id), with the end of its id; opens the job."},
	"queue.state":   {Label: "State", Help: "fetching: a downloader is fetching the URL; preparing: converting the upload; queued: waiting for a worker; running: tasks in progress; done; failed."},
	"queue.weight":  {Label: "Weight", Help: "The job's priority class within the queue: high, medium or low.", More: designDoc + "#82-scheduling-weighted-fair-queueing"},
	"queue.model":   {Label: "Model", Help: "The whisper model the job is transcribed with."},
	"queue.audio":   {Label: "Audio", Help: "The length of the job's audio, once prepared."},
	"queue.tasks":   {Label: "Tasks", Help: "Task counts: done of total, then pending, leased (running) and failed ones. \"waiting\" is how long pending tasks have waited with none running."},
	"queue.workers": {Label: "Workers", Help: "Workers running the job's tasks now, or that finished them. Workers you may not see are counted as \"running\"."},
	"queue.created": {Label: "Created", Help: "When the job was submitted."},
	// Queue page: pending summary.
	"pending.kind":   {Label: "Kind", Help: "prepare converts an upload to canonical audio; transcribe runs whisper on a span of it."},
	"pending.model":  {Label: "Model", Help: "The whisper model the tasks need; a worker must offer it."},
	"pending.weight": {Label: "Weight", Help: "The jobs' priority class within the queue."},
	"pending.tasks":  {Label: "Tasks", Help: "Pending tasks in this group."},
	"pending.audio":  {Label: "Audio", Help: "Audio still to transcribe in this group."},
	"pending.oldest": {Label: "Oldest wait", Help: "How long the group's longest-waiting task has been pending."},
	"pending.why": {Label: "Why waiting", Help: "Why no eligible worker can take these tasks right now, best effort. Empty: they are waiting their turn.",
		More: designDoc + "#63-ui-ui"},
	// Queue page: access.
	"access.principal": {Label: "Principal", Help: "Who holds the role: a user, an API client or a worker."},
	"access.role":      {Label: "Role", Help: "What the role allows on this queue.", More: designDoc + "#5-authorization"},
	"access.actions":   {Label: "Actions", Help: "Revoke a grant you could have given."},

	// Job page.
	"task.seq":       {Label: "#", Help: "The task's number in the job: 0 is prepare, 1 and up are transcribe spans."},
	"task.kind":      {Label: "Kind", Help: "prepare converts the upload to canonical audio; transcribe runs whisper on a span of it."},
	"task.worker":    {Label: "Worker", Help: "The worker that holds or finished the task. Workers you may not see are not named."},
	"task.leaseage":  {Label: "Lease age", Help: "How long the worker has held the task."},
	"task.heartbeat": {Label: "Last heartbeat", Help: "When the worker last reported progress on the task; it renews the lease."},
	"task.attempt":   {Label: "Attempt", Help: "Which attempt this is, counting workers that failed the task or let it expire."},
	"task.audio":     {Label: "Audio", Help: "The span of the job's audio the task transcribed."},
	"task.engine":    {Label: "Engine", Help: "The whisper.cpp (transcribe) or ffmpeg (prepare) version that ran the task."},
	"task.compute":   {Label: "Compute", Help: "Wall time the worker spent on the task."},
	"task.state":     {Label: "State", Help: "pending: waiting for a worker; failed: failed for good; cancelled: will not run because the job failed."},
	"task.waiting":   {Label: "Waiting", Help: "How long a pending task has waited since it last became pending."},
	"task.attempts":  {Label: "Attempts", Help: "Workers that failed the task or let it expire so far."},
	"task.lastfail":  {Label: "Last failure", Help: "The latest failure reason and the worker it happened on."},
	"task.why": {Label: "Why waiting", Help: "Why no eligible worker can take the task right now, best effort. Empty: it is waiting its turn.",
		More: designDoc + "#63-ui-ui"},

	// Bulk results.
	"bulk.job":     {Label: "Job", Help: "The job acted on."},
	"bulk.outcome": {Label: "Outcome", Help: "What happened to the job, and why if it was skipped."},

	// Workers table.
	"workers.worker": {Label: "Worker", Help: "The worker's description, else its hostname, with the start of its id; opens the worker."},
	"workers.state":  {Label: "State", Help: "pending: awaiting approval; active: taking work; paused: finishing current work, taking none; rejected: token revoked."},
	"workers.kind":   {Label: "Kind", Help: "processing: prepares and transcribes audio; downloader: fetches media from URLs (with its egress key and yt-dlp version). Fixed when the worker registers."},
	"workers.queues": {Label: "Queues", Help: "Queues the worker may work for: process grants for processing workers, fetch grants for downloaders."},
	"workers.sandbox": {Label: "Sandbox", Help: "How strongly the worker can isolate media tools. Converting uploads (prepare tasks) needs the server's minimum, full by default.",
		More: designDoc + "#74-priority-and-sandbox"},
	"workers.version": {Label: "Version", Help: "The worker's whisper-network version compared with this server's: outdated is older, a note marks newer, differs when they cannot be ordered. Hover a version for the engine and ffmpeg versions."},
	"workers.slots":   {Label: "Slots", Help: "Busy / total task slots, from the last heartbeat; – before one."},
	"workers.seen":    {Label: "Last seen", Help: "When the worker last contacted the server; online if active and seen in the last few minutes."},
	"workers.done":    {Label: "Tasks done", Help: "Tasks the worker has completed; hover for the split by kind."},
	"workers.audio":   {Label: "Audio hours", Help: "Hours of audio the worker has transcribed."},
	"workers.rtf":     {Label: "Avg RTF", Help: "Average realtime factor: seconds of audio transcribed per second of compute. Higher is faster."},
	"workers.failure": {Label: "Failure rate", Help: "Failed or expired leases as a share of all the worker's counted task outcomes."},
	"workers.actions": {Label: "Actions", Help: "Controls for the worker's state, where you may change it."},

	// Fetch page.
	"fetch.key":         {Label: "Key", Help: "The site the policy governs, or site@egress-key for one egress; * is the default for any site."},
	"fetch.rules":       {Label: "Rules", Help: "At most N fetch starts per window, per egress: N/DURATION, e.g. 1/3m. None means unlimited."},
	"fetch.concurrency": {Label: "Concurrency", Help: "Fetches of the site that may run at once per egress."},
	"fetch.backoff":     {Label: "Backoff", Help: "Cooldown after the site rate-limits a fetch: the first, doubling on each repeat up to the cap, reset by a success."},
	"fetch.updated":     {Label: "Updated", Help: "Who last changed the policy, and when."},
	"fetch.actions":     {Label: "Actions", Help: "Edit loads the policy into the form; delete removes it (not the * default)."},
	"cool.egress":       {Label: "Egress", Help: "The egress key that is cooling down."},
	"cool.site":         {Label: "Site", Help: "The site that is not fetched from that egress meanwhile."},
	"cool.until":        {Label: "Until", Help: "When the cooldown ends."},
	"cool.kind":         {Label: "Kind", Help: "manual: set by an admin; automatic: set after the site rate-limited a fetch, with its backoff level."},
	"cool.reason":       {Label: "Reason", Help: "Why the cooldown was set, and by whom."},
	"cool.actions":      {Label: "Actions", Help: "End the cooldown now."},

	// Clients.
	"clients.name":    {Label: "Name", Help: "The client's name, chosen when it was created; opens the client."},
	"clients.id":      {Label: "ID", Help: "The client's id, which audit entries and job owners refer to."},
	"clients.roles":   {Label: "Roles", Help: "The client's grants: a role on a scope (* or a queue).", More: designDoc + "#5-authorization"},
	"clients.created": {Label: "Created", Help: "When the client was created."},
	"clients.used":    {Label: "Last used", Help: "When the client's token was last used."},
	"clients.actions": {Label: "Actions", Help: "Edit permissions or revoke the token, where you may."},
	"activity.time":   {Label: "Time", Help: "When it happened (UTC)."},
	"activity.action": {Label: "Action", Help: "What the client did, as the audit log records it."},
	"activity.target": {Label: "Target", Help: "What it acted on."},

	// Users.
	"users.user":    {Label: "User", Help: "The user's sign-in name."},
	"users.status":  {Label: "Status", Help: "active; must change password at next sign-in; or disabled."},
	"users.roles":   {Label: "Roles", Help: "The user's grants: a role on a scope (*, a queue or a worker).", More: designDoc + "#5-authorization"},
	"users.created": {Label: "Created", Help: "When the user was created."},
	"users.actions": {Label: "Actions", Help: "Disable the user, ending their sessions."},

	// Grant matrix.
	"matrix.queue": {Label: "Queue", Help: "Roles in this row apply to this queue only."},

	// Audit log.
	"audit.time":   {Label: "Time", Help: "When it happened (UTC)."},
	"audit.actor":  {Label: "Actor", Help: "Who did it: a user, an API client, a worker, the server itself (system) or the offline admin command."},
	"audit.action": {Label: "Action", Help: "What was done, as <area>.<verb>, e.g. worker.approve."},
	"audit.target": {Label: "Target", Help: "What it was done to: a user, client, worker, job or queue."},
	"audit.detail": {Label: "Detail", Help: "The change's parameters, as JSON."},

	// Dashboard tiles (the caption under each figure).
	"tile.running": {Label: "running tasks", Help: "Tasks a worker holds a lease on now, across all queues."},
	"tile.pending": {Label: "pending tasks", Help: "Tasks waiting for a worker, across all queues."},
	"tile.online":  {Label: "workers online", Help: "Active workers seen in the last few minutes. Below: all workers by state; pending ones await approval."},
	"tile.capacity": {Label: "slots in use · capacity", Help: "Task slots of online active workers, from their last heartbeat: in use / total. Available slots can take work now; deferred ones are on hosts busy with other work. Paused, pending and offline workers' slots are counted apart.",
		More: designDoc + "#63-ui-ui"},
	"tile.audiorate": {Label: "audio-hours / hour", Help: "Hours of audio transcribed by tasks completed in the last hour, across all queues. Each task counts as it completes, so a long job contributes steadily, and deleting a job afterwards does not change the figure."},
	"tile.versions": {Label: "Fleet versions", Help: "Versions run by active and paused workers, with how many run each, compared with this server's. Mixed engine (whisper.cpp) versions mix engines within one transcript's provenance.",
		More: designDoc + "#84-assembly-and-transcript-format"},

	// Queue page facts.
	"queuefact.desc":   {Label: "Description", Help: "What the queue is for, set by its queue admin."},
	"queuefact.weight": {Label: "Weight", Help: "The queue's share of the fleet relative to other queues (default 100).", More: designDoc + "#82-scheduling-weighted-fair-queueing"},
	"queuefact.cap":    {Label: "Upload cap", Help: "The largest upload the queue accepts; default is the server's limit."},

	// Job page facts.
	"jobfact.queue":     {Label: "Queue", Help: "The queue the job was submitted to."},
	"jobfact.state":     {Label: "State", Help: "preparing: converting the upload; queued: waiting for a worker; running: tasks in progress; done; failed."},
	"jobfact.error":     {Label: "Error", Help: "Why the job failed: the failing task's last reason."},
	"jobfact.label":     {Label: "Label", Help: "The submitter's name for the job, shown in lists."},
	"jobfact.source":    {Label: "Source", Help: "Where the submitter says the media came from, e.g. a file name or URL."},
	"jobfact.submitter": {Label: "Submitted by", Help: "The user or API client that created the job (the API's submitted_by); linked for client admins."},
	"jobfact.weight":    {Label: "Weight", Help: "The job's priority class within the queue: high, medium or low.", More: designDoc + "#82-scheduling-weighted-fair-queueing"},
	"jobfact.model":     {Label: "Model", Help: "The whisper model and language the job is transcribed with."},
	"jobfact.prompt":    {Label: "Prompt", Help: "Text given to whisper as the initial prompt, to steer spelling and style."},
	"jobfact.media":     {Label: "Media", Help: "The upload's size and type, and the audio length once prepared. Purged files are deleted under the retention policy."},
	"jobfact.tasks":     {Label: "Tasks", Help: "The job's tasks by state: one prepare, then one transcribe per audio span. Cancelled tasks will not run because the job failed."},
	"jobfact.created":   {Label: "Created", Help: "When the job was submitted."},
	"jobfact.started":   {Label: "Started", Help: "When a worker first took one of its tasks."},
	"jobfact.completed": {Label: "Completed", Help: "When the job finished, done or failed."},
	"jobfact.timing":    {Label: "Timing", Help: "waited: created to started; elapsed: created to completed; compute: wall time workers spent on its tasks, summed (it can exceed elapsed when tasks run in parallel)."},
	"jobfact.downloads": {Label: "Downloads", Help: "How many times the transcript was fetched, and when last."},
	"jobfact.files":     {Label: "Files", Help: "Until when a failed job's media is kept so it can be retried; the disk filling purges it sooner."},
	"jobfact.retries":   {Label: "Retries", Help: "How many times the job was retried after failing."},

	// Worker page facts.
	"workerfact.id":       {Label: "ID", Help: "The worker's id, which tasks, grants and the audit log refer to."},
	"workerfact.host":     {Label: "Hostname", Help: "The host name the worker reported when it registered."},
	"workerfact.desc":     {Label: "Description", Help: "The worker's --description, shown as its name when set."},
	"workerfact.cpu":      {Label: "CPU", Help: "The host's CPU model and logical core count, as the worker reported them."},
	"workerfact.mem":      {Label: "Memory", Help: "The host's total memory."},
	"workerfact.platform": {Label: "Platform", Help: "Operating system / CPU architecture of the worker binary."},
	"workerfact.versions": {Label: "Versions", Help: "whisper-network, whisper.cpp engine and ffmpeg versions the worker runs; transcripts record them as provenance."},
	"workerfact.sandbox": {Label: "Sandbox", Help: "How strongly the worker isolates media tools: full (filesystem and network confined), fs-only (filesystem only) or none. Prepare tasks need the server's minimum, full by default.",
		More: designDoc + "#74-priority-and-sandbox"},
	"workerfact.kind":    {Label: "Kind", Help: "processing (prepares and transcribes) or downloader (fetches URLs); fixed when the worker registered."},
	"workerfact.egress":  {Label: "Egress", Help: "Where the worker's downloads leave the network: the key per-site pacing is kept under, its operator-set group and the address it reported."},
	"workerfact.ytdlp":   {Label: "yt-dlp", Help: "The yt-dlp version the downloader runs."},
	"workerfact.kinds":   {Label: "Kinds", Help: "Task kinds the worker offers: prepare (convert uploads) and/or transcribe."},
	"workerfact.models":  {Label: "Models", Help: "Whisper models the worker offers; it only gets tasks for these."},
	"workerfact.slots":   {Label: "Slots", Help: "Tasks the worker runs at once; then, from its last heartbeat, slots busy now, the host's 1-minute load average and the memory (RSS) of its child processes."},
	"workerfact.hostcpu": {Label: "Host CPU", Help: "CPU used on the host by processes other than the worker's own, over the last minute. Above the worker's --max-host-load it stops claiming tasks (busy host) until load falls."},
	"workerfact.disk":    {Label: "Disk", Help: "In the worker's state directory: the model cache (including partial downloads), task work directories, and free space on that filesystem. ? means not reported."},
	"workerfact.cached":  {Label: "Cached models", Help: "Complete model files in the worker's cache; a task for another model downloads it first."},
	"workerfact.seen":    {Label: "Last seen", Help: "When the worker last contacted the server; online if active and seen in the last few minutes."},
	"workerfact.reg":     {Label: "Registered", Help: "When the worker first registered with the server."},
	"workerfact.decided": {Label: "Decided by", Help: "Who last approved, rejected, paused or resumed the worker; system for the circuit breaker."},

	// Worker page statistics.
	"workerstat.done":     {Label: "Tasks done", Help: "Tasks the worker completed, by kind, and the distinct jobs they belonged to."},
	"workerstat.audio":    {Label: "Audio", Help: "Hours of audio the worker has transcribed."},
	"workerstat.compute":  {Label: "Compute", Help: "Wall time the worker spent on completed tasks."},
	"workerstat.rtf":      {Label: "Realtime factor", Help: "Audio transcribed / compute time on transcribe tasks: 10× means an hour of audio in six minutes. Higher is faster."},
	"workerstat.failures": {Label: "Failures", Help: "Leases the worker failed or let expire. Failure rate: those as a share of all its counted outcomes (completed + failed + expired)."},
	"workerstat.streak": {Label: "Failure streak", Help: "Failed or expired leases in a row, and the jobs they span; a completed task ends it; failures that are the input's fault do not count. At the server's --worker-failure-limit (default 3) spanning at least 2 jobs, the circuit breaker pauses the worker.",
		More: designDoc + "#42-state-machines"},
	"workerstat.first": {Label: "First task", Help: "When the worker finished its first task."},
	"workerstat.last":  {Label: "Last task", Help: "When the worker last finished a task."},

	// Client page details.
	"clientfact.id":      {Label: "Client ID", Help: "The client's id, which audit entries and job owners refer to."},
	"clientfact.created": {Label: "Created", Help: "When the client was created, and by whom."},
	"clientfact.used":    {Label: "Last used", Help: "When the client's token was last used."},
	"clientfact.revoked": {Label: "Revoked", Help: "When the token was revoked; programs using it are refused."},
	"clientfact.roles":   {Label: "Roles", Help: "The client's grants: a role on a scope (* or a queue).", More: designDoc + "#5-authorization"},
}

// col returns the header of column key, with an optional cell class. A
// page with the same column in two tables tells them apart with a suffix,
// "key@table", which only changes the help's DOM id. An unknown key is an
// error, which fails the page render.
func col(key string, class ...string) (column, error) {
	name, _, _ := strings.Cut(key, "@")
	c, ok := columns[name]
	if !ok {
		return column{}, fmt.Errorf("col: no help for column %q", name)
	}
	c.ID = "help-" + strings.NewReplacer(".", "-", "@", "-").Replace(key)
	c.Class = strings.Join(class, " ")
	return c, nil
}

// colText is a header whose label and help are data (the grant matrix's
// role columns); key makes its DOM id unique.
func colText(key, label, help string, class ...string) column {
	return column{ID: "help-" + key, Label: label, Help: help, Class: strings.Join(class, " ")}
}

// valueHelp explains badge values where they appear (#79), keyed by
// "kind:value".
var valueHelp = map[string]string{
	"job:fetching":      "A downloader worker is fetching the media from its URL.",
	"job:preparing":     "Converting the upload to canonical audio.",
	"job:queued":        "Waiting for a worker.",
	"job:running":       "Tasks are in progress.",
	"job:done":          "The transcript is ready.",
	"job:failed":        "Failed for good; it can be retried while its media is kept.",
	"task:pending":      "Waiting for a worker.",
	"task:failed":       "Failed for good.",
	"task:cancelled":    "Will not run: the job failed.",
	"worker:downloader": "Fetches media from URLs for queues it is approved for; never transcribes.",
	"worker:processing": "Prepares and transcribes audio.",
	"worker:pending":    "Registered, awaiting approval by an admin.",
	"worker:active":     "Approved and taking work.",
	"worker:paused":     "Finishing current work, taking no new tasks.",
	"worker:rejected":   "Refused by an administrator; its token is revoked.",
	"badge:auto":        "Paused automatically by the circuit breaker after repeated failures.",
	"badge:busyhost":    "The host is busy with other work, so the worker is not taking tasks.",
	"badge:outdated":    "Older than the server's version.",
	"badge:differs":     "Cannot be ordered against the server's version.",
	"badge:online":      "Active and seen in the last few minutes.",
	"badge:revoked":     "The token is revoked; programs using it are refused.",
	"badge:noeligible":  "No active worker may process this queue: pending tasks cannot run.",
}

// vhelp returns the help for a badge value, or "".
func vhelp(kind string, value any) string { return valueHelp[kind+":"+fmt.Sprint(value)] }

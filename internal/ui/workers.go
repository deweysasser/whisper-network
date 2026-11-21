package ui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/auth"
	"github.com/deweysasser/whisper-network/internal/store"
	"github.com/deweysasser/whisper-network/internal/version"
)

// workersView is the workers table.
type workersView struct {
	Rows   []workerRow
	State  string // the state filter in force; empty for all
	States []api.WorkerState
	// ServerVersion is the server's own version, which the Version column
	// compares against.
	ServerVersion string
	// DeletableRejected counts the rejected workers the principal may
	// delete, whatever the filter; the "Delete all rejected" action is
	// offered when it is not zero.
	DeletableRejected int
}

// workerRow is one row of the workers table. CSRF is carried per row
// because the row is also rendered alone, as the htmx fragment that answers
// a pause or resume from the table.
//
// Per-worker summary columns are fields here, filled in workerRowFor and
// rendered as cells in the "worker-row" template with headers in the
// workers page's table. The work counters (tasks done, audio hours, average
// realtime factor, failure rate) are such columns; busy slots come from the
// last heartbeat, and the leases themselves are on the worker's page.
type workerRow struct {
	CSRF     string
	ID       string
	Ref      ref    // the worker as a ref: Name and the short id (#57)
	Name     string // description, else hostname, else the short id
	Hostname string // shown under Name when Name is the description
	State    api.WorkerState
	Queues   []string // queues it may process
	Sandbox  api.SandboxLevel
	// Kind is processing or downloader; a downloader also has its egress
	// key and yt-dlp version.
	Kind         api.WorkerKind
	EgressKey    string
	YtDlpVersion string
	Slots        int
	SlotsBusy    *int // from the last heartbeat's gauges; nil before one
	LastSeenAt   *time.Time
	Online       bool
	Stats        workStats // work counters; the table shows a summary, the page all
	// PauseReason is why the circuit breaker paused the worker (empty if
	// it did not); FailureStreak and StreakJobs are its current streak.
	PauseReason               string
	FailureStreak, StreakJobs int
	// Version is the worker's whisper-network version, Skew how it
	// compares with the server's (ServerVersion); EngineVersion and
	// FFmpegVersion go in the column's tooltip.
	Version, EngineVersion, FFmpegVersion string
	ServerVersion                         string
	Skew                                  version.Skew

	// Compact controls, each shown only when the principal may use it.
	CanApprove          bool // links to the detail page's approve form
	CanPause, CanResume bool
}

// workerDetail is the worker page: the row's summary plus every fact and
// the full controls.
//
// The worker's full statistics come from the embedded row's Stats and are
// rendered by the "worker-stats" template; its current leases by the
// "worker-leases" template.
type workerDetail struct {
	workerRow
	Facts        api.WorkerFacts
	RegisteredAt time.Time
	DecidedBy    string
	DecidedAt    *time.Time
	Gauges       *api.Gauges
	QueueGrants  []workerQueue
	Back         string   // this page, for forms that return here
	GrantRole    api.Role // the role granting the worker a queue: process, or fetch for a downloader
	// Leases are the worker's live leases on jobs the principal may view;
	// OtherLeases counts the rest.
	Leases      []workerLease
	OtherLeases int

	// Controls, each shown only when the principal may use it.
	ApproveQueues []string // queues the principal may grant process on
	CanReject     bool
	CanDelete     bool     // a rejected worker the principal may delete now
	AddQueues     []string // further queues grantable to an active worker
}

// workStats is a worker's counters with the figures the pages derive
// from them.
type workStats struct {
	api.WorkerStats
	TasksDone   int64
	AudioHours  float64
	RTF         float64 // audio seconds per compute second; 0 if none
	FailurePct  float64 // failed and expired leases, of counted outcomes
	ComputeTime time.Duration
}

func newWorkStats(s api.WorkerStats) workStats {
	return workStats{WorkerStats: s, TasksDone: s.TasksDone(), AudioHours: s.AudioS / 3600,
		RTF: s.RealtimeFactor(), FailurePct: 100 * s.FailureRate(),
		ComputeTime: time.Duration(s.ComputeS * float64(time.Second))}
}

// workerQueue is a queue a worker may process.
type workerQueue struct {
	Name      string
	CanRevoke bool
}

// workerStates are the states the table filters by, in display order.
var workerStates = []api.WorkerState{api.WorkerPending, api.WorkerActive, api.WorkerPaused, api.WorkerRejected}

// mayViewWorker mirrors GET /admin/workers: a queue_admin sees every worker
// (to approve them for its queues), a worker_admin only those in scope.
func mayViewWorker(rc *reqCtx, id string) bool {
	if !mayListWorkers(rc.P) {
		return false
	}
	return holdsRole(rc.P, api.RoleQueueAdmin) || rc.can(auth.ActViewWorker, auth.Resource{WorkerID: id})
}

// handleWorkers lists workers, pending first, optionally filtered by state.
func (u *UI) handleWorkers(w http.ResponseWriter, r *http.Request, rc *reqCtx) error {
	if !mayListWorkers(rc.P) {
		return u.deny(r, rc, "worker_admin or queue_admin required")
	}
	v := workersView{States: workerStates, ServerVersion: u.srv.Version()}
	if s := r.URL.Query().Get("state"); s != "" {
		if !slices.Contains(workerStates, api.WorkerState(s)) {
			return badRequest("Unknown worker state.")
		}
		v.State = s
	}
	ctx := r.Context()
	ws, err := u.srv.Store().ListWorkers(ctx)
	if err != nil {
		return err
	}
	for i := range ws {
		wk := &ws[i]
		if !mayViewWorker(rc, wk.ID) {
			continue
		}
		if mayDeleteWorker(rc, wk) {
			v.DeletableRejected++
		}
		if v.State != "" && string(wk.State) != v.State {
			continue
		}
		grants, err := u.processGrants(ctx, wk)
		if err != nil {
			return err
		}
		v.Rows = append(v.Rows, u.workerRowFor(rc, wk, grants))
	}
	slices.SortStableFunc(v.Rows, func(a, b workerRow) int { return stateRank(a.State) - stateRank(b.State) })
	u.render(w, r, rc, http.StatusOK, "workers", "Workers", v)
	return nil
}

// stateRank orders the workers table: pending first, rejected last.
func stateRank(s api.WorkerState) int {
	switch s {
	case api.WorkerPending:
		return 0
	case api.WorkerActive:
		return 1
	case api.WorkerPaused:
		return 2
	}
	return 3
}

// handleWorker shows one worker. A worker the principal may not see is
// reported as missing, so its existence does not leak.
func (u *UI) handleWorker(w http.ResponseWriter, r *http.Request, rc *reqCtx) error {
	id := r.PathValue("id")
	if !mayViewWorker(rc, id) {
		return u.hide(r, rc, "worker not visible")
	}
	ctx := r.Context()
	wk, err := u.srv.Store().WorkerByID(ctx, id)
	if err != nil {
		return err
	}
	queues, err := u.queueNames(ctx)
	if err != nil {
		return err
	}
	v, err := u.workerDetailFor(ctx, rc, wk, queues)
	if err != nil {
		return err
	}
	leases, others, err := u.workerLeases(ctx, rc, []string{id})
	if err != nil {
		return err
	}
	v.Leases, v.OtherLeases = leases[id], others[id]
	u.render(w, r, rc, http.StatusOK, "worker", "Worker "+v.Name, v)
	return nil
}

// workerGrantRole is the role that lets the worker work for a queue: fetch
// for a downloader, process otherwise.
func workerGrantRole(k api.WorkerKind) api.Role {
	if k == api.WorkerDownloader {
		return api.RoleFetch
	}
	return api.RoleProcess
}

// processGrants returns the worker's grants of its working role (process, or
// fetch for a downloader).
func (u *UI) processGrants(ctx context.Context, wk *store.Worker) ([]api.Grant, error) {
	grants, err := u.srv.Ops().WireGrants(ctx, wk.Principal())
	if err != nil {
		return nil, err
	}
	role := workerGrantRole(wk.Kind)
	return slices.DeleteFunc(grants, func(g api.Grant) bool { return g.Role != role }), nil
}

// workerName is how the UI names a worker: its description, else its
// hostname, else its abbreviated id.
func workerName(wk *store.Worker) string {
	switch {
	case strings.TrimSpace(wk.Facts.Description) != "":
		return wk.Facts.Description
	case strings.TrimSpace(wk.Facts.Hostname) != "":
		return wk.Facts.Hostname
	}
	return shortID(wk.ID)
}

// workerGauges decodes the worker's latest heartbeat gauges, or nil.
func workerGauges(wk *store.Worker) *api.Gauges {
	if len(wk.Gauges) == 0 {
		return nil
	}
	var g api.Gauges
	if json.Unmarshal(wk.Gauges, &g) != nil {
		return nil
	}
	return &g
}

// workerRowFor builds the table row for wk, given its process grants.
func (u *UI) workerRowFor(rc *reqCtx, wk *store.Worker, process []api.Grant) workerRow {
	res := auth.Resource{WorkerID: wk.ID}
	row := workerRow{CSRF: rc.CSRF, ID: wk.ID, Name: workerName(wk), State: wk.State,
		Sandbox: wk.Facts.SandboxLevel, Kind: wk.Kind, EgressKey: wk.EgressKey, YtDlpVersion: wk.Facts.YtDlpVersion, Slots: wk.Facts.Slots, LastSeenAt: wk.LastSeenAt,
		Online: isOnline(wk, u.srv.Now()), Stats: newWorkStats(wk.Stats), PauseReason: wk.PauseReason,
		FailureStreak: wk.FailureStreak, StreakJobs: wk.StreakJobs,
		Version: wk.Facts.Version, EngineVersion: wk.Facts.EngineVersion, FFmpegVersion: wk.Facts.FFmpegVersion,
		ServerVersion: u.srv.Version(), Skew: version.Compare(wk.Facts.Version, u.srv.Version())}
	row.Ref = workerRef(store.WorkerRef{ID: wk.ID, Hostname: wk.Facts.Hostname, Description: wk.Facts.Description})
	if row.Name == wk.Facts.Description && wk.Facts.Hostname != row.Name {
		row.Hostname = wk.Facts.Hostname
	}
	if g := workerGauges(wk); g != nil {
		row.SlotsBusy = &g.SlotsBusy
	}
	for _, g := range process {
		row.Queues = append(row.Queues, strings.TrimPrefix(g.Scope, "queue:"))
	}
	row.CanApprove = wk.State == api.WorkerPending && rc.can(auth.ActApproveWorker, res)
	row.CanPause = wk.State == api.WorkerActive && rc.can(auth.ActPauseWorker, res)
	row.CanResume = wk.State == api.WorkerPaused && rc.can(auth.ActResumeWorker, res)
	return row
}

// workerDetailFor builds the worker page for wk; queues are every queue's
// name, from which the grantable ones are offered.
func (u *UI) workerDetailFor(ctx context.Context, rc *reqCtx, wk *store.Worker, queues []string) (*workerDetail, error) {
	process, err := u.processGrants(ctx, wk)
	if err != nil {
		return nil, err
	}
	v := &workerDetail{workerRow: u.workerRowFor(rc, wk, process), Facts: wk.Facts,
		RegisteredAt: wk.RegisteredAt, DecidedBy: wk.DecidedBy, DecidedAt: wk.DecidedAt,
		Gauges: workerGauges(wk), Back: workerPath(wk.ID), GrantRole: workerGrantRole(wk.Kind)}
	held := map[string]bool{}
	for _, g := range process {
		name := strings.TrimPrefix(g.Scope, "queue:")
		held[name] = true
		v.QueueGrants = append(v.QueueGrants, workerQueue{Name: name,
			CanRevoke: auth.CanGrantTo(rc.P.Principal, []api.Grant{g}) == nil})
	}
	var grantable []string
	for _, q := range queues {
		if rc.can(workerGrantAction(wk.Kind), auth.Resource{Queue: q}) {
			grantable = append(grantable, q)
		}
	}
	switch wk.State {
	case api.WorkerPending:
		if v.CanApprove {
			v.ApproveQueues = grantable
		}
	case api.WorkerActive, api.WorkerPaused:
		for _, q := range grantable {
			if !held[q] {
				v.AddQueues = append(v.AddQueues, q)
			}
		}
	}
	v.CanReject = wk.State != api.WorkerRejected && rc.can(auth.ActRejectWorker, auth.Resource{WorkerID: wk.ID})
	v.CanDelete = mayDeleteWorker(rc, wk)
	return v, nil
}

// workerGrantAction is the action that grants a worker its working role
// (process or fetch; both are queue_admin acts, see auth.CanApproveWorker).
func workerGrantAction(api.WorkerKind) auth.Action { return auth.ActGrantProcess }

// mayDeleteWorker reports whether the principal may delete wk now: it is
// rejected and the principal holds the right that rejects it.
func mayDeleteWorker(rc *reqCtx, wk *store.Worker) bool {
	return wk.State == api.WorkerRejected && rc.can(auth.ActDeleteWorker, auth.Resource{WorkerID: wk.ID})
}

// workerPath is the worker's detail page.
func workerPath(id string) string { return "/ui/workers/" + url.PathEscape(id) }

// handleApproveWorker approves a worker for the chosen queues: worker_admin
// over the worker and queue_admin on every chosen queue (auth.CanApproveWorker,
// as POST /admin/workers/{id}/approve).
func (u *UI) handleApproveWorker(w http.ResponseWriter, r *http.Request, rc *reqCtx) error {
	id := r.PathValue("id")
	if err := r.ParseForm(); err != nil {
		return badRequest("Malformed form.")
	}
	queues := r.PostForm["queue"]
	if err := auth.CanApproveWorker(rc.P.Principal, id, queues); err != nil {
		return u.deny(r, rc, err.Error())
	}
	if err := u.srv.Ops().ApproveWorker(r.Context(), rc.P.Actor(), id, queues); err != nil {
		return err
	}
	return u.workerDone(w, r, rc, id)
}

// handleWorkerState pauses, resumes or rejects a worker (worker_admin, as
// POST /admin/workers/{id}/{pause,resume,reject}).
func (u *UI) handleWorkerState(w http.ResponseWriter, r *http.Request, rc *reqCtx) error {
	id := r.PathValue("id")
	var act auth.Action
	var state api.WorkerState
	switch r.PathValue("verb") {
	case "pause":
		act, state = auth.ActPauseWorker, api.WorkerPaused
	case "resume":
		act, state = auth.ActResumeWorker, api.WorkerActive
	case "reject":
		act, state = auth.ActRejectWorker, api.WorkerRejected
	default:
		return errNotFound
	}
	if !rc.can(act, auth.Resource{WorkerID: id}) {
		return u.deny(r, rc, "worker_admin required")
	}
	if err := u.srv.Ops().SetWorkerState(r.Context(), rc.P.Actor(), id, state); err != nil {
		return err
	}
	return u.workerDone(w, r, rc, id)
}

// handleDeleteWorker deletes a rejected worker now, as
// DELETE /admin/workers/{id}, and returns to the workers page.
func (u *UI) handleDeleteWorker(w http.ResponseWriter, r *http.Request, rc *reqCtx) error {
	id := r.PathValue("id")
	if !rc.can(auth.ActDeleteWorker, auth.Resource{WorkerID: id}) {
		return u.deny(r, rc, "worker_admin required")
	}
	if err := u.srv.Ops().DeleteWorker(r.Context(), rc.P.Actor(), id); err != nil {
		return err
	}
	u.redirect(w, r, "/ui/workers")
	return nil
}

// handleDeleteRejectedWorkers deletes every rejected worker the principal
// may delete, in one transaction with one worker.delete entry each, as the
// reaper does. Workers out of the principal's scope are left alone.
func (u *UI) handleDeleteRejectedWorkers(w http.ResponseWriter, r *http.Request, rc *reqCtx) error {
	if !holdsRole(rc.P, api.RoleWorkerAdmin) && !holdsRole(rc.P, api.RoleAdmin) {
		return u.deny(r, rc, "worker_admin required")
	}
	ws, err := u.srv.Store().ListWorkers(r.Context())
	if err != nil {
		return err
	}
	var ids []string
	for i := range ws {
		if mayDeleteWorker(rc, &ws[i]) {
			ids = append(ids, ws[i].ID)
		}
	}
	if len(ids) > 0 {
		if _, err := u.srv.Ops().DeleteRejectedWorkers(r.Context(), rc.P.Actor(), ids); err != nil {
			return err
		}
	}
	u.redirect(w, r, "/ui/workers")
	return nil
}

// workerDone answers a worker action. An htmx request from a table row
// (fragment=row) gets the refreshed row; anything else is sent to "back"
// (checked like a login's next), by default the worker's page.
func (u *UI) workerDone(w http.ResponseWriter, r *http.Request, rc *reqCtx, id string) error {
	if isHTMX(r) && r.PostFormValue("fragment") == "row" {
		wk, err := u.srv.Store().WorkerByID(r.Context(), id)
		if err != nil {
			return err
		}
		grants, err := u.processGrants(r.Context(), wk)
		if err != nil {
			return err
		}
		u.renderFragment(w, http.StatusOK, "workers", "worker-row", u.workerRowFor(rc, wk, grants))
		return nil
	}
	back := workerPath(id)
	if b := r.PostFormValue("back"); b != "" {
		back = safeNext(b)
	}
	u.redirect(w, r, back)
	return nil
}

// queueNames returns every queue name, sorted.
func (u *UI) queueNames(ctx context.Context) ([]string, error) {
	qs, err := u.srv.Store().ListQueues(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(qs))
	for i, q := range qs {
		out[i] = q.Name
	}
	return out, nil
}

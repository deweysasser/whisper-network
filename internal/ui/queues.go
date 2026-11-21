package ui

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/auth"
	"github.com/deweysasser/whisper-network/internal/server"
	"github.com/deweysasser/whisper-network/internal/store"
)

// queuesView is the queue list.
type queuesView struct {
	Queues    []queueItem
	CanCreate bool
}

type queueItem struct {
	store.Queue
	SeesJobs bool
}

// queueView is one queue: settings, jobs and grants.
type queueView struct {
	Queue                                  store.Queue
	CanEditDesc, CanEditMax, CanEditWeight bool

	Visibility auth.Visibility
	SeesJobs   bool
	OwnOnly    bool // SeesOwn: the list holds the principal's jobs only
	Jobs       []jobRow
	State      string // the state filter in force
	States     []api.JobState
	Sort       string // the order in force: sortState, sortNewest or sortOldest
	Sorts      []sortChoice
	NextPage   string // URL of the next page, if any
	// Matching counts the jobs matching the state filter (when one is set),
	// for "select all matching".
	Matching int
	CSRF     string
	Script   string // URL of queue.js

	// Pending is the pending-work summary (#54), for those who see all
	// the queue's jobs.
	Pending *pendingView

	CanManageGrants bool
	Grants          []grantRow
	GrantRoles      []api.Role // roles the principal may hand out here
	Back            string
}

// pendingView summarizes a queue's pending tasks (#54).
type pendingView struct {
	Tasks       int
	Prepare     int // pending prepare tasks
	Transcribe  int // pending transcribe tasks
	AudioHours  float64
	Oldest      time.Duration // the longest wait, 0 if nothing is pending
	Eligible    int           // active workers seen recently with process on the queue
	EligibleCap store.SlotCount
	Groups      []pendingRow
}

// pendingRow is the pending tasks of one kind, model and weight class.
type pendingRow struct {
	store.PendingGroup
	AudioS  float64
	Oldest  time.Duration
	Reasons []string
}

// grantRow is one principal's role on a scope, with a revoke control.
type grantRow struct {
	Ref       ref    // the principal, named (#57, #60)
	Principal string // wire form: user:<name>, client:<id>, worker:<id>
	Role      api.Role
	Scope     string
	CanRevoke bool
}

func (u *UI) handleQueues(w http.ResponseWriter, r *http.Request, rc *reqCtx) error {
	qs, err := u.srv.Store().ListQueues(r.Context())
	if err != nil {
		return err
	}
	v := queuesView{CanCreate: rc.can(auth.ActCreateQueue, auth.Resource{})}
	for _, q := range qs {
		v.Queues = append(v.Queues, queueItem{Queue: q,
			SeesJobs: auth.VisibilityOn(rc.P.Principal, q.Name) != auth.SeesNone})
	}
	u.render(w, r, rc, http.StatusOK, "queues", "Queues", v)
	return nil
}

// handleCreateQueue creates a queue (admin, as POST /admin/queues).
func (u *UI) handleCreateQueue(w http.ResponseWriter, r *http.Request, rc *reqCtx) error {
	if !rc.can(auth.ActCreateQueue, auth.Resource{}) {
		return u.deny(r, rc, "admin required")
	}
	q := api.Queue{Name: strings.TrimSpace(r.PostFormValue("name")), Description: r.PostFormValue("description")}
	var err error
	if q.Weight, err = optionalInt(r.PostFormValue("weight")); err != nil {
		return badRequest("Weight must be a whole number from 1 to 10000.")
	}
	if q.MaxUploadBytes, err = optionalBytes(r.PostFormValue("max_upload")); err != nil {
		return badRequest("Max upload must be a size such as 500MiB or 2GiB.")
	}
	created, err := u.srv.Ops().CreateQueue(r.Context(), rc.P.Actor(), q)
	if err != nil {
		return err
	}
	http.Redirect(w, r, "/ui/queues/"+url.PathEscape(created.Name), http.StatusSeeOther)
	return nil
}

// handleQueue shows a queue. Anyone signed in may see its settings (as GET
// /admin/queues); jobs follow auth.VisibilityOn (as GET /queue/{q}); grants
// are shown to those who may manage them.
func (u *UI) handleQueue(w http.ResponseWriter, r *http.Request, rc *reqCtx) error {
	ctx := r.Context()
	q, err := u.srv.Store().QueueByName(ctx, r.PathValue("q"))
	if err != nil {
		return err
	}
	res := auth.Resource{Queue: q.Name}
	v := queueView{Queue: *q,
		CanEditDesc:   rc.can(auth.ActPatchQueueDescription, res),
		CanEditMax:    rc.can(auth.ActPatchQueueMaxUpload, res),
		CanEditWeight: rc.can(auth.ActSetQueueWeight, res),
		Visibility:    auth.VisibilityOn(rc.P.Principal, q.Name),
		States:        []api.JobState{api.JobFetching, api.JobPreparing, api.JobQueued, api.JobRunning, api.JobDone, api.JobFailed},
		Sorts:         sortChoices,
		Back:          "/ui/queues/" + url.PathEscape(q.Name),
		CSRF:          rc.CSRF,
		Script:        u.assetURL("queue.js"),
	}
	v.SeesJobs = v.Visibility != auth.SeesNone
	v.OwnOnly = v.Visibility == auth.SeesOwn
	if v.SeesJobs {
		if err := u.queueJobs(w, r, rc, q, &v); err != nil {
			return err
		}
	}
	if v.Visibility == auth.SeesAll {
		if v.Pending, err = u.queuePending(ctx, q); err != nil {
			return err
		}
	}
	if rc.can(auth.ActGrantQueueRole, res) {
		v.CanManageGrants = true
		if v.Grants, err = u.queueGrants(ctx, rc, q); err != nil {
			return err
		}
		for _, role := range []api.Role{api.RoleQueueAdmin, api.RoleSubmit, api.RoleSubmitURL, api.RoleViewAll, api.RoleReadAll, api.RoleDeleteAll} {
			if auth.CanGrantTo(rc.P.Principal, []api.Grant{{Role: role, Scope: auth.QueueScope(q.Name)}}) == nil {
				v.GrantRoles = append(v.GrantRoles, role)
			}
		}
	}
	u.render(w, r, rc, http.StatusOK, "queue", "Queue "+q.Name, v)
	return nil
}

// queuePending summarizes q's pending tasks: two aggregate queries (the
// pending groups and the eligible workers), whatever the backlog.
func (u *UI) queuePending(ctx context.Context, q *store.Queue) (*pendingView, error) {
	groups, err := u.srv.Store().QueuePending(ctx, q.ID)
	if err != nil {
		return nil, err
	}
	fleet, err := u.queueFleet(ctx, q.Name, q.ID)
	if err != nil {
		return nil, err
	}
	now := u.srv.Now()
	v := &pendingView{Eligible: len(fleet.workers)}
	v.EligibleCap = store.FleetCapacity(fleet.workers, now, onlineWindow, fleet.prepareMin).All
	for _, g := range groups {
		row := pendingRow{PendingGroup: g, AudioS: samplesToSeconds(g.AudioSamples),
			Oldest: max(now.Sub(g.OldestSince), 0), Reasons: fleet.waitingReasons(g.Kind, g.Model, nil)}
		v.Tasks += g.Tasks
		if g.Kind == api.KindPrepare {
			v.Prepare += g.Tasks
		} else {
			v.Transcribe += g.Tasks
		}
		v.AudioHours += row.AudioS / 3600
		v.Oldest = max(v.Oldest, row.Oldest)
		v.Groups = append(v.Groups, row)
	}
	return v, nil
}

// The queue page's orders (#67). The URL carries the API's parameters
// (?sort=state, ?sort=created&order=desc|asc); the choice is remembered per
// browser in the sortCookie cookie.
const (
	sortState  = "state"
	sortNewest = "created-desc"
	sortOldest = "created-asc"
	sortCookie = "wn_queue_sort"
)

// sortChoice is one option of the order control.
type sortChoice struct{ Value, Label string }

var sortChoices = []sortChoice{
	{sortState, "state (running, waiting, failed, done)"},
	{sortNewest, "newest first"},
	{sortOldest, "oldest first"},
}

// queueSort returns the page's order: from the URL (the order control's
// order_by, or the API's sort and order), and then remembered in a cookie;
// else from the cookie; else by state.
func queueSort(w http.ResponseWriter, r *http.Request) (string, error) {
	q := r.URL.Query()
	var v string
	switch sort, order, by := q.Get("sort"), q.Get("order"), q.Get("order_by"); {
	case by != "":
		for _, sc := range sortChoices {
			if by == sc.Value {
				v = by
			}
		}
		if v == "" {
			return "", badRequest("Unknown sort order.")
		}
	case sort == "" && order == "":
		if c, err := r.Cookie(sortCookie); err == nil {
			for _, sc := range sortChoices {
				if c.Value == sc.Value {
					return c.Value, nil
				}
			}
		}
		return sortState, nil
	case sort == "state" && order == "":
		v = sortState
	case sort == "created" && (order == "" || order == "desc"):
		v = sortNewest
	case sort == "created" && order == "asc":
		v = sortOldest
	default:
		return "", badRequest("Unknown sort order.")
	}
	http.SetCookie(w, &http.Cookie{Name: sortCookie, Value: v, Path: "/ui/queues", MaxAge: 365 * 24 * 3600,
		HttpOnly: true, SameSite: http.SameSiteLaxMode, Secure: r.TLS != nil})
	return v, nil
}

// sortParams sets the URL parameters of order v.
func sortParams(p url.Values, v string) {
	switch v {
	case sortState:
		p.Set("sort", "state")
	case sortNewest:
		p.Set("sort", "created")
		p.Set("order", "desc")
	case sortOldest:
		p.Set("sort", "created")
		p.Set("order", "asc")
	}
}

// queueJobs fills the job list: everything for SeesAll, the principal's own
// jobs for SeesOwn, filtered by state, in the chosen order and paged by
// cursor.
func (u *UI) queueJobs(w http.ResponseWriter, r *http.Request, rc *reqCtx, q *store.Queue, v *queueView) error {
	qv := r.URL.Query()
	f := store.JobFilter{QueueID: q.ID, Limit: 50, Cursor: qv.Get("cursor")}
	var err error
	if v.Sort, err = queueSort(w, r); err != nil {
		return err
	}
	switch v.Sort {
	case sortState:
		f.Sort = store.SortState
	case sortNewest:
		f.Sort, f.Desc = store.SortCreated, true
	default:
		f.Sort = store.SortCreated
	}
	if v.Visibility == auth.SeesOwn {
		f.ClientID = rc.P.ID
	}
	if s := qv.Get("state"); s != "" {
		if !validJobState(api.JobState(s)) {
			return badRequest("Unknown job state.")
		}
		f.States = []api.JobState{api.JobState(s)}
		v.State = s
	}
	jobs, next, err := u.srv.Store().ListJobs(r.Context(), f)
	if errors.Is(err, store.ErrInvalid) {
		return badRequest("Bad page cursor.")
	}
	if err != nil {
		return err
	}
	if v.Jobs, err = u.queueJobRows(r.Context(), rc, q.Name, jobs); err != nil {
		return err
	}
	if next != "" {
		p := url.Values{"cursor": {next}}
		if v.State != "" {
			p.Set("state", v.State)
		}
		sortParams(p, v.Sort)
		v.NextPage = v.Back + "?" + p.Encode()
	}
	if v.State != "" {
		f.Cursor = ""
		if v.Matching, err = u.srv.Store().CountJobs(r.Context(), f); err != nil {
			return err
		}
	}
	return nil
}

func validJobState(s api.JobState) bool {
	switch s {
	case api.JobFetching, api.JobPreparing, api.JobQueued, api.JobRunning, api.JobDone, api.JobFailed:
		return true
	}
	return false
}

// queueGrants lists who holds which role on q, in wire form.
func (u *UI) queueGrants(ctx context.Context, rc *reqCtx, q *store.Queue) ([]grantRow, error) {
	st := u.srv.Store()
	users, err := st.ListUsers(ctx)
	if err != nil {
		return nil, err
	}
	names := make(map[string]string, len(users))
	for i := range users {
		names[users[i].Principal()] = "user:" + users[i].Username
	}
	var out []grantRow
	var ids labelIDs
	scope := auth.QueueScope(q.Name)
	for _, role := range []api.Role{api.RoleQueueAdmin, api.RoleSubmit, api.RoleSubmitURL, api.RoleViewAll, api.RoleReadAll, api.RoleDeleteAll, api.RoleProcess, api.RoleFetch} {
		ps, err := st.PrincipalsWithRole(ctx, role, store.QueueScope(q.ID))
		if err != nil {
			return nil, err
		}
		for _, p := range ps {
			if n, ok := names[p]; ok {
				p = n
			}
			ids.principal(p)
			out = append(out, grantRow{Principal: p, Role: role, Scope: scope,
				CanRevoke: auth.CanGrantTo(rc.P.Principal, []api.Grant{{Role: role, Scope: scope}}) == nil})
		}
	}
	lb, err := u.labels(ctx, rc, ids)
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].Ref = lb.principal(out[i].Principal)
	}
	return out, nil
}

// handlePatchQueue edits a queue. Only fields that changed are applied, and
// each one is authorized exactly as PATCH /queue/{q} does: weight is admin
// only, description and upload cap need queue_admin.
func (u *UI) handlePatchQueue(w http.ResponseWriter, r *http.Request, rc *reqCtx) error {
	q, err := u.srv.Store().QueueByName(r.Context(), r.PathValue("q"))
	if err != nil {
		return err
	}
	if err := r.ParseForm(); err != nil {
		return badRequest("Malformed form.")
	}
	var patch api.QueuePatch
	if _, ok := r.PostForm["description"]; ok {
		if d := r.PostFormValue("description"); d != q.Description {
			patch.Description = &d
		}
	}
	if _, ok := r.PostForm["max_upload"]; ok {
		n, err := optionalBytes(r.PostFormValue("max_upload"))
		if err != nil {
			return badRequest("Max upload must be a size such as 500MiB or 2GiB.")
		}
		if n != q.MaxUploadBytes {
			patch.MaxUploadBytes = &n
		}
	}
	if _, ok := r.PostForm["weight"]; ok {
		n, err := strconv.Atoi(strings.TrimSpace(r.PostFormValue("weight")))
		if err != nil {
			return badRequest("Weight must be a whole number from 1 to 10000.")
		}
		if n != q.Weight {
			patch.Weight = &n
		}
	}
	res := auth.Resource{Queue: q.Name}
	checks := []struct {
		set bool
		act auth.Action
		why string
	}{
		{patch.Weight != nil, auth.ActSetQueueWeight, "only admin may change a queue's weight"},
		{patch.Description != nil, auth.ActPatchQueueDescription, "queue_admin required"},
		{patch.MaxUploadBytes != nil, auth.ActPatchQueueMaxUpload, "queue_admin required"},
	}
	for _, c := range checks {
		if c.set && !rc.can(c.act, res) {
			return u.deny(r, rc, c.why)
		}
	}
	if patch != (api.QueuePatch{}) {
		if _, err := u.srv.Ops().PatchQueue(r.Context(), rc.P.Actor(), q, patch); err != nil {
			return err
		}
	}
	http.Redirect(w, r, "/ui/queues/"+url.PathEscape(q.Name), http.StatusSeeOther)
	return nil
}

func optionalInt(s string) (int, error) {
	if s = strings.TrimSpace(s); s == "" {
		return 0, nil
	}
	return strconv.Atoi(s)
}

// optionalBytes parses a size; empty means 0 (the server default).
func optionalBytes(s string) (int64, error) {
	if s = strings.TrimSpace(s); s == "" || s == "0" {
		return 0, nil
	}
	return server.ParseByteSize(s)
}

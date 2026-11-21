package ui

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/auth"
	"github.com/deweysasser/whisper-network/internal/store"
	"github.com/deweysasser/whisper-network/internal/version"
)

// throughputWindow is the window for audio-hours per hour.
const throughputWindow = time.Hour

// dashboardView is the dashboard's data. Fleet statistics are shown only to
// principals allowed auth.ActViewMetricsUI; everyone else gets links to the
// queues whose jobs they can see.
type dashboardView struct {
	Stats  *statsView
	Queues []string
}

// statsView is the refreshable statistics block.
type statsView struct {
	Queues                        []queueStatRow
	RunningTasks, PendingTasks    int
	WorkersOnline, WorkersActive  int
	WorkersPending, WorkersPaused int
	AudioHoursPerHour             float64
	ShareSpan                     time.Duration // 0: since server start
	Generated                     time.Time
	Versions                      fleetVersions
	// Capacity is the fleet's slots in use and free (#81).
	Capacity store.Capacity
}

// fleetVersions is the "Fleet versions" tile: the distinct versions run by
// active and paused workers (pending ones are unvetted self-reports,
// rejected ones gone), with how many run each.
type fleetVersions struct {
	Server  string         // the server's own version
	Workers []versionCount // whisper-network versions, compared with Server
	Engines []versionCount // whisper.cpp versions
	// EngineMix is set when workers run more than one engine version,
	// which mixes engines within a transcript's provenance (§8.4).
	EngineMix bool
}

// versionCount is one distinct version and how many workers run it. An
// empty Version is a worker that reported none.
type versionCount struct {
	Version string
	Count   int
	Skew    version.Skew // against the server; whisper-network versions only
}

// countVersions tallies the fleet's versions, most common first, then by
// version.
func countVersions(workers []store.Worker, server string) fleetVersions {
	out := fleetVersions{Server: server}
	tally := func(m map[string]int) []versionCount {
		var vs []versionCount
		for v, n := range m {
			vs = append(vs, versionCount{Version: v, Count: n})
		}
		slices.SortFunc(vs, func(a, b versionCount) int {
			if a.Count != b.Count {
				return b.Count - a.Count
			}
			return strings.Compare(a.Version, b.Version)
		})
		return vs
	}
	wn, eng := map[string]int{}, map[string]int{}
	for i := range workers {
		wk := &workers[i]
		if wk.State != api.WorkerActive && wk.State != api.WorkerPaused {
			continue
		}
		wn[wk.Facts.Version]++
		eng[wk.Facts.EngineVersion]++
	}
	out.Workers, out.Engines = tally(wn), tally(eng)
	for i := range out.Workers {
		out.Workers[i].Skew = version.Compare(out.Workers[i].Version, server)
	}
	known := 0
	for _, e := range out.Engines {
		if e.Version != "" {
			known++
		}
	}
	out.EngineMix = known > 1
	return out
}

// queueStatRow is one queue on the dashboard.
type queueStatRow struct {
	Name                                               string
	Weight                                             int
	Fetching, Preparing, Queued, Running, Done, Failed int
	PendingTasks, LeasedTasks                          int
	BacklogCost                                        float64
	OldestPending                                      time.Duration
	EligibleWorkers                                    int
	// EligibleSlots and EligibleBusy are the slots of the eligible workers
	// and how many are in use (#81).
	EligibleSlots, EligibleBusy int
	// Involved queues are backlogged, running or recently served; the
	// configured share is their weight over the involved queues' total,
	// which is what the scheduler converges to (§8.2).
	Involved        bool
	ConfiguredShare float64 // percent
	ActualShare     float64 // percent of cost served in the window
}

func (u *UI) handleDashboard(w http.ResponseWriter, r *http.Request, rc *reqCtx) error {
	var v dashboardView
	if rc.can(auth.ActViewMetricsUI, auth.Resource{}) {
		st, err := u.stats(r.Context())
		if err != nil {
			return err
		}
		v.Stats = st
	} else {
		qs, err := u.srv.Store().ListQueues(r.Context())
		if err != nil {
			return err
		}
		for _, q := range qs {
			if auth.VisibilityOn(rc.P.Principal, q.Name) != auth.SeesNone {
				v.Queues = append(v.Queues, q.Name)
			}
		}
	}
	u.render(w, r, rc, http.StatusOK, "dashboard", "Dashboard", v)
	return nil
}

// handleDashboardStats returns the statistics block alone, for the
// dashboard's periodic htmx refresh.
func (u *UI) handleDashboardStats(w http.ResponseWriter, r *http.Request, rc *reqCtx) error {
	if !rc.can(auth.ActViewMetricsUI, auth.Resource{}) {
		return u.deny(r, rc, "admin, queue_admin or worker_admin required")
	}
	st, err := u.stats(r.Context())
	if err != nil {
		return err
	}
	u.renderFragment(w, http.StatusOK, "dashboard", "stats", st)
	return nil
}

// stats gathers the dashboard numbers from the store's Stats snapshot, the
// worker list, recently completed jobs and the served-cost counter.
func (u *UI) stats(ctx context.Context) (*statsView, error) {
	st := u.srv.Store()
	now := u.srv.Now()
	snap, err := st.Stats(ctx, onlineWindow)
	if err != nil {
		return nil, err
	}
	out := &statsView{Generated: now, WorkersActive: snap.Workers[api.WorkerActive],
		WorkersPending: snap.Workers[api.WorkerPending], WorkersPaused: snap.Workers[api.WorkerPaused]}

	rows := make([]queueStatRow, len(snap.Queues))
	index := make(map[string]*queueStatRow, len(rows))
	for i, q := range snap.Queues {
		rows[i] = queueStatRow{Name: q.Name, Weight: q.Weight, PendingTasks: q.PendingTasks,
			BacklogCost: q.BacklogCost, OldestPending: q.OldestPending, EligibleWorkers: q.EligibleWorkers,
			EligibleSlots: q.EligibleSlots, EligibleBusy: q.EligibleSlotsBusy}
		index[q.Name] = &rows[i]
	}
	for _, j := range snap.Jobs {
		row := index[j.Queue]
		if row == nil {
			continue
		}
		switch j.State {
		case api.JobFetching:
			row.Fetching = j.Count
		case api.JobPreparing:
			row.Preparing = j.Count
		case api.JobQueued:
			row.Queued = j.Count
		case api.JobRunning:
			row.Running = j.Count
		case api.JobDone:
			row.Done = j.Count
		case api.JobFailed:
			row.Failed = j.Count
		}
	}
	for _, t := range snap.Tasks {
		if t.State == api.TaskLeased {
			out.RunningTasks += t.Count
			if row := index[t.Queue]; row != nil {
				row.LeasedTasks += t.Count
			}
		}
	}

	served, span := u.share.observe(now)
	out.ShareSpan = span
	var weights, total float64
	for i := range rows {
		row := &rows[i]
		out.PendingTasks += row.PendingTasks
		row.Involved = row.PendingTasks > 0 || row.LeasedTasks > 0 || served[row.Name] > 0
		if row.Involved {
			weights += float64(row.Weight)
		}
		total += served[row.Name]
	}
	for i := range rows {
		row := &rows[i]
		if row.Involved && weights > 0 {
			row.ConfiguredShare = 100 * float64(row.Weight) / weights
		}
		if total > 0 {
			row.ActualShare = 100 * served[row.Name] / total
		}
	}
	out.Queues = rows

	workers, err := st.ListWorkers(ctx)
	if err != nil {
		return nil, err
	}
	for i := range workers {
		if isOnline(&workers[i], now) {
			out.WorkersOnline++
		}
	}
	out.Versions = countVersions(workers, u.srv.Version())
	out.Capacity = store.FleetCapacity(workers, now, onlineWindow, st.PrepareMinSandbox())
	if out.AudioHoursPerHour, err = u.audioHoursPerHour(ctx, now); err != nil {
		return nil, err
	}
	return out, nil
}

// isOnline reports whether an active worker has been seen recently.
func isOnline(w *store.Worker, now time.Time) bool {
	return w.State == api.WorkerActive && w.LastSeenAt != nil && now.Sub(*w.LastSeenAt) <= onlineWindow
}

// audioHoursPerHour is the audio transcribed by tasks completed in the last
// throughputWindow, per hour. Each task counts when it completes, whatever
// later happens to its job (#87).
func (u *UI) audioHoursPerHour(ctx context.Context, now time.Time) (float64, error) {
	total, _, err := u.srv.Store().ThroughputSince(ctx, now.Add(-throughputWindow))
	if err != nil {
		return 0, err
	}
	return total.Audio.Hours() / throughputWindow.Hours(), nil
}

package obs

import (
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

// Snapshot is the database-derived part of the metric catalog. The server
// builds one periodically from its store and hands it to Metrics.SetSnapshot;
// every scrape then renders the latest one. A series absent from the next
// snapshot disappears from the output, so retired queues and workers do not
// linger. Slices need not be sorted. Labels never carry job, lease or client
// IDs (DESIGN.md §10.2 cardinality rules).
type Snapshot struct {
	Jobs   []JobCount
	Tasks  []TaskCount
	Queues []QueueStat
	// Workers counts workers by state ("pending", "active", ...).
	Workers []WorkerCount
	// WorkerInfo lists per-worker readings, one entry per known worker.
	WorkerInfo []WorkerStat
	Storage    StorageBytes
	// DataFreeBytes is the free space on the data directory's filesystem.
	DataFreeBytes int64
	// RetryableFailedJobs counts failed jobs whose media is still kept, so
	// they can be retried.
	RetryableFailedJobs int
	// FleetSlots is the fleet's capacity (whisper_fleet_slots).
	FleetSlots []FleetSlots
	// FetchCooldowns counts the fetch cooldowns in force by site label
	// (whisper_fetch_cooldowns), and FetchBlocked the pending fetch tasks
	// pacing holds back from every downloader (whisper_fetch_blocked_tasks).
	// Site labels are policy site keys or "other", never URLs.
	FetchCooldowns, FetchBlocked []SiteCount
}

// SiteCount is a count for one fetch site label.
type SiteCount struct {
	Site string
	N    int
}

// FleetSlots is the number of worker slots in one state ("in_use",
// "available", "deferred", "paused") able to take one kind of task ("all",
// "transcribe", "prepare").
type FleetSlots struct {
	State, Kind string
	N           int
}

// JobCount is the number of jobs in one queue and state.
type JobCount struct {
	Queue, State string
	N            int
}

// TaskCount is the number of tasks in one queue, kind and state.
type TaskCount struct {
	Queue, Kind, State string
	N                  int
}

// QueueStat holds per-queue scheduler gauges.
type QueueStat struct {
	Queue string
	// BacklogCost is the pending cost, in model-weighted audio seconds.
	BacklogCost float64
	// OldestPending is the age of the oldest pending task; zero when none.
	OldestPending time.Duration
	Weight        float64
	VTime         float64
	// EligibleWorkers counts active workers granted this queue that were
	// seen recently (the "no eligible worker" alert).
	EligibleWorkers int
}

// WorkerCount is the number of workers in one state.
type WorkerCount struct {
	State string
	N     int
}

// WorkerStat describes one worker. The descriptive fields become the labels of
// whisper_worker_info; the rest are gauges labelled by worker ID only.
type WorkerStat struct {
	ID            string
	Hostname      string
	Description   string
	Version       string // whisper-network version
	EngineVersion string
	FFmpegVersion string
	SandboxLevel  string
	Arch          string

	LastSeen  time.Time
	Slots     int
	SlotsBusy int
	// FailureStreak is the circuit breaker's failure streak; AutoPaused is
	// true while the breaker holds the worker paused (DESIGN.md §4.2).
	FailureStreak int
	AutoPaused    bool
	// The remaining fields come from heartbeat gauges.
	ChildRSSBytes int64
	Load1         float64
	CPUThrottled  time.Duration
	// Disk readings; nil when the worker did not report them (an older
	// worker, or a failed probe), and then no series is written.
	ModelsBytes, WorkBytes, StateFreeBytes *int64
	// HostLoad is the host CPU utilisation in percent, excluding the
	// worker's own processes, and ClaimsDeferred the worker's count of
	// claims withheld for it; nil when not reported.
	HostLoad       *float64
	ClaimsDeferred *int64
}

// StorageBytes is the stored size per blob kind. Failed is the part of
// Original and Canonical held for failed jobs (failed-job retention).
type StorageBytes struct {
	Original, Canonical, Transcript, Failed int64
}

// SetSnapshot replaces the snapshot rendered by gauges. It is safe to call
// concurrently with scrapes.
func (m *Metrics) SetSnapshot(s Snapshot) { m.snap.Store(&s) }

// writeSnapshot renders the current snapshot in Prometheus text format.
func (m *Metrics) writeSnapshot(w io.Writer) {
	if v := m.version.Load(); v != nil {
		metrics.WriteGaugeFloat64(w, series("whisper_server_info", "version", *v), 1)
	}
	if n := m.invalidPolicies.Load(); n != nil {
		metrics.WriteGaugeFloat64(w, "whisper_fetch_invalid_policies", float64(*n))
	}
	s := m.snap.Load()
	if s == nil {
		return
	}
	g := func(name string, v float64, kv ...string) {
		metrics.WriteGaugeFloat64(w, series(name, kv...), v)
	}

	jobs := append([]JobCount(nil), s.Jobs...)
	sort.Slice(jobs, func(i, j int) bool {
		return less2(jobs[i].Queue, jobs[i].State, jobs[j].Queue, jobs[j].State)
	})
	for _, j := range jobs {
		g("whisper_jobs", float64(j.N), "queue", j.Queue, "state", j.State)
	}

	tasks := append([]TaskCount(nil), s.Tasks...)
	sort.Slice(tasks, func(i, j int) bool {
		a, b := tasks[i], tasks[j]
		return less2(a.Queue+"\x00"+a.Kind, a.State, b.Queue+"\x00"+b.Kind, b.State)
	})
	for _, t := range tasks {
		g("whisper_tasks", float64(t.N), "queue", t.Queue, "kind", t.Kind, "state", t.State)
	}

	queues := append([]QueueStat(nil), s.Queues...)
	sort.Slice(queues, func(i, j int) bool { return queues[i].Queue < queues[j].Queue })
	for _, c := range []struct {
		name string
		val  func(QueueStat) float64
	}{
		{"whisper_queue_backlog_cost", func(q QueueStat) float64 { return q.BacklogCost }},
		{"whisper_queue_oldest_pending_seconds", func(q QueueStat) float64 { return q.OldestPending.Seconds() }},
		{"whisper_queue_weight", func(q QueueStat) float64 { return q.Weight }},
		{"whisper_sched_vtime", func(q QueueStat) float64 { return q.VTime }},
		{"whisper_queue_eligible_workers", func(q QueueStat) float64 { return float64(q.EligibleWorkers) }},
	} {
		for _, q := range queues {
			g(c.name, c.val(q), "queue", q.Queue)
		}
	}

	ws := append([]WorkerCount(nil), s.Workers...)
	sort.Slice(ws, func(i, j int) bool { return ws[i].State < ws[j].State })
	for _, c := range ws {
		g("whisper_workers", float64(c.N), "state", c.State)
	}

	slots := append([]FleetSlots(nil), s.FleetSlots...)
	sort.Slice(slots, func(i, j int) bool {
		return less2(slots[i].Kind, slots[i].State, slots[j].Kind, slots[j].State)
	})
	for _, c := range slots {
		g("whisper_fleet_slots", float64(c.N), "kind", c.Kind, "state", c.State)
	}

	for _, c := range []struct {
		name string
		in   []SiteCount
	}{{"whisper_fetch_cooldowns", s.FetchCooldowns}, {"whisper_fetch_blocked_tasks", s.FetchBlocked}} {
		sc := append([]SiteCount(nil), c.in...)
		sort.Slice(sc, func(i, j int) bool { return sc[i].Site < sc[j].Site })
		for _, x := range sc {
			g(c.name, float64(x.N), "site", x.Site)
		}
	}

	m.writeWorkers(w, s.WorkerInfo)

	g("whisper_storage_bytes", float64(s.Storage.Original), "kind", "original")
	g("whisper_storage_bytes", float64(s.Storage.Canonical), "kind", "canonical")
	g("whisper_storage_bytes", float64(s.Storage.Transcript), "kind", "transcript")
	g("whisper_storage_bytes", float64(s.Storage.Failed), "kind", "failed")
	g("whisper_data_free_bytes", float64(s.DataFreeBytes))
	g("whisper_failed_jobs_retryable", float64(s.RetryableFailedJobs))
}

func (m *Metrics) writeWorkers(w io.Writer, in []WorkerStat) {
	ws := append([]WorkerStat(nil), in...)
	sort.Slice(ws, func(i, j int) bool { return ws[i].ID < ws[j].ID })
	type col struct {
		name string
		val  func(WorkerStat) float64
	}
	cols := []col{
		{"whisper_worker_last_seen_timestamp_seconds", func(x WorkerStat) float64 {
			if x.LastSeen.IsZero() {
				return 0
			}
			return float64(x.LastSeen.UnixNano()) / 1e9
		}},
		{"whisper_worker_slots", func(x WorkerStat) float64 { return float64(x.Slots) }},
		{"whisper_worker_slots_busy", func(x WorkerStat) float64 { return float64(x.SlotsBusy) }},
		{"whisper_worker_child_rss_bytes", func(x WorkerStat) float64 { return float64(x.ChildRSSBytes) }},
		{"whisper_worker_load1", func(x WorkerStat) float64 { return x.Load1 }},
		{"whisper_worker_cpu_throttled_seconds", func(x WorkerStat) float64 { return x.CPUThrottled.Seconds() }},
		{"whisper_worker_failure_streak", func(x WorkerStat) float64 { return float64(x.FailureStreak) }},
		{"whisper_worker_auto_paused", func(x WorkerStat) float64 {
			if x.AutoPaused {
				return 1
			}
			return 0
		}},
	}
	for _, x := range ws {
		metrics.WriteGaugeFloat64(w, series("whisper_worker_info",
			"worker", x.ID, "hostname", x.Hostname, "description", x.Description,
			"version", x.Version, "engine_version", x.EngineVersion, "ffmpeg_version", x.FFmpegVersion,
			"sandbox_level", x.SandboxLevel, "arch", x.Arch), 1)
	}
	for _, c := range cols {
		for _, x := range ws {
			fmt.Fprintf(w, "%s %g\n", series(c.name, "worker", x.ID), c.val(x))
		}
	}
	for _, c := range []struct {
		name string
		val  func(WorkerStat) *int64
	}{
		{"whisper_worker_models_bytes", func(x WorkerStat) *int64 { return x.ModelsBytes }},
		{"whisper_worker_work_bytes", func(x WorkerStat) *int64 { return x.WorkBytes }},
		{"whisper_worker_state_free_bytes", func(x WorkerStat) *int64 { return x.StateFreeBytes }},
	} {
		for _, x := range ws {
			if v := c.val(x); v != nil {
				fmt.Fprintf(w, "%s %d\n", series(c.name, "worker", x.ID), *v)
			}
		}
	}
	for _, x := range ws {
		if x.HostLoad != nil {
			fmt.Fprintf(w, "%s %g\n", series("whisper_worker_host_load", "worker", x.ID), *x.HostLoad)
		}
	}
	for _, x := range ws {
		if x.ClaimsDeferred != nil {
			fmt.Fprintf(w, "%s %d\n", series("whisper_worker_claims_deferred_total", "worker", x.ID), *x.ClaimsDeferred)
		}
	}
}

func less2(a1, a2, b1, b2 string) bool {
	if a1 != b1 {
		return a1 < b1
	}
	return a2 < b2
}

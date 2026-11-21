package server

import (
	"context"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/store"
)

// eligibleWindow is how recently a worker must have been seen to count in
// whisper_queue_eligible_workers (the "no eligible worker" alert, §10.2).
const eligibleWindow = 10 * time.Minute

// refreshStats rebuilds the database-derived metric snapshot.
func (s *Server) refreshStats(ctx context.Context) {
	st, err := s.st.Stats(ctx, eligibleWindow)
	if err != nil {
		if ctx.Err() == nil {
			s.log.Warn().Err(err).Msg("stats refresh failed")
		}
		return
	}
	ws, err := s.st.ListWorkers(ctx)
	if err != nil {
		if ctx.Err() == nil {
			s.log.Warn().Err(err).Msg("stats refresh: listing workers failed")
		}
		return
	}
	var snap obs.Snapshot
	for _, j := range st.Jobs {
		snap.Jobs = append(snap.Jobs, obs.JobCount{Queue: j.Queue, State: string(j.State), N: j.Count})
	}
	for _, t := range st.Tasks {
		snap.Tasks = append(snap.Tasks, obs.TaskCount{Queue: t.Queue, Kind: string(t.Kind), State: string(t.State), N: t.Count})
	}
	for _, q := range st.Queues {
		snap.Queues = append(snap.Queues, obs.QueueStat{Queue: q.Name, BacklogCost: q.BacklogCost,
			OldestPending: q.OldestPending, Weight: float64(q.Weight), VTime: q.VTime, EligibleWorkers: q.EligibleWorkers})
	}
	for state, n := range st.Workers {
		snap.Workers = append(snap.Workers, obs.WorkerCount{State: string(state), N: n})
	}
	snap.FleetSlots = fleetSlots(store.FleetCapacity(ws, s.now(), eligibleWindow, s.st.PrepareMinSandbox()))
	for i := range ws {
		wk := &ws[i]
		if wk.State == api.WorkerRejected || wk.State == api.WorkerPending {
			// Rejected workers are kept for the audit trail and pending
			// ones are unvetted self-reports (anyone can register): neither
			// belongs in per-worker series.
			continue
		}
		ws := obs.WorkerStat{ID: wk.ID, Hostname: wk.Facts.Hostname, Description: wk.Facts.Description,
			Version: wk.Facts.Version, EngineVersion: wk.Facts.EngineVersion, FFmpegVersion: wk.Facts.FFmpegVersion,
			SandboxLevel: string(wk.Facts.SandboxLevel), Arch: wk.Facts.Arch, Slots: wk.Facts.Slots,
			FailureStreak: wk.FailureStreak, AutoPaused: wk.State == api.WorkerPaused && wk.PauseReason != ""}
		if wk.LastSeenAt != nil {
			ws.LastSeen = *wk.LastSeenAt
		}
		if g := gaugesOf(wk.Gauges); g != nil {
			ws.SlotsBusy, ws.ChildRSSBytes, ws.Load1 = g.SlotsBusy, g.ChildRSSBytes, g.Load1
			ws.CPUThrottled = secs(g.CPUThrottledSecs)
			ws.ModelsBytes, ws.WorkBytes, ws.StateFreeBytes = g.ModelsBytes, g.WorkBytes, g.StateFreeBytes
			ws.HostLoad, ws.ClaimsDeferred = g.HostLoad, g.ClaimsDeferred
		}
		snap.WorkerInfo = append(snap.WorkerInfo, ws)
	}
	if u, err := s.blobs.Usage(); err == nil {
		snap.Storage = obs.StorageBytes{Original: u.Original, Canonical: u.Canonical, Transcript: u.Transcript}
	} else {
		s.log.Warn().Err(err).Msg("stats refresh: blob usage failed")
	}
	if free, err := s.freeBytes(s.cfg.DataDir); err == nil {
		snap.DataFreeBytes = free
	}
	if fs, err := s.st.FailedJobStats(ctx); err == nil {
		snap.Storage.Failed, snap.RetryableFailedJobs = fs.Bytes, fs.Retryable
	} else if ctx.Err() == nil {
		s.log.Warn().Err(err).Msg("stats refresh: failed job stats failed")
	}
	s.fetchStats(ctx, &snap)
	s.metrics.SetSnapshot(snap)
}

// fetchStats adds the fetch pacing gauges to snap, and re-checks the fetch
// policies (whisper_fetch_invalid_policies).
func (s *Server) fetchStats(ctx context.Context, snap *obs.Snapshot) {
	label := fetchSiteLabels(s.checkFetchPolicies(ctx))
	now := s.now()
	bySite := func(m map[string]int) []obs.SiteCount {
		agg := map[string]int{}
		for site, n := range m {
			agg[label(site)] += n
		}
		out := make([]obs.SiteCount, 0, len(agg))
		for site, n := range agg {
			out = append(out, obs.SiteCount{Site: site, N: n})
		}
		return out
	}
	if cds, err := s.st.FetchCooldowns(ctx, now); err == nil {
		m := map[string]int{}
		for _, c := range cds {
			m[c.Site]++
		}
		snap.FetchCooldowns = bySite(m)
	} else if ctx.Err() == nil {
		s.log.Warn().Err(err).Msg("stats refresh: fetch cooldowns failed")
	}
	if b, err := s.st.FetchBlockedTasks(ctx, now); err == nil {
		snap.FetchBlocked = bySite(b)
	} else if ctx.Err() == nil {
		s.log.Warn().Err(err).Msg("stats refresh: blocked fetch tasks failed")
	}
}

// fleetSlots is the whisper_fleet_slots series of c: in use, available and
// deferred slots per kind, and paused slots (kind "all").
func fleetSlots(c store.Capacity) []obs.FleetSlots {
	var out []obs.FleetSlots
	for _, k := range []struct {
		kind string
		n    store.SlotCount
	}{{"all", c.All}, {"transcribe", c.Transcribe}, {"prepare", c.Prepare}} {
		out = append(out, obs.FleetSlots{State: "in_use", Kind: k.kind, N: k.n.InUse},
			obs.FleetSlots{State: "available", Kind: k.kind, N: k.n.Available()},
			obs.FleetSlots{State: "deferred", Kind: k.kind, N: k.n.Deferred})
	}
	return append(out, obs.FleetSlots{State: "paused", Kind: "all", N: c.Paused})
}

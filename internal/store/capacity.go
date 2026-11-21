package store

import (
	"encoding/json"
	"slices"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
)

// Capacity is how much work the fleet can take right now (DESIGN.md §6.3,
// whisper_fleet_slots), from each worker's reported slots and its last
// heartbeat's busy slots. The unit is the worker slot; it is kept apart from
// the callers so it can become a resource view without reshaping them.
type Capacity struct {
	// All counts the slots of online active workers; Transcribe and
	// Prepare those of the workers able to take each kind (prepare needs
	// the prepare kind and the minimum sandbox level). A worker offering
	// both kinds counts in both.
	All, Transcribe, Prepare SlotCount
	// Slots of workers not taking work, counted apart: paused, pending
	// approval, and active but not seen recently.
	Paused, Pending, Offline int
}

// SlotCount is slots in use and free. Deferred are free slots on hosts that
// are deferring claims for load; Available are the rest of the free slots.
type SlotCount struct {
	Total, InUse, Deferred int
}

// Available is the free slots that can take work now.
func (c SlotCount) Available() int { return c.Total - c.InUse - c.Deferred }

func (c *SlotCount) add(total, busy int, deferred bool) {
	c.Total += total
	c.InUse += busy
	if deferred {
		c.Deferred += total - busy
	}
}

// FleetCapacity tallies ws. A worker is online if active and seen within
// seenWithin of now; prepareMin is the store's PrepareMinSandbox.
func FleetCapacity(ws []Worker, now time.Time, seenWithin time.Duration, prepareMin api.SandboxLevel) Capacity {
	var c Capacity
	for i := range ws {
		w := &ws[i]
		slots := max(w.Facts.Slots, 0)
		switch {
		case w.State == api.WorkerPaused:
			c.Paused += slots
			continue
		case w.State == api.WorkerPending:
			c.Pending += slots
			continue
		case w.State != api.WorkerActive:
			continue
		case w.LastSeenAt == nil || now.Sub(*w.LastSeenAt) > seenWithin:
			c.Offline += slots
			continue
		}
		busy, deferred := 0, false
		if len(w.Gauges) > 0 {
			var g api.Gauges
			if json.Unmarshal(w.Gauges, &g) == nil {
				busy, deferred = min(max(g.SlotsBusy, 0), slots), g.DeferredForLoad
			}
		}
		c.All.add(slots, busy, deferred)
		if w.Kind != api.WorkerDownloader && slices.Contains(w.Facts.Kinds, api.KindTranscribe) {
			c.Transcribe.add(slots, busy, deferred)
		}
		if CanPrepare(w.Facts, prepareMin) {
			c.Prepare.add(slots, busy, deferred)
		}
	}
	return c
}

// CanPrepare reports whether a worker with facts f may be offered prepare
// tasks when they need sandbox level prepareMin (as Claim decides: never a
// downloader).
func CanPrepare(f api.WorkerFacts, prepareMin api.SandboxLevel) bool {
	return f.Kind != api.WorkerDownloader && slices.Contains(f.Kinds, api.KindPrepare) && f.SandboxLevel.Rank() >= prepareMin.Rank()
}

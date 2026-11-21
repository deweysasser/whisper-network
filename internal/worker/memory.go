package worker

import (
	"context"
	"slices"
	"sync"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/models"
)

// memBudget is the per-claim memory accounting of DESIGN.md §7.3. Before a
// slot claims, it reserves the most it could need for anything it offers;
// once the lease names the work, the reservation shrinks to what that work
// needs, and it is returned when the task ends. Slots therefore never
// collectively accept more than the budget, at the price of an idle
// long-polling slot holding a provisional reservation.
type memBudget struct {
	mu       sync.Mutex
	total    int64
	reserved int64
	changed  chan struct{} // closed and replaced whenever memory is freed
}

func newMemBudget(total int64) *memBudget {
	return &memBudget{total: total, changed: make(chan struct{})}
}

// reservation is memory held by one slot.
type reservation struct {
	b      *memBudget
	amount int64
}

// offer is what a slot may claim right now.
type offer struct {
	kinds  []api.TaskKind
	models []string
}

func (o offer) empty() bool {
	return !slices.Contains(o.kinds, api.KindPrepare) && !slices.Contains(o.kinds, api.KindFetch) && len(o.models) == 0
}

// reserve computes what fits in unreserved memory and reserves the largest
// amount any of it could need. With nothing fitting it waits for memory to be
// freed, or returns ctx's error.
func (b *memBudget) reserve(ctx context.Context, kinds []api.TaskKind, candidates []models.Model, prepareMem, fetchMem int64) (offer, *reservation, error) {
	for {
		b.mu.Lock()
		free := b.total - b.reserved
		var o offer
		var need int64
		if slices.Contains(kinds, api.KindPrepare) && prepareMem <= free {
			o.kinds = append(o.kinds, api.KindPrepare)
			need = prepareMem
		}
		if slices.Contains(kinds, api.KindFetch) && fetchMem <= free {
			o.kinds = append(o.kinds, api.KindFetch)
			need = max(need, fetchMem)
		}
		if slices.Contains(kinds, api.KindTranscribe) {
			for _, m := range candidates {
				if fits(m, free) {
					o.models = append(o.models, m.Name)
					need = max(need, m.PeakRSS)
				}
			}
			if len(o.models) > 0 {
				o.kinds = append(o.kinds, api.KindTranscribe)
			}
		}
		if !o.empty() {
			b.reserved += need
			b.mu.Unlock()
			return o, &reservation{b: b, amount: need}, nil
		}
		wait := b.changed
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return offer{}, nil, ctx.Err()
		case <-wait:
		}
	}
}

// shrink lowers the reservation to amount (never raises it).
func (r *reservation) shrink(amount int64) {
	r.b.mu.Lock()
	defer r.b.mu.Unlock()
	if amount < r.amount {
		r.b.reserved -= r.amount - amount
		r.amount = amount
		r.b.notifyLocked()
	}
}

// release returns the whole reservation. It is idempotent.
func (r *reservation) release() { r.shrink(0) }

func (b *memBudget) notifyLocked() {
	close(b.changed)
	b.changed = make(chan struct{})
}

// inUse reports the reserved total (for tests and gauges).
func (b *memBudget) inUse() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.reserved
}

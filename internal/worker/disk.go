package worker

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/media"
)

// diskTTL is how long a disk reading is reused. Gauges are read by every
// heartbeat and every claim of every slot; the walk behind them is small (a
// few model files, a few files per running lease) but need not repeat more
// often than this.
const diskTTL = 5 * time.Second

// diskReading is one measurement of the state directory.
type diskReading struct {
	models, work int64
	free         int64
	freeOK       bool
	cached       []api.CachedModel
}

// diskMeter measures the model cache, the lease work directories and the free
// space of the state filesystem, caching the result for diskTTL.
type diskMeter struct {
	state, models, work string
	catalog             Catalog
	freeBytes           func(string) (int64, error)
	now                 func() time.Time

	mu   sync.Mutex
	at   time.Time
	last diskReading
}

func newDiskMeter(state, models, work string, cat Catalog, free func(string) (int64, error)) *diskMeter {
	return &diskMeter{state: state, models: models, work: work, catalog: cat, freeBytes: free, now: time.Now}
}

// read returns a reading at most diskTTL old.
func (d *diskMeter) read() diskReading {
	d.mu.Lock()
	defer d.mu.Unlock()
	if now := d.now(); d.at.IsZero() || now.Sub(d.at) >= diskTTL {
		d.last = d.measure()
		d.at = now
	}
	return d.last
}

// invalidate makes the next read measure again; called on changes the
// worker makes itself (a lease directory created or removed, a model
// downloaded), so gauges do not lag them by the TTL.
func (d *diskMeter) invalidate() {
	d.mu.Lock()
	d.at = time.Time{}
	d.mu.Unlock()
}

// fresh measures now, for decisions that must not use a stale reading.
func (d *diskMeter) fresh() diskReading {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.last = d.measure()
	d.at = d.now()
	return d.last
}

func (d *diskMeter) measure() diskReading {
	var r diskReading
	r.models, r.cached = d.measureModels()
	r.work = treeSize(d.work)
	if n, err := d.freeBytes(d.state); err == nil {
		r.free, r.freeOK = n, true
	}
	return r
}

// measureModels sizes the model directory (flat: one file per model, plus
// ".part" temporaries) and names the complete files the catalog knows.
func (d *diskMeter) measureModels() (int64, []api.CachedModel) {
	ents, err := os.ReadDir(d.models)
	if err != nil {
		return 0, nil
	}
	byFile := map[string]string{}
	for _, m := range d.catalog {
		byFile[m.File] = m.Name
	}
	var total int64
	var cached []api.CachedModel
	for _, e := range ents {
		fi, err := e.Info()
		if err != nil || !fi.Mode().IsRegular() {
			continue
		}
		total += fi.Size()
		if name, ok := byFile[e.Name()]; ok && !strings.HasPrefix(e.Name(), ".") {
			cached = append(cached, api.CachedModel{Name: name, Bytes: fi.Size()})
		}
	}
	slices.SortFunc(cached, func(a, b api.CachedModel) int { return strings.Compare(a.Name, b.Name) })
	return total, cached
}

// treeSize sums the regular files under root. Files vanishing during the
// walk (a lease finishing) are skipped.
func treeSize(root string) int64 {
	var n int64
	_ = filepath.WalkDir(root, func(_ string, e fs.DirEntry, err error) error {
		if err != nil || !e.Type().IsRegular() {
			return nil
		}
		if fi, err := e.Info(); err == nil {
			n += fi.Size()
		}
		return nil
	})
	return n
}

// gauges adds the reading to g.
func (r diskReading) gauges(g *api.Gauges) {
	g.ModelsBytes, g.WorkBytes = ptr(r.models), ptr(r.work)
	if r.freeOK {
		g.StateFreeBytes = ptr(r.free)
	}
	g.CachedModels = r.cached
}

func ptr[T any](v T) *T { return &v }

// ---------------------------------------------------------------------------
// Admission (DESIGN.md §7.2)

// diskBlockMax bounds how long a kind stays withheld after a refusal, so a
// smaller task of that kind can be tried even if space never frees.
const diskBlockMax = 10 * time.Minute

// diskGate admits leases by disk space. A lease is admitted when its
// expected footprint fits in
//
//	free − reserve − (footprints of running leases − bytes they wrote so far)
//
// so concurrent slots cannot together promise the same space. A kind whose
// lease was refused is withheld from claims until the space it needed is
// available or diskBlockMax passes.
type diskGate struct {
	meter   *diskMeter
	reserve int64
	now     func() time.Time

	mu        sync.Mutex
	committed int64
	blocked   map[api.TaskKind]diskBlock
}

type diskBlock struct {
	need  int64
	until time.Time
}

func newDiskGate(m *diskMeter, reserve int64) *diskGate {
	return &diskGate{meter: m, reserve: reserve, now: time.Now, blocked: map[api.TaskKind]diskBlock{}}
}

// availableLocked is the space a new lease may use given reading r; g.mu is
// held.
func (g *diskGate) availableLocked(r diskReading) int64 {
	return r.free - g.reserve - max(0, g.committed-r.work)
}

// admit reserves need bytes for a lease, or reports how much space was
// available if it does not fit. Without a free-space reading it admits: an
// unknown disk is not a reason to refuse work.
func (g *diskGate) admit(kind api.TaskKind, need int64) (release func(), avail int64, ok bool) {
	r := g.meter.fresh()
	g.mu.Lock()
	defer g.mu.Unlock()
	if r.freeOK {
		avail = g.availableLocked(r)
		if need > avail {
			g.blocked[kind] = diskBlock{need: need, until: g.now().Add(diskBlockMax)}
			return nil, avail, false
		}
	}
	delete(g.blocked, kind)
	g.committed += need
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			g.committed -= need
			g.mu.Unlock()
		})
	}, avail, true
}

// filter drops withheld kinds from an offer.
func (g *diskGate) filter(o offer) offer {
	g.mu.Lock()
	n := len(g.blocked)
	g.mu.Unlock()
	if n == 0 {
		return o
	}
	// A fresh reading: this runs only while something is withheld, and
	// the point is to notice space freeing up.
	r := g.meter.fresh()
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now()
	withheld := func(k api.TaskKind) bool {
		b, ok := g.blocked[k]
		if !ok {
			return false
		}
		if now.After(b.until) || !r.freeOK || b.need <= g.availableLocked(r) {
			delete(g.blocked, k)
			return false
		}
		return true
	}
	var out offer
	for _, k := range o.kinds {
		if withheld(k) {
			continue
		}
		out.kinds = append(out.kinds, k)
		if k == api.KindTranscribe {
			out.models = o.models
		}
	}
	return out
}

// diskSlack covers a task's small files: whisper's JSON output and the
// argument file of a transcribe task.
const diskSlack = 16 * mib

// leaseDisk is the disk a lease is expected to need: its input, its output
// and any model it must download first.
//
//   - prepare: the original (input_bytes) plus the canonical FLAC, which is
//     at most the original's size (media containers hold compressed audio at
//     or above FLAC's 16 kHz mono rate, or video) and at most
//     prepareOutputCap, the child's RLIMIT_FSIZE. The cap is what keeps a
//     large video from being charged twice its size: 12 GiB of video
//     needs 12 GiB plus about 1.6 GiB, not 24 GiB.
//   - transcribe: the WAV (44 + 2 × audio_samples) plus diskSlack, plus the
//     model and VAD model if not cached.
func (w *Worker) leaseDisk(l *api.Lease) int64 {
	switch l.Kind {
	case api.KindPrepare:
		in := max(l.InputBytes, 0)
		return in + min(in, w.prepareOutputCap(l)) + diskSlack
	case api.KindTranscribe:
		need := int64(len(media.WAVHeader(0))) + max(l.AudioSamples, 0)*media.BytesPerSample + diskSlack
		for _, name := range []string{l.Model, l.VADModel} {
			if m, ok := w.cfg.Catalog.Lookup(name); ok && name != "" && w.cache != nil && !w.cache.Cached(m) {
				need += m.Size
			}
		}
		return need
	}
	return diskSlack
}

// prepareOutputCap is the most a prepare child may write: the worker's
// PrepareFileSize, lowered to the server's bound on the result (from the
// lease, else the last bound the server gave). Output beyond the server's
// bound would be refused anyway, so the child is stopped there.
func (w *Worker) prepareOutputCap(l *api.Lease) int64 {
	c := w.cfg.Limits.PrepareFileSize
	srv := w.serverOutCap.Load()
	if l != nil && l.MaxOutputBytes > 0 {
		srv = l.MaxOutputBytes
	}
	if srv > 0 {
		c = min(c, srv)
	}
	return c
}

// prepareFit is the largest prepare input whose footprint (see leaseDisk)
// fits in avail bytes, given the output cap; 0 if none does.
func prepareFit(avail, outCap int64) int64 {
	a := avail - diskSlack
	switch {
	case a <= 0:
		return 0
	case a >= 2*outCap:
		return a - outCap
	}
	return a / 2
}

// prepareBounds sets the gauges telling the server which prepare tasks fit:
// PrepareMaxInputBytes for the space available to a new lease now, and
// PrepareCapacityBytes for the space the worker would have idle (its work
// directories empty, nothing committed). Without a free-space reading
// neither is set, and the server holds nothing back.
func (g *diskGate) prepareBounds(r diskReading, outCap int64, gs *api.Gauges) {
	if !r.freeOK {
		return
	}
	g.mu.Lock()
	now := g.availableLocked(r)
	g.mu.Unlock()
	fitNow := prepareFit(now, outCap)
	fitIdle := prepareFit(r.free+r.work-g.reserve, outCap)
	gs.PrepareMaxInputBytes, gs.PrepareCapacityBytes = &fitNow, &fitIdle
}

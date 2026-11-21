package obs

import (
	"strconv"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"

	"github.com/deweysasser/whisper-network/internal/api"
)

// Bucket upper bounds, in seconds unless noted. Histograms are
// PrometheusHistograms, so they expose standard cumulative `le` buckets and
// histogram_quantile works in Prometheus as well as VictoriaMetrics.
var (
	// httpBuckets spans fast API calls up to multi-gigabyte uploads.
	httpBuckets = []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300}
	// jobBuckets spans seconds to days: client-visible latency.
	jobBuckets = []float64{1, 5, 15, 60, 300, 900, 1800, 3600, 2 * 3600, 6 * 3600, 12 * 3600, 24 * 3600, 48 * 3600, 96 * 3600}
	// computeBuckets covers one task, up to the task target (default 60 min) and beyond.
	computeBuckets = []float64{1, 5, 15, 30, 60, 120, 300, 600, 1200, 1800, 3600, 7200, 14400}
	// rtfBuckets is compute seconds per audio second; below 1 is faster than real time.
	rtfBuckets = []float64{.01, .025, .05, .1, .2, .3, .5, .75, 1, 1.5, 2, 4, 8}
	// claimBuckets covers an immediate hit through a long-poll that times out.
	claimBuckets = []float64{.005, .025, .1, .5, 1, 2.5, 5, 10, 20, 30, 60, 120, 300}
	// dbBuckets covers SQLite transactions from microseconds to lock stalls.
	dbBuckets = []float64{.0001, .0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 5}
)

// Metrics is the typed face of the metric catalog (DESIGN.md §10.2). Create it
// with NewMetrics over an injected set; all methods are safe for concurrent
// use. Event methods update counters and histograms; database-derived gauges
// come from SetSnapshot.
//
// Callers must pass bounded label values: route patterns, never raw paths;
// queue and worker names, never job, lease or client IDs.
type Metrics struct {
	set  *metrics.Set
	snap atomic.Pointer[Snapshot]
	// version is the server's own version, for whisper_server_info; nil
	// until SetServerVersion.
	version atomic.Pointer[string]
	// invalidPolicies is whisper_fetch_invalid_policies; nil until
	// SetFetchInvalidPolicies.
	invalidPolicies atomic.Pointer[int]
}

// NewMetrics registers the catalog's snapshot gauges on set and returns the
// recorder. The set must not be the library's default set.
func NewMetrics(set *metrics.Set) *Metrics {
	m := &Metrics{set: set}
	set.RegisterMetricsWriter(m.writeSnapshot)
	return m
}

// SetServerVersion sets the version reported by whisper_server_info.
func (m *Metrics) SetServerVersion(v string) { m.version.Store(&v) }

// Set returns the underlying set.
func (m *Metrics) Set() *metrics.Set { return m.set }

func (m *Metrics) counter(name string, kv ...string) *metrics.Counter {
	return m.set.GetOrCreateCounter(series(name, kv...))
}

func (m *Metrics) fcounter(name string, kv ...string) *metrics.FloatCounter {
	return m.set.GetOrCreateFloatCounter(series(name, kv...))
}

func (m *Metrics) hist(name string, buckets []float64, kv ...string) *metrics.PrometheusHistogram {
	return m.set.GetOrCreatePrometheusHistogramExt(series(name, kv...), buckets)
}

// HTTPRequest records one served request. route is the pattern (for example
// "/job/{id}"), never the raw path.
func (m *Metrics) HTTPRequest(route, method string, code int, d time.Duration) {
	m.counter("whisper_http_requests_total", "route", route, "method", method, "code", strconv.Itoa(code)).Inc()
	m.hist("whisper_http_request_duration_seconds", httpBuckets, "route", route).Update(d.Seconds())
}

// JobCreated counts a submitted job.
func (m *Metrics) JobCreated(queue string, weight api.Weight, fastPath bool) {
	m.counter("whisper_jobs_created_total", "queue", queue, "weight", string(weight),
		"fast_path", strconv.FormatBool(fastPath)).Inc()
}

// JobFinished counts a job reaching a terminal state (done or failed) and
// records the latency clients saw: wait is submission to first lease, elapsed
// is submission to completion.
func (m *Metrics) JobFinished(queue string, outcome api.JobState, wait, elapsed time.Duration) {
	m.counter("whisper_jobs_finished_total", "queue", queue, "outcome", string(outcome)).Inc()
	m.hist("whisper_job_wait_seconds", jobBuckets, "queue", queue).Update(wait.Seconds())
	m.hist("whisper_job_elapsed_seconds", jobBuckets, "queue", queue).Update(elapsed.Seconds())
}

// ServedCost adds cost handed to workers from queue, by weight class, for the
// fairness check against whisper_queue_weight.
func (m *Metrics) ServedCost(queue string, class api.Weight, cost float64) {
	m.fcounter("whisper_sched_served_cost_total", "queue", queue, "class", string(class)).Add(cost)
}

// TaskCompute records the compute time of a finished task.
func (m *Metrics) TaskCompute(kind api.TaskKind, model string, seconds float64) {
	m.hist("whisper_task_compute_seconds", computeBuckets, "kind", string(kind), "model", model).Update(seconds)
}

// TaskRealtimeFactor records compute seconds per audio second for a finished
// transcribe task.
func (m *Metrics) TaskRealtimeFactor(model, worker string, rtf float64) {
	m.hist("whisper_task_realtime_factor", rtfBuckets, "model", model, "worker", worker).Update(rtf)
}

// LeaseEnded counts a lease ending. A high "expired" rate means sick workers
// or a TTL that is too short.
func (m *Metrics) LeaseEnded(outcome api.LeaseOutcome, kind api.TaskKind) {
	m.counter("whisper_leases_ended_total", "outcome", string(outcome), "kind", string(kind)).Inc()
}

// ClaimWait records how long a worker's claim waited for work.
func (m *Metrics) ClaimWait(d time.Duration) {
	m.hist("whisper_claim_wait_seconds", claimBuckets).Update(d.Seconds())
}

// WorkerAudio adds audio seconds transcribed by a worker.
func (m *Metrics) WorkerAudio(worker, model string, seconds float64) {
	m.fcounter("whisper_worker_audio_seconds_total", "worker", worker, "model", model).Add(seconds)
}

// WorkerAutoPaused counts the circuit breaker pausing a worker.
func (m *Metrics) WorkerAutoPaused(worker string) {
	m.counter("whisper_worker_auto_paused_total", "worker", worker).Inc()
}

// Upload adds bytes received from clients for queue.
func (m *Metrics) Upload(queue string, n int64) {
	m.counter("whisper_upload_bytes_total", "queue", queue).AddInt64(n)
}

// InputServed adds bytes of task input sent to workers; kind is the task kind.
func (m *Metrics) InputServed(kind api.TaskKind, n int64) {
	m.counter("whisper_input_served_bytes_total", "kind", string(kind)).AddInt64(n)
}

// DBTx records one database transaction named by op.
func (m *Metrics) DBTx(op string, d time.Duration) {
	m.hist("whisper_db_tx_seconds", dbBuckets, "op", op).Update(d.Seconds())
}

// FetchResult counts a fetch lease's outcome ("done", "failed", "released", "expired",
// "rate_limited", "not_media", "too_large", "invalid") by site label: a
// policy site key or "other", never a URL.
func (m *Metrics) FetchResult(site, outcome string) {
	m.counter("whisper_fetch_total", "site", site, "outcome", outcome).Inc()
}

// FetchBytes adds bytes of fetched media accepted from downloaders, by site
// label (see FetchResult).
func (m *Metrics) FetchBytes(site string, n int64) {
	m.counter("whisper_fetch_bytes_total", "site", site).AddInt64(n)
}

// SetFetchInvalidPolicies sets whisper_fetch_invalid_policies, the number of
// stored fetch policies that do not parse or validate (each blocks what it
// governs).
func (m *Metrics) SetFetchInvalidPolicies(n int) { m.invalidPolicies.Store(&n) }

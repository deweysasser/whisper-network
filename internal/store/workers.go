package store

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
)

// Worker is a registered worker machine.
type Worker struct {
	ID           string
	State        api.WorkerState
	Facts        api.WorkerFacts
	RegisteredAt time.Time
	DecidedBy    string
	DecidedAt    *time.Time
	LastSeenAt   *time.Time
	// Gauges is the JSON of the latest heartbeat gauges, nil if none.
	Gauges json.RawMessage
	// Stats are the worker's work counters (zero if it has done nothing).
	Stats api.WorkerStats
	// FailureStreak and StreakJobs are the worker's current failure streak
	// and the distinct jobs in it (the circuit breaker, DESIGN.md §4.2).
	FailureStreak, StreakJobs int
	// PauseReason says why the system paused the worker by itself; empty
	// unless the circuit breaker paused it.
	PauseReason string
	// Kind is fixed at registration: processing (the default) or
	// downloader. Facts.Kind mirrors it.
	Kind api.WorkerKind
	// EgressKey identifies the network egress the worker fetches from;
	// "unknown" until the server sets it.
	EgressKey string
}

// Principal returns the worker's grant principal.
func (w *Worker) Principal() string { return WorkerPrincipal(w.ID) }

// RegisterWorker creates a pending worker with a server-assigned id.
func (s *Store) RegisterWorker(ctx context.Context, tokenHash string, facts api.WorkerFacts) (*Worker, error) {
	return inTx1(ctx, s, func(tx *Tx) (*Worker, error) { return tx.RegisterWorker(ctx, tokenHash, facts) })
}

// RegisterWorker is Store.RegisterWorker inside the transaction.
func (t *Tx) RegisterWorker(ctx context.Context, tokenHash string, facts api.WorkerFacts) (*Worker, error) {
	return t.registerWorker(ctx, tokenHash, facts, "")
}

// registerWorker inserts a pending worker registered from source (an
// auth.IPKey, or "" if unknown).
func (t *Tx) registerWorker(ctx context.Context, tokenHash string, facts api.WorkerFacts, source string) (*Worker, error) {
	if tokenHash == "" {
		return nil, fmt.Errorf("%w: empty token hash", ErrInvalid)
	}
	kinds, models, err := factsJSON(facts)
	if err != nil {
		return nil, err
	}
	now := fromMS(toMS(t.s.now()))
	if facts.Kind == "" {
		facts.Kind = api.WorkerProcessing
	}
	w := &Worker{ID: t.s.cfg.NewID(IDWorker), State: api.WorkerPending, Facts: facts, RegisteredAt: now,
		Kind: facts.Kind, EgressKey: "unknown"}
	_, err = t.tx.ExecContext(ctx, `INSERT INTO workers (id, token_hash, state, hostname, description, cpu_model,
		cores, mem_bytes, os, arch, engine_version, ffmpeg_version, sandbox_level, kinds, slots, models, version,
		registered_at, registered_from, kind, ytdlp_version, egress_group, egress_ip)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		w.ID, tokenHash, string(w.State), facts.Hostname, facts.Description, facts.CPUModel, facts.Cores,
		facts.MemBytes, facts.OS, facts.Arch, facts.EngineVersion, facts.FFmpegVersion, string(facts.SandboxLevel),
		kinds, facts.Slots, models, facts.Version, toMS(now), source, string(facts.Kind), facts.YtDlpVersion,
		facts.EgressGroup, facts.EgressIP)
	if isUnique(err) {
		return nil, fmt.Errorf("register worker: %w", ErrConflict)
	}
	if err != nil {
		return nil, fmt.Errorf("register worker: %w", err)
	}
	return w, nil
}

func factsJSON(f api.WorkerFacts) (kinds, models string, err error) {
	k, err := json.Marshal(nonNil(f.Kinds))
	if err != nil {
		return "", "", fmt.Errorf("worker kinds: %w", err)
	}
	m, err := json.Marshal(nonNil(f.Models))
	if err != nil {
		return "", "", fmt.Errorf("worker models: %w", err)
	}
	return string(k), string(m), nil
}

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

// workerSelect selects a worker (aliased w) with its counters.
const workerSelect = `SELECT w.id, w.state, w.hostname, w.description, w.cpu_model, w.cores, w.mem_bytes, w.os,
	w.arch, w.engine_version, w.ffmpeg_version, w.sandbox_level, w.kinds, w.slots, w.models, w.version,
	w.registered_at, w.decided_by, w.decided_at, w.last_seen_at, w.gauges, w.pause_reason,
	w.kind, w.egress_key, w.ytdlp_version, w.egress_group, w.egress_ip,
	(SELECT COALESCE(SUM(f.failures), 0) FROM worker_failure_streak f WHERE f.worker_id = w.id),
	(SELECT COUNT(*) FROM worker_failure_streak f WHERE f.worker_id = w.id), ` + workerStatsCols + `
	FROM workers w LEFT JOIN worker_stats ws ON ws.worker_id = w.id`

func scanWorker(row interface{ Scan(...any) error }) (*Worker, error) {
	var w Worker
	var f = &w.Facts
	var kinds, models string
	var registered int64
	var decidedBy, gauges, pauseReason sql.NullString
	var decidedAt, lastSeen sql.NullInt64
	var st statsScan
	if err := row.Scan(append([]any{&w.ID, &w.State, &f.Hostname, &f.Description, &f.CPUModel, &f.Cores,
		&f.MemBytes, &f.OS, &f.Arch, &f.EngineVersion, &f.FFmpegVersion, &f.SandboxLevel, &kinds, &f.Slots,
		&models, &f.Version, &registered, &decidedBy, &decidedAt, &lastSeen, &gauges, &pauseReason,
		&w.Kind, &w.EgressKey, &f.YtDlpVersion, &f.EgressGroup, &f.EgressIP,
		&w.FailureStreak, &w.StreakJobs}, st.dest()...)...); err != nil {
		return nil, err
	}
	w.Stats = st.stats()
	if err := json.Unmarshal([]byte(kinds), &f.Kinds); err != nil {
		return nil, fmt.Errorf("worker %s kinds: %w", w.ID, err)
	}
	if err := json.Unmarshal([]byte(models), &f.Models); err != nil {
		return nil, fmt.Errorf("worker %s models: %w", w.ID, err)
	}
	f.Kind = w.Kind
	w.RegisteredAt = fromMS(registered)
	w.DecidedBy = decidedBy.String
	w.PauseReason = pauseReason.String
	w.DecidedAt = optTime(decidedAt)
	w.LastSeenAt = optTime(lastSeen)
	if gauges.Valid {
		w.Gauges = json.RawMessage(gauges.String)
	}
	return &w, nil
}

// WorkerByID returns a worker in any state.
func (s *Store) WorkerByID(ctx context.Context, id string) (*Worker, error) {
	w, err := scanWorker(s.r.QueryRowContext(ctx, workerSelect+` WHERE w.id = ?`, id))
	if err != nil {
		return nil, notFound(err, "worker "+id)
	}
	return w, nil
}

// WorkerByID is Store.WorkerByID inside the transaction.
func (t *Tx) WorkerByID(ctx context.Context, id string) (*Worker, error) {
	w, err := scanWorker(t.tx.QueryRowContext(ctx, workerSelect+` WHERE w.id = ?`, id))
	if err != nil {
		return nil, notFound(err, "worker "+id)
	}
	return w, nil
}

// WorkerByTokenHash returns the worker owning a token. Rejected workers'
// tokens are revoked, so they are reported as ErrNotFound.
func (s *Store) WorkerByTokenHash(ctx context.Context, hash string) (*Worker, error) {
	w, err := scanWorker(s.r.QueryRowContext(ctx,
		workerSelect+` WHERE w.token_hash = ? AND w.state != 'rejected'`, hash))
	if err != nil {
		return nil, notFound(err, "worker by token")
	}
	return w, nil
}

// ListWorkers returns all workers, pending first, then by registration time.
func (s *Store) ListWorkers(ctx context.Context) ([]Worker, error) {
	rows, err := s.r.QueryContext(ctx,
		workerSelect+` ORDER BY w.state != 'pending', w.registered_at, w.id`)
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	defer rows.Close()
	var out []Worker
	for rows.Next() {
		w, err := scanWorker(rows)
		if err != nil {
			return nil, fmt.Errorf("list workers: %w", err)
		}
		out = append(out, *w)
	}
	return out, rows.Err()
}

// UpdateWorkerFacts replaces a worker's reported facts. The worker kind is
// fixed at registration: facts naming another kind are ErrConflict. It is a
// convenience for tests and tools: it drops the fetching jobs the change can
// fail, which the server reports through Tx.UpdateWorkerFacts.
func (s *Store) UpdateWorkerFacts(ctx context.Context, id string, facts api.WorkerFacts) error {
	return s.InTx(ctx, func(tx *Tx) error { _, err := tx.UpdateWorkerFacts(ctx, id, facts); return err })
}

// UpdateWorkerFacts is Store.UpdateWorkerFacts inside the transaction. A
// downloader that reports a sandbox level below FetchMinSandbox is no longer
// eligible, so fetching jobs it was the last eligible downloader for fail;
// they are returned.
func (t *Tx) UpdateWorkerFacts(ctx context.Context, id string, facts api.WorkerFacts) (OrphanFailures, error) {
	var none OrphanFailures
	kinds, models, err := factsJSON(facts)
	if err != nil {
		return none, err
	}
	var kind api.WorkerKind
	if err := t.tx.QueryRowContext(ctx, `SELECT kind FROM workers WHERE id = ?`, id).Scan(&kind); err != nil {
		return none, notFound(err, "worker "+id)
	}
	if want := cmp.Or(facts.Kind, api.WorkerProcessing); want != kind {
		return none, fmt.Errorf("worker %s is a %s, not a %s: %w", id, kind, want, ErrConflict)
	}
	res, err := t.tx.ExecContext(ctx, `UPDATE workers SET hostname=?, description=?, cpu_model=?, cores=?,
		mem_bytes=?, os=?, arch=?, engine_version=?, ffmpeg_version=?, sandbox_level=?, kinds=?, slots=?,
		models=?, version=?, ytdlp_version=?, egress_group=?, egress_ip=? WHERE id = ?`,
		facts.Hostname, facts.Description, facts.CPUModel, facts.Cores, facts.MemBytes, facts.OS, facts.Arch,
		facts.EngineVersion, facts.FFmpegVersion, string(facts.SandboxLevel), kinds, facts.Slots, models,
		facts.Version, facts.YtDlpVersion, facts.EgressGroup, facts.EgressIP, id)
	if err := mustAffect(res, err, "update worker facts"); err != nil {
		return none, err
	}
	if kind == api.WorkerDownloader && facts.SandboxLevel.Rank() < t.s.cfg.FetchMinSandbox.Rank() {
		return t.failAllOrphanedFetchJobs(ctx)
	}
	return none, nil
}

// SetWorkerEgressKey records the egress key the server derived for a worker.
func (s *Store) SetWorkerEgressKey(ctx context.Context, id, key string) error {
	return s.InTx(ctx, func(tx *Tx) error { return tx.SetWorkerEgressKey(ctx, id, key) })
}

// SetWorkerEgressKey is Store.SetWorkerEgressKey inside the transaction.
func (t *Tx) SetWorkerEgressKey(ctx context.Context, id, key string) error {
	if key == "" {
		return fmt.Errorf("%w: empty egress key", ErrInvalid)
	}
	res, err := t.tx.ExecContext(ctx, `UPDATE workers SET egress_key = ? WHERE id = ?`, key, id)
	return mustAffect(res, err, "set worker egress key")
}

// workerTransitions is the worker state machine (DESIGN.md §4.2).
var workerTransitions = map[api.WorkerState][]api.WorkerState{
	api.WorkerPending: {api.WorkerActive, api.WorkerRejected},
	api.WorkerActive:  {api.WorkerPaused, api.WorkerRejected},
	api.WorkerPaused:  {api.WorkerActive, api.WorkerRejected},
}

func canTransition(from, to api.WorkerState) bool {
	if from == to {
		return true
	}
	for _, s := range workerTransitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// SetWorkerState moves a worker to state, recording who decided. Illegal
// transitions (e.g. out of rejected) return ErrConflict; setting the current
// state again is a no-op. A change clears the circuit breaker's pause
// reason, and making the worker active (resume, approval) also clears its
// failure streak. Rejecting a worker cancels its live leases, whose tasks
// return to pending without consuming an attempt; their ids are returned.
//
// It is a convenience for tests and tools: it does not report the fetching
// jobs a rejection fails for want of a downloader. The server changes state
// through Tx.ChangeWorkerState, which does.
func (s *Store) SetWorkerState(ctx context.Context, id string, state api.WorkerState, decidedBy string) (cancelled []string, err error) {
	return inTx1(ctx, s, func(tx *Tx) ([]string, error) {
		c, err := tx.ChangeWorkerState(ctx, id, state, decidedBy)
		return c.Cancelled, err
	})
}

// WorkerStateChange is the effect of a worker state change.
type WorkerStateChange struct {
	// Cancelled are the worker's own leases that rejection ended.
	Cancelled []string
	// Orphaned are the fetching jobs failed because no eligible downloader
	// was left, and the leases (any worker's) ended by failing them.
	Orphaned OrphanFailures
}

// ChangeWorkerState is SetWorkerState reporting the whole effect.
func (t *Tx) ChangeWorkerState(ctx context.Context, id string, state api.WorkerState, decidedBy string) (out WorkerStateChange, err error) {
	var cur api.WorkerState
	var kind api.WorkerKind
	if err := t.tx.QueryRowContext(ctx, `SELECT state, kind FROM workers WHERE id = ?`, id).Scan(&cur, &kind); err != nil {
		return out, notFound(err, "worker "+id)
	}
	if !canTransition(cur, state) {
		return out, fmt.Errorf("worker %s: %s -> %s: %w", id, cur, state, ErrConflict)
	}
	if cur == state {
		return out, nil
	}
	now := t.s.now()
	if _, err := t.tx.ExecContext(ctx, `UPDATE workers SET state = ?, decided_by = ?, decided_at = ?,
		pause_reason = NULL WHERE id = ?`, string(state), decidedBy, toMS(now), id); err != nil {
		return out, fmt.Errorf("set worker state: %w", err)
	}
	if state == api.WorkerActive {
		if err := clearFailureStreak(ctx, t.tx, id); err != nil {
			return out, err
		}
	}
	if state != api.WorkerRejected {
		return out, nil
	}
	ls, err := liveLeases(ctx, t.tx, `l.worker_id = ?`, id)
	if err != nil {
		return out, err
	}
	for _, l := range ls {
		if _, err := t.s.endLease(ctx, t.tx, l, api.OutcomeCancelled, true, "worker rejected", "", now); err != nil {
			return out, err
		}
		out.Cancelled = append(out.Cancelled, l.ID)
	}
	if kind == api.WorkerDownloader {
		if out.Orphaned, err = t.failAllOrphanedFetchJobs(ctx); err != nil {
			return out, err
		}
	}
	return out, nil
}

// TouchWorker records that a worker was just seen. Non-nil gauges replace the
// stored heartbeat gauges.
func (s *Store) TouchWorker(ctx context.Context, id string, gauges *api.Gauges) error {
	var g sql.NullString
	if gauges != nil {
		b, err := json.Marshal(gauges)
		if err != nil {
			return fmt.Errorf("touch worker: %w", err)
		}
		g = sql.NullString{String: string(b), Valid: true}
	}
	return s.write(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE workers SET last_seen_at = ?, gauges = COALESCE(?, gauges) WHERE id = ?`,
			toMS(s.now()), g, id)
		return mustAffect(res, err, "touch worker")
	})
}

// DeletePendingWorkersUnseen deletes workers still pending that registered
// before cutoff, with every grant they hold or that is scoped to them, and
// returns how many. Expiry counts from
// registration, not last contact: a pending worker cannot hold its slot
// under the pending cap just by polling.
func (s *Store) DeletePendingWorkersUnseen(ctx context.Context, cutoff time.Time) (int64, error) {
	ids, err := inTx1(ctx, s, func(tx *Tx) ([]string, error) { return tx.DeletePendingWorkersUnseen(ctx, cutoff) })
	return int64(len(ids)), err
}

// DeletePendingWorkersUnseen is Store.DeletePendingWorkersUnseen inside the
// transaction. It returns the deleted workers' ids.
func (t *Tx) DeletePendingWorkersUnseen(ctx context.Context, cutoff time.Time) ([]string, error) {
	const where = `state = 'pending' AND registered_at < ?`
	ms := toMS(cutoff)
	if _, err := t.tx.ExecContext(ctx, `DELETE FROM grants
		WHERE principal IN (SELECT 'worker:' || id FROM workers WHERE `+where+`)
		   OR scope IN (SELECT 'worker:' || id FROM workers WHERE `+where+`)`, ms, ms); err != nil {
		return nil, fmt.Errorf("delete pending workers: %w", err)
	}
	rows, err := t.tx.QueryContext(ctx, `DELETE FROM workers WHERE `+where+` RETURNING id`, toMS(cutoff))
	if err != nil {
		return nil, fmt.Errorf("delete pending workers: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("delete pending workers: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// CountPendingWorkers returns the number of workers awaiting approval.
func (s *Store) CountPendingWorkers(ctx context.Context) (int, error) {
	var n int
	if err := s.r.QueryRowContext(ctx, `SELECT COUNT(*) FROM workers WHERE state = 'pending'`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count pending workers: %w", err)
	}
	return n, nil
}

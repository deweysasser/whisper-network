package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/fetchpolicy"
)

// FetchSeq is the task sequence number of a fetch task (the download that
// precedes prepare). It is negative so that seq 0 stays the prepare task
// everywhere else, and so that a fetch task sorts before it.
const FetchSeq = -1

// fetchGrantFor is a condition (over j = jobs) that the worker whose
// principal is the SQL expression principal ("?" for an argument) holds
// fetch on the job's queue.
func fetchGrantFor(principal string) string {
	return `EXISTS (SELECT 1 FROM grants g WHERE g.principal = ` + principal + ` AND g.role = 'fetch'
	AND (g.scope = '*' OR g.scope = 'queue:' || j.queue_id))`
}

// fetchGrantSQL is fetchGrantFor with the principal as its one argument.
var fetchGrantSQL = fetchGrantFor("?")

// fetchBranchSQL is the fetch branch of the claim predicate (over t = tasks,
// j = jobs) for the downloader whose principal is the SQL expression
// principal: a fetch task on a queue it holds fetch on. Claim adds the
// pacing and failed-here exclusions; rotation (pacer.canServe) uses the
// same branch for other groups' downloaders.
func fetchBranchSQL(principal string) string {
	return "t.kind = 'fetch' AND " + fetchGrantFor(principal)
}

// QueueHasDownloader reports whether the queue has an eligible downloader:
// a downloader worker that is active or paused, whose sandbox level ranks at
// least minSandbox, and that holds fetch on the queue (or on "*").
func (s *Store) QueueHasDownloader(ctx context.Context, queueID int64, minSandbox api.SandboxLevel) (bool, error) {
	return queueHasDownloader(ctx, s.r, queueID, minSandbox)
}

func queueHasDownloader(ctx context.Context, q querier, queueID int64, minSandbox api.SandboxLevel) (bool, error) {
	rows, err := q.QueryContext(ctx, `SELECT w.sandbox_level FROM workers w
		WHERE w.kind = 'downloader' AND w.state IN ('active','paused')
		AND EXISTS (SELECT 1 FROM grants g WHERE g.principal = 'worker:' || w.id AND g.role = 'fetch'
			AND (g.scope = '*' OR g.scope = ?))`, QueueScope(queueID))
	if err != nil {
		return false, fmt.Errorf("queue has downloader: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var lvl api.SandboxLevel
		if err := rows.Scan(&lvl); err != nil {
			return false, fmt.Errorf("queue has downloader: %w", err)
		}
		if lvl.Rank() >= minSandbox.Rank() {
			return true, nil
		}
	}
	return false, rows.Err()
}

// FailOrphanedFetchJobs fails the fetching jobs of a queue that no eligible
// downloader (see QueueHasDownloader) can serve, and returns their ids.
func (t *Tx) FailOrphanedFetchJobs(ctx context.Context, queueID int64, minSandbox api.SandboxLevel) ([]string, error) {
	o, err := t.failOrphanedFetchJobs(ctx, queueID, minSandbox)
	return o.Jobs, err
}

// OrphanFailures are the fetching jobs failed for want of an eligible
// downloader, and the leases ended by failing them (which can belong to any
// worker, e.g. a downloader below the sandbox floor).
type OrphanFailures struct{ Jobs, Leases []string }

// failOrphanedFetchJobs is FailOrphanedFetchJobs that also returns the ids
// of the leases it cancelled.
func (t *Tx) failOrphanedFetchJobs(ctx context.Context, queueID int64, minSandbox api.SandboxLevel) (out OrphanFailures, err error) {
	ok, err := queueHasDownloader(ctx, t.tx, queueID, minSandbox)
	if err != nil || ok {
		return out, err
	}
	var name string
	if err := t.tx.QueryRowContext(ctx, `SELECT name FROM queues WHERE id = ?`, queueID).Scan(&name); err != nil {
		return out, notFound(err, fmt.Sprintf("queue %d", queueID))
	}
	rows, err := t.tx.QueryContext(ctx, `SELECT id FROM jobs WHERE queue_id = ? AND state = 'fetching' ORDER BY id`, queueID)
	if err != nil {
		return out, fmt.Errorf("orphaned fetch jobs: %w", err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return out, fmt.Errorf("orphaned fetch jobs: %w", err)
		}
		out.Jobs = append(out.Jobs, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return out, err
	}
	rows.Close()
	now := t.s.now()
	for _, id := range out.Jobs {
		ended, err := failJob(ctx, t.tx, id, "no downloader holds fetch on queue "+name, now)
		if err != nil {
			return out, err
		}
		out.Leases = append(out.Leases, ended...)
	}
	return out, nil
}

// failAllOrphanedFetchJobs applies failOrphanedFetchJobs, at the store's
// FetchMinSandbox, to every queue with a fetching job.
func (t *Tx) failAllOrphanedFetchJobs(ctx context.Context) (out OrphanFailures, err error) {
	rows, err := t.tx.QueryContext(ctx, `SELECT DISTINCT queue_id FROM jobs WHERE state = 'fetching' ORDER BY queue_id`)
	if err != nil {
		return out, fmt.Errorf("orphaned fetch jobs: %w", err)
	}
	var qs []int64
	for rows.Next() {
		var q int64
		if err := rows.Scan(&q); err != nil {
			rows.Close()
			return out, fmt.Errorf("orphaned fetch jobs: %w", err)
		}
		qs = append(qs, q)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return out, err
	}
	rows.Close()
	for _, q := range qs {
		o, err := t.failOrphanedFetchJobs(ctx, q, t.s.cfg.FetchMinSandbox)
		if err != nil {
			return out, err
		}
		out.Jobs, out.Leases = append(out.Jobs, o.Jobs...), append(out.Leases, o.Leases...)
	}
	return out, nil
}

// fetchMaxBytes is a fetch lease's byte cap: the queue's upload cap if it
// has one, never above the server's (0 = no cap at that level).
func fetchMaxBytes(queue, server int64) int64 {
	switch {
	case queue > 0 && server > 0:
		return min(queue, server)
	case queue > 0:
		return queue
	}
	return server
}

// FetchCompletion is the original media a fetch task downloaded, already
// stored by the server, with the server's ingest decision: Canonical (and
// its Ranges) when the original is itself canonical audio, as for an upload
// on the fast path; nil to have it prepared.
type FetchCompletion struct {
	Original  Media
	Canonical *Canonical
	Ranges    []SampleRange
}

// AcceptFetch atomically completes a fetch lease: the fetch task is done,
// the job's original is recorded, the automatic backoff of the lease's
// (egress key, site) is reset (see resetFetchBackoff), and the job either
// becomes queued with one transcribe task per range (Canonical set) or
// preparing with a seq-0 prepare task. Repeating it for a lease that
// already completed with the same original hash succeeds again; another
// hash is ErrConflict; otherwise a non-live lease is ErrLeaseGone.
func (s *Store) AcceptFetch(ctx context.Context, leaseID, workerID string, c FetchCompletion) error {
	if c.Canonical != nil {
		if err := validRanges(c.Ranges, c.Canonical.Samples); err != nil {
			return err
		}
	} else if len(c.Ranges) > 0 {
		return fmt.Errorf("%w: task ranges without canonical audio", ErrInvalid)
	}
	now := s.now()
	return s.write(ctx, func(tx *sql.Tx) error {
		l, err := heldLease(ctx, tx, leaseID, workerID)
		if err != nil {
			return err
		}
		if l.TaskExists && l.Kind != api.KindFetch {
			return fmt.Errorf("%w: lease %s is a %s lease", ErrInvalid, leaseID, l.Kind)
		}
		if l.TaskExists && l.Ended && l.Outcome == api.OutcomeDone {
			var sha string
			if err := tx.QueryRowContext(ctx, `SELECT original_sha256 FROM jobs WHERE id = ?`, l.JobID).Scan(&sha); err != nil {
				return fmt.Errorf("accept fetch: %w", err)
			}
			if sha == c.Original.SHA256 {
				return nil
			}
			return fmt.Errorf("lease %s already completed with different media: %w", leaseID, ErrConflict)
		}
		if err := s.checkLive(l, now); err != nil {
			return err
		}
		ms := toMS(now)
		state := api.JobPreparing
		var canonBytes, canonSamples sql.NullInt64
		var canonSHA sql.NullString
		if cn := c.Canonical; cn != nil {
			state = api.JobQueued
			canonBytes = sql.NullInt64{Int64: cn.Bytes, Valid: true}
			canonSamples = sql.NullInt64{Int64: cn.Samples, Valid: true}
			canonSHA = sql.NullString{String: cn.SHA256, Valid: true}
		}
		var model, site string
		err = tx.QueryRowContext(ctx, `UPDATE jobs SET state = ?, original_bytes = ?, original_sha256 = ?,
			original_type = ?, canonical_bytes = ?, canonical_sha256 = ?, audio_samples = ?, updated_at = ?
			WHERE id = ? AND state = 'fetching' RETURNING model, COALESCE(fetch_site, '')`,
			string(state), c.Original.Bytes, c.Original.SHA256, c.Original.Type, canonBytes, canonSHA, canonSamples,
			ms, l.JobID).Scan(&model, &site)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("accept fetch: job %s not fetching: %w", l.JobID, ErrConflict)
		}
		if err != nil {
			return fmt.Errorf("accept fetch: %w", err)
		}
		res, err := tx.ExecContext(ctx, `UPDATE tasks SET state = 'done', worker_id = ?
			WHERE job_id = ? AND seq = ? AND state = 'leased'`, workerID, l.JobID, l.Seq)
		if err := mustAffect(res, err, "complete fetch task"); err != nil {
			return err
		}
		if err := countCompletion(ctx, tx, l, 0, now); err != nil {
			return err
		}
		if err := finishLease(ctx, tx, l, now); err != nil {
			return err
		}
		var egress string
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(egress_key, '') FROM leases WHERE id = ?`,
			l.ID).Scan(&egress); err != nil {
			return fmt.Errorf("accept fetch: %w", err)
		}
		if err := resetFetchBackoff(ctx, tx, egress, site, now); err != nil {
			return err
		}
		if err := reactivate(ctx, tx, l.QueueID, l.Weight); err != nil {
			return err
		}
		if c.Canonical != nil {
			return s.insertTranscribeTasks(ctx, tx, l.JobID, model, c.Ranges)
		}
		return s.insertPrepareTask(ctx, tx, l.JobID)
	})
}

// resetFetchBackoff resets the automatic backoff of (egress, site) after a
// successful fetch: an expired automatic cooldown is removed, and an active
// one keeps its until (a fetch that started before the cooldown was set does
// not lift it) but drops to level 0, so the next rate limit starts again at
// the policy's initial backoff. A manual cooldown is left alone.
func resetFetchBackoff(ctx context.Context, tx *sql.Tx, egress, site string, now time.Time) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM fetch_cooldowns WHERE egress_key = ? AND site = ? AND manual = 0
		AND until <= ?`, egress, site, toMS(now)); err != nil {
		return fmt.Errorf("reset fetch backoff: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE fetch_cooldowns SET level = 0 WHERE egress_key = ? AND site = ?
		AND manual = 0`, egress, site); err != nil {
		return fmt.Errorf("reset fetch backoff: %w", err)
	}
	return nil
}

// fetchStartsKeep is the least fetch-start history kept, whatever the
// longest rate window: rotation compares groups' most recent starts, which
// sites without rate rules would otherwise lose at once.
const fetchStartsKeep = 24 * time.Hour

// recordFetchStart records a fetch claim, in the claim's transaction: the
// start for (egress, site) and the lease's egress key. It also prunes
// starts older than the longest rule window of any policy in fp (at least
// fetchStartsKeep).
func (t *Tx) recordFetchStart(ctx context.Context, fp *fetchPolicies, l *Lease, egress string, now time.Time) error {
	ms := toMS(now)
	if _, err := t.tx.ExecContext(ctx, `INSERT INTO fetch_starts (egress_key, site, started_at) VALUES (?,?,?)`,
		egress, l.Site, ms); err != nil {
		return fmt.Errorf("record fetch start: %w", err)
	}
	if _, err := t.tx.ExecContext(ctx, `UPDATE leases SET egress_key = ? WHERE id = ?`, egress, l.ID); err != nil {
		return fmt.Errorf("record fetch start: %w", err)
	}
	l.EgressKey = egress
	keep := max(fetchpolicy.LongestWindow(fp.ps), fetchStartsKeep)
	if _, err := t.tx.ExecContext(ctx, `DELETE FROM fetch_starts WHERE started_at < ?`,
		toMS(now.Add(-keep))); err != nil {
		return fmt.Errorf("prune fetch starts: %w", err)
	}
	return nil
}

// fetchPolicies is one snapshot of the fetch policy table.
//
// A row that does not parse or validate (the admin API never writes one; a
// hand edit might) fails closed: it stays in ps as a placeholder with its
// site and egress key and zero concurrency, ahead of the valid rows so that
// it wins Resolve's ties, and so blocks whatever it would have governed --
// the site, or (site, egress) for a site@egress row. It never falls back to
// a less specific policy. An invalid '*' row blocks every site.
type fetchPolicies struct {
	ps []fetchpolicy.Policy
	// invalid are the keys of the invalid rows, in key order.
	invalid        []string
	defaultInvalid bool
}

func loadFetchPolicies(ctx context.Context, q querier) (*fetchPolicies, error) {
	rows, err := q.QueryContext(ctx, `SELECT key, site, COALESCE(egress_key, ''), rules, concurrency,
		backoff_initial_s, backoff_cap_s FROM fetch_policies ORDER BY key`)
	if err != nil {
		return nil, fmt.Errorf("fetch policies: %w", err)
	}
	defer rows.Close()
	out := &fetchPolicies{}
	var valid, bad []fetchpolicy.Policy
	for rows.Next() {
		var p fetchpolicy.Policy
		var rules string
		var initial, cap int64
		if err := rows.Scan(&p.Key, &p.Site, &p.EgressKey, &rules, &p.Concurrency, &initial, &cap); err != nil {
			return nil, fmt.Errorf("fetch policies: %w", err)
		}
		p.BackoffInitial, p.BackoffCap = time.Duration(initial)*time.Second, time.Duration(cap)*time.Second
		if parseFetchRules(&p, rules) == nil && p.Validate() == nil {
			valid = append(valid, p)
			continue
		}
		out.invalid = append(out.invalid, p.Key)
		if p.Key == "*" || p.Site == "*" && p.EgressKey == "" {
			out.defaultInvalid = true
		}
		bad = append(bad, fetchpolicy.Policy{Key: p.Key, Site: p.Site, EgressKey: p.EgressKey})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fetch policies: %w", err)
	}
	out.ps = append(bad, valid...)
	return out, nil
}

// resolve is the policy for (site, egress); zero Concurrency means blocked
// (no policy, or an invalid one).
func (fp *fetchPolicies) resolve(site, egress string) fetchpolicy.Policy {
	if fp.defaultInvalid {
		return fetchpolicy.Policy{}
	}
	return fetchpolicy.Resolve(fp.ps, site, egress)
}

// InvalidFetchPolicies returns the keys of fetch policy rows that do not
// parse or validate, in key order. Each blocks what it governs (see
// fetchPolicies); the server should log or meter them.
func (s *Store) InvalidFetchPolicies(ctx context.Context) ([]string, error) {
	fp, err := loadFetchPolicies(ctx, s.r)
	if err != nil {
		return nil, err
	}
	return fp.invalid, nil
}

func parseFetchRules(p *fetchpolicy.Policy, js string) error {
	var raw []string
	if err := json.Unmarshal([]byte(js), &raw); err != nil {
		return err
	}
	for _, r := range raw {
		rule, err := fetchpolicy.ParseRule(r)
		if err != nil {
			return err
		}
		p.Rules = append(p.Rules, rule)
	}
	return nil
}

// pacer evaluates fetch pacing for (egress key, site) pairs against one
// snapshot of the policies.
type pacer struct {
	q          querier
	fp         *fetchPolicies
	minSandbox api.SandboxLevel
	now        time.Time
}

// decide is fetchpolicy.Decide for (egress, site) with its stored state. It
// fails closed: when no valid policy applies the site is blocked with no
// time at which it clears.
func (p *pacer) decide(ctx context.Context, egress, site string) (ok bool, next time.Time, err error) {
	pol := p.fp.resolve(site, egress)
	if pol.Concurrency < 1 {
		return false, time.Time{}, nil
	}
	var st fetchpolicy.State
	if w := fetchpolicy.LongestWindow([]fetchpolicy.Policy{pol}); w > 0 {
		rows, err := p.q.QueryContext(ctx, `SELECT started_at FROM fetch_starts WHERE egress_key = ? AND site = ?
			AND started_at > ?`, egress, site, toMS(p.now.Add(-w)))
		if err != nil {
			return false, time.Time{}, fmt.Errorf("fetch starts: %w", err)
		}
		for rows.Next() {
			var at int64
			if err := rows.Scan(&at); err != nil {
				rows.Close()
				return false, time.Time{}, fmt.Errorf("fetch starts: %w", err)
			}
			st.Starts = append(st.Starts, fromMS(at))
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return false, time.Time{}, fmt.Errorf("fetch starts: %w", err)
		}
	}
	// In flight: unended leases from this egress on jobs of this site.
	if err := p.q.QueryRowContext(ctx, `SELECT COUNT(*) FROM leases l JOIN jobs j ON j.id = l.job_id
		WHERE l.egress_key = ? AND l.ended_at IS NULL AND COALESCE(j.fetch_site, '') = ?`,
		egress, site).Scan(&st.InFlight); err != nil {
		return false, time.Time{}, fmt.Errorf("fetch in flight: %w", err)
	}
	var until int64
	err = p.q.QueryRowContext(ctx, `SELECT until FROM fetch_cooldowns WHERE egress_key = ? AND site = ?`,
		egress, site).Scan(&until)
	switch {
	case err == nil:
		st.CooldownUntil = fromMS(until)
	case !errors.Is(err, sql.ErrNoRows):
		return false, time.Time{}, fmt.Errorf("fetch cooldown: %w", err)
	}
	ok, next = fetchpolicy.Decide(pol, st, p.now)
	return ok, next, nil
}

// lastStart is the most recent recorded fetch start of (egress, site), or
// math.MinInt64 if there is none (never fetched sorts as least recent).
func (p *pacer) lastStart(ctx context.Context, egress, site string) (int64, error) {
	var at sql.NullInt64
	if err := p.q.QueryRowContext(ctx, `SELECT MAX(started_at) FROM fetch_starts WHERE egress_key = ? AND site = ?`,
		egress, site).Scan(&at); err != nil {
		return 0, fmt.Errorf("last fetch start: %w", err)
	}
	if !at.Valid {
		return math.MinInt64, nil
	}
	return at.Int64, nil
}

// canServe reports whether some downloader of egress could claim a pending
// fetch task of site now, by the claim's own per-worker rules: an active
// downloader at or above the fetch sandbox floor, the claim's fetch branch
// (fetchBranchSQL: kind and grant) and the failed-here exclusion. A waiting
// group that cannot claim the site's tasks must not hold others back from
// them, or the site could stall.
func (p *pacer) canServe(ctx context.Context, egress, site string) (bool, error) {
	levels, err := queryStrings(ctx, p.q, `SELECT DISTINCT w.sandbox_level FROM workers w
		WHERE w.kind = 'downloader' AND w.state = 'active' AND w.egress_key = ?
		AND EXISTS (SELECT 1 FROM tasks t JOIN jobs j ON j.id = t.job_id
			WHERE t.state = 'pending' AND COALESCE(j.fetch_site, '') = ?
			AND `+fetchBranchSQL("'worker:' || w.id")+` AND NOT `+failedHereFor("w.id")+`)`, egress, site)
	if err != nil {
		return false, fmt.Errorf("fetch rotation: %w", err)
	}
	for _, l := range levels {
		if api.SandboxLevel(l).Rank() >= p.minSandbox.Rank() {
			return true, nil
		}
	}
	return false, nil
}

// pendingFetchSites lists the distinct sites of pending fetch tasks.
func pendingFetchSites(ctx context.Context, q querier) ([]string, error) {
	return queryStrings(ctx, q, `SELECT DISTINCT COALESCE(j.fetch_site, '') FROM tasks t JOIN jobs j ON j.id = t.job_id
		WHERE t.kind = 'fetch' AND t.state = 'pending' ORDER BY 1`)
}

func queryStrings(ctx context.Context, q querier, query string, args ...any) ([]string, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("query: %w", err)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// blockedFetchSites returns the sites of pending fetch tasks that a claim
// from egressKey may not take now. A site is blocked when
//
//   - pacing says no: fetchpolicy.Decide for (egressKey, site), with the
//     policy fetchpolicy.Resolve picks from fp (no valid policy blocks:
//     fail closed, see fetchPolicies), the starts recorded in
//     fetch_starts, the in-flight count (unended leases from egressKey on
//     jobs of the site) and the cooldown; or
//   - rotation prefers another group: some other key K in waiting (egress
//     keys with a fetch claim waiting now) is itself allowed by pacing,
//     has a downloader that could claim one of the site's pending tasks
//     now (pacer.canServe: the claim's own worker rules), and
//     last started a fetch of the site strictly earlier than egressKey did
//     (never counts as earliest). Equal last starts do not block, so two
//     groups that never fetched the site do not hold each other back.
//
// It runs inside the claim's transaction, so the decision and the start the
// claim then records are atomic with respect to other claims.
func (t *Tx) blockedFetchSites(ctx context.Context, fp *fetchPolicies, egressKey string, waiting []string,
	now time.Time) ([]string, error) {
	sites, err := pendingFetchSites(ctx, t.tx)
	if err != nil || len(sites) == 0 {
		return nil, err
	}
	p := &pacer{q: t.tx, fp: fp, minSandbox: t.s.cfg.FetchMinSandbox, now: now}
	var others []string
	for _, k := range waiting {
		if k != egressKey && !slices.Contains(others, k) {
			others = append(others, k)
		}
	}
	var blocked []string
	for _, site := range sites {
		ok, _, err := p.decide(ctx, egressKey, site)
		if err != nil {
			return nil, err
		}
		if ok && len(others) > 0 {
			ok, err = p.rotationAllows(ctx, egressKey, site, others)
			if err != nil {
				return nil, err
			}
		}
		if !ok {
			blocked = append(blocked, site)
		}
	}
	return blocked, nil
}

// rotationAllows reports whether no group in others should take site
// before egress (see blockedFetchSites).
func (p *pacer) rotationAllows(ctx context.Context, egress, site string, others []string) (bool, error) {
	mine, err := p.lastStart(ctx, egress, site)
	if err != nil {
		return false, err
	}
	for _, k := range others {
		theirs, err := p.lastStart(ctx, k, site)
		if err != nil {
			return false, err
		}
		if theirs >= mine {
			continue
		}
		ok, _, err := p.decide(ctx, k, site)
		if err != nil {
			return false, err
		}
		if !ok {
			continue
		}
		if ok, err = p.canServe(ctx, k, site); err != nil {
			return false, err
		}
		if ok {
			return false, nil
		}
	}
	return true, nil
}

// NextFetchEligible returns the earliest time after now at which pacing
// stops holding back some (egress key of an active downloader, site of a
// pending fetch task) pair, so the server can wake waiting claims then. It
// returns ok = false when no pair is waiting on a time: none is held back,
// or only by concurrency (which clears when a lease ends, a wake-up of its
// own) or by the absence of any policy.
func (s *Store) NextFetchEligible(ctx context.Context, now time.Time) (time.Time, bool, error) {
	sites, err := pendingFetchSites(ctx, s.r)
	if err != nil || len(sites) == 0 {
		return time.Time{}, false, err
	}
	keys, err := queryStrings(ctx, s.r, `SELECT DISTINCT egress_key FROM workers
		WHERE kind = 'downloader' AND state = 'active' ORDER BY 1`)
	if err != nil || len(keys) == 0 {
		return time.Time{}, false, err
	}
	fp, err := loadFetchPolicies(ctx, s.r)
	if err != nil {
		return time.Time{}, false, err
	}
	p := &pacer{q: s.r, fp: fp, minSandbox: s.cfg.FetchMinSandbox, now: now}
	var best time.Time
	for _, k := range keys {
		for _, site := range sites {
			_, next, err := p.decide(ctx, k, site)
			if err != nil {
				return time.Time{}, false, err
			}
			if next.After(now) && (best.IsZero() || next.Before(best)) {
				best = next
			}
		}
	}
	return best, !best.IsZero(), nil
}

// FetchBlockedTasks counts, per site, the pending fetch tasks that pacing
// holds back at now from every egress key of an active downloader (see
// pacer.decide: cooldown, concurrency, rate rules, or no valid policy).
// Sites some active downloader's group may fetch now are left out, and so
// is everything when no downloader is active (the tasks then wait for a
// downloader, not for pacing).
func (s *Store) FetchBlockedTasks(ctx context.Context, now time.Time) (map[string]int, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT COALESCE(j.fetch_site, ''), COUNT(*) FROM tasks t
		JOIN jobs j ON j.id = t.job_id WHERE t.kind = 'fetch' AND t.state = 'pending' GROUP BY 1 ORDER BY 1`)
	if err != nil {
		return nil, fmt.Errorf("fetch blocked tasks: %w", err)
	}
	pending := map[string]int{}
	for rows.Next() {
		var site string
		var n int
		if err := rows.Scan(&site, &n); err != nil {
			rows.Close()
			return nil, fmt.Errorf("fetch blocked tasks: %w", err)
		}
		pending[site] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(pending) == 0 {
		return nil, err
	}
	keys, err := queryStrings(ctx, s.r, `SELECT DISTINCT egress_key FROM workers
		WHERE kind = 'downloader' AND state = 'active' ORDER BY 1`)
	if err != nil || len(keys) == 0 {
		return nil, err
	}
	fp, err := loadFetchPolicies(ctx, s.r)
	if err != nil {
		return nil, err
	}
	p := &pacer{q: s.r, fp: fp, minSandbox: s.cfg.FetchMinSandbox, now: now}
	out := map[string]int{}
	for site, n := range pending {
		blocked := true
		for _, k := range keys {
			ok, _, err := p.decide(ctx, k, site)
			if err != nil {
				return nil, err
			}
			if ok {
				blocked = false
				break
			}
		}
		if blocked {
			out[site] = n
		}
	}
	return out, nil
}

// fallbackBackoff is the cooldown backoff when neither the governing policy
// nor the '*' policy is usable (see rateLimitBackoff).
var fallbackBackoff = fetchpolicy.Policy{BackoffInitial: 12 * time.Hour, BackoffCap: 24 * time.Hour}

// rateLimitBackoff is the policy whose backoff applies to (site, egress): the
// one the claim resolves. When that is blocked (an invalid row, or no
// policy) the '*' policy's values are used if valid, else fallbackBackoff,
// so a rate limit always cools the pair down.
func (fp *fetchPolicies) rateLimitBackoff(site, egress string) fetchpolicy.Policy {
	if p := fp.resolve(site, egress); p.Concurrency >= 1 {
		return p
	}
	for _, p := range fp.ps {
		if p.Key == "*" && p.Concurrency >= 1 {
			return p
		}
	}
	return fallbackBackoff
}

// rateLimitBurst is how recently an automatic cooldown must have been set
// for a further rate_limited report to count as the same burst.
const rateLimitBurst = time.Minute

// startRateLimitCooldown, in the transaction that ended l with stage
// rate_limited, cools down l's (egress key, site): until = now +
// Backoff(policy, level) with the row's level before it is incremented.
//
// An existing manual row whose until is later than that is left unchanged
// (a rate limit never shortens a manual cooldown). Otherwise the row becomes
// automatic with the new until and level+1, replacing a shorter manual one.
// A report that falls in the burst of the cooldown just set (see
// rateLimitBurst) changes nothing.
func (s *Store) startRateLimitCooldown(ctx context.Context, tx *sql.Tx, l leaseRow, now time.Time) error {
	var egress, site string
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(l.egress_key, ''), COALESCE(j.fetch_site, '')
		FROM leases l LEFT JOIN jobs j ON j.id = l.job_id WHERE l.id = ?`, l.ID).Scan(&egress, &site); err != nil {
		return fmt.Errorf("rate limit cooldown: %w", err)
	}
	fp, err := loadFetchPolicies(ctx, tx)
	if err != nil {
		return err
	}
	var until int64
	var level int
	var manual bool
	err = tx.QueryRowContext(ctx, `SELECT until, level, manual FROM fetch_cooldowns WHERE egress_key = ? AND site = ?`,
		egress, site).Scan(&until, &level, &manual)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("rate limit cooldown: %w", err)
	}
	pol := fp.rateLimitBackoff(site, egress)
	next := toMS(now.Add(fetchpolicy.Backoff(pol, level)))
	if manual && until > next {
		return nil
	}
	// Leases of one burst hit the same refusal together: an automatic row
	// still in force that was set within rateLimitBurst of now (until is
	// still within rateLimitBurst of now + the backoff that set it) is
	// that burst's own, and is not escalated again.
	if !manual && level > 0 && until > toMS(now) {
		b := fetchpolicy.Backoff(pol, level-1)
		if until >= toMS(now.Add(b-min(rateLimitBurst, b/2))) {
			return nil
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO fetch_cooldowns (egress_key, site, until, level, manual, set_by, reason)
		VALUES (?, ?, ?, ?, 0, ?, 'rate limited')
		ON CONFLICT (egress_key, site) DO UPDATE SET until = excluded.until, level = excluded.level, manual = 0,
		set_by = excluded.set_by, reason = excluded.reason`,
		egress, site, next, level+1, SystemActor); err != nil {
		return fmt.Errorf("rate limit cooldown: %w", err)
	}
	return nil
}

// FetchPolicies returns the valid fetch policies in key order. Rows that do
// not parse or validate are omitted (see InvalidFetchPolicies).
func (s *Store) FetchPolicies(ctx context.Context) ([]fetchpolicy.Policy, error) {
	fp, err := loadFetchPolicies(ctx, s.r)
	if err != nil {
		return nil, err
	}
	var out []fetchpolicy.Policy
	for _, p := range fp.ps {
		if p.Concurrency >= 1 {
			out = append(out, p)
		}
	}
	slices.SortFunc(out, func(a, b fetchpolicy.Policy) int { return strings.Compare(a.Key, b.Key) })
	return out, nil
}

// PutFetchPolicy inserts or replaces a fetch policy, recording by as the
// editor. It is ErrInvalid unless the policy validates (which includes Key
// being Site or Site@EgressKey), every rule parses and the backoffs are
// whole seconds. created reports an insert.
func (t *Tx) PutFetchPolicy(ctx context.Context, p fetchpolicy.Policy, by string) (created bool, err error) {
	if err := p.Validate(); err != nil {
		return false, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if p.BackoffInitial%time.Second != 0 || p.BackoffCap%time.Second != 0 {
		return false, fmt.Errorf("%w: backoff must be whole seconds", ErrInvalid)
	}
	rules := make([]string, 0, len(p.Rules))
	for _, r := range p.Rules {
		if _, err := fetchpolicy.ParseRule(r.String()); err != nil {
			return false, fmt.Errorf("%w: rule %s: %v", ErrInvalid, r, err)
		}
		rules = append(rules, r.String())
	}
	js, err := json.Marshal(rules)
	if err != nil {
		return false, fmt.Errorf("put fetch policy: %w", err)
	}
	var n int
	if err := t.tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fetch_policies WHERE key = ?`, p.Key).Scan(&n); err != nil {
		return false, fmt.Errorf("put fetch policy: %w", err)
	}
	var egress sql.NullString
	if p.EgressKey != "" {
		egress = sql.NullString{String: p.EgressKey, Valid: true}
	}
	if _, err := t.tx.ExecContext(ctx, `INSERT INTO fetch_policies (key, site, egress_key, rules, concurrency,
		backoff_initial_s, backoff_cap_s, updated_by, updated_at) VALUES (?,?,?,?,?,?,?,?,?)
		ON CONFLICT (key) DO UPDATE SET site = excluded.site, egress_key = excluded.egress_key,
		rules = excluded.rules, concurrency = excluded.concurrency, backoff_initial_s = excluded.backoff_initial_s,
		backoff_cap_s = excluded.backoff_cap_s, updated_by = excluded.updated_by, updated_at = excluded.updated_at`,
		p.Key, p.Site, egress, string(js), p.Concurrency, int64(p.BackoffInitial/time.Second),
		int64(p.BackoffCap/time.Second), by, toMS(t.s.now())); err != nil {
		return false, fmt.Errorf("put fetch policy: %w", err)
	}
	return n == 0, nil
}

// DeleteFetchPolicy removes a fetch policy. The '*' default cannot be
// deleted (ErrConflict); an unknown key is ErrNotFound.
func (t *Tx) DeleteFetchPolicy(ctx context.Context, key string) error {
	if key == "*" {
		return fmt.Errorf("%w: the default fetch policy cannot be deleted", ErrConflict)
	}
	res, err := t.tx.ExecContext(ctx, `DELETE FROM fetch_policies WHERE key = ?`, key)
	if err != nil {
		return fmt.Errorf("delete fetch policy: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("delete fetch policy: %w", err)
	} else if n == 0 {
		return fmt.Errorf("fetch policy %q: %w", key, ErrNotFound)
	}
	return nil
}

// FetchCooldowns returns the cooldowns in force at now (until after now),
// earliest first.
func (s *Store) FetchCooldowns(ctx context.Context, now time.Time) ([]api.FetchCooldown, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT egress_key, site, until, level, manual, set_by, reason
		FROM fetch_cooldowns WHERE until > ? ORDER BY until, egress_key, site`, toMS(now))
	if err != nil {
		return nil, fmt.Errorf("fetch cooldowns: %w", err)
	}
	defer rows.Close()
	var out []api.FetchCooldown
	for rows.Next() {
		var c api.FetchCooldown
		var until int64
		if err := rows.Scan(&c.EgressKey, &c.Site, &until, &c.Level, &c.Manual, &c.SetBy, &c.Reason); err != nil {
			return nil, fmt.Errorf("fetch cooldowns: %w", err)
		}
		c.Until = fromMS(until)
		out = append(out, c)
	}
	return out, rows.Err()
}

// SetCooldown manually cools (egressKey, site) down until until, replacing
// any existing row (ErrInvalid for an empty key or site or an until that is
// not in the future). An existing row's level is kept (so the automatic
// backoff resumes where it was); a new row starts at 0.
func (t *Tx) SetCooldown(ctx context.Context, egressKey, site string, until time.Time, by, reason string) error {
	if egressKey == "" || site == "" {
		return fmt.Errorf("%w: cooldown needs an egress key and a site", ErrInvalid)
	}
	if !until.After(t.s.now()) {
		return fmt.Errorf("%w: cooldown must end in the future", ErrInvalid)
	}
	if _, err := t.tx.ExecContext(ctx, `INSERT INTO fetch_cooldowns (egress_key, site, until, level, manual, set_by, reason)
		VALUES (?, ?, ?, 0, 1, ?, ?)
		ON CONFLICT (egress_key, site) DO UPDATE SET until = excluded.until, manual = 1,
		set_by = excluded.set_by, reason = excluded.reason`,
		egressKey, site, toMS(until), by, reason); err != nil {
		return fmt.Errorf("set cooldown: %w", err)
	}
	return nil
}

// ClearCooldown deletes the cooldown row of (egressKey, site), if any, which
// also resets its level.
func (t *Tx) ClearCooldown(ctx context.Context, egressKey, site string) error {
	if _, err := t.tx.ExecContext(ctx, `DELETE FROM fetch_cooldowns WHERE egress_key = ? AND site = ?`,
		egressKey, site); err != nil {
		return fmt.Errorf("clear cooldown: %w", err)
	}
	return nil
}

// FetchPolicyMeta is who last edited a fetch policy, and when.
type FetchPolicyMeta struct {
	UpdatedBy string
	UpdatedAt time.Time
}

// FetchPolicyMetas returns the edit metadata of every fetch policy row, by
// key (including rows FetchPolicies omits as invalid).
func (s *Store) FetchPolicyMetas(ctx context.Context) (map[string]FetchPolicyMeta, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT key, updated_by, updated_at FROM fetch_policies`)
	if err != nil {
		return nil, fmt.Errorf("fetch policy metas: %w", err)
	}
	defer rows.Close()
	out := map[string]FetchPolicyMeta{}
	for rows.Next() {
		var k string
		var m FetchPolicyMeta
		var at int64
		if err := rows.Scan(&k, &m.UpdatedBy, &at); err != nil {
			return nil, fmt.Errorf("fetch policy metas: %w", err)
		}
		m.UpdatedAt = fromMS(at)
		out[k] = m
	}
	return out, rows.Err()
}

// FailAllOrphanedFetchJobs fails the fetching jobs of every queue without an
// eligible downloader (at the store's FetchMinSandbox) and returns them with
// the leases that ended. It is the reaper's backstop for floors raised or
// downloaders removed behind the store's back.
func (s *Store) FailAllOrphanedFetchJobs(ctx context.Context) (OrphanFailures, error) {
	return inTx1(ctx, s, func(tx *Tx) (OrphanFailures, error) { return tx.failAllOrphanedFetchJobs(ctx) })
}

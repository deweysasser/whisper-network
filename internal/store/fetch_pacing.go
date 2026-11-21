package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/fetchpolicy"
)

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
	rows, err := queryFetchPolicyRows(ctx, q, "")
	if err != nil {
		return nil, err
	}
	out := &fetchPolicies{}
	var valid, bad []fetchpolicy.Policy
	for _, r := range rows {
		if p, ok := r.policy(); ok {
			valid = append(valid, p)
			continue
		}
		out.invalid = append(out.invalid, r.Key)
		if r.Key == "*" || r.Site == "*" && r.EgressKey == "" {
			out.defaultInvalid = true
		}
		bad = append(bad, fetchpolicy.Policy{Key: r.Key, Site: r.Site, EgressKey: r.EgressKey})
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

// canServe reports whether one of the waiting downloaders ws could claim a
// pending fetch task of site now, by the claim's own per-worker rules: an
// active downloader whose claim reports a sandbox level at or above the
// fetch floor, the claim's fetch branch (fetchBranchSQL: kind and grant)
// and the failed-here exclusion. Only downloaders actually waiting count: a
// group whose waiting downloaders cannot claim the site's tasks must not
// hold others back from them (its other downloaders, busy or offline, are
// not asking), or the site could stall.
func (p *pacer) canServe(ctx context.Context, ws []FetchWaiter, site string) (bool, error) {
	for _, w := range ws {
		if w.SandboxLevel.Rank() < p.minSandbox.Rank() {
			continue
		}
		var ok bool
		if err := p.q.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM workers w
			WHERE w.id = ? AND w.kind = 'downloader' AND w.state = 'active'
			AND EXISTS (SELECT 1 FROM tasks t JOIN jobs j ON j.id = t.job_id
				WHERE t.state = 'pending' AND COALESCE(j.fetch_site, '') = ?
				AND `+fetchBranchSQL("'worker:' || w.id")+` AND NOT `+failedHereFor("w.id")+`))`,
			w.WorkerID, site).Scan(&ok); err != nil {
			return false, fmt.Errorf("fetch rotation: %w", err)
		}
		if ok {
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
//   - rotation prefers another group: some other key K of waiting (the
//     downloaders with a fetch claim waiting now) is itself allowed by
//     pacing, has a waiting downloader that could claim one of the site's
//     pending tasks now (pacer.canServe: the claim's own worker rules), and
//     last started a fetch of the site strictly earlier than egressKey did
//     (never counts as earliest). Equal last starts do not block, so two
//     groups that never fetched the site do not hold each other back.
//
// It runs inside the claim's transaction, so the decision and the start the
// claim then records are atomic with respect to other claims.
func (t *Tx) blockedFetchSites(ctx context.Context, fp *fetchPolicies, egressKey string, waiting []FetchWaiter,
	now time.Time) ([]string, error) {
	sites, err := pendingFetchSites(ctx, t.tx)
	if err != nil || len(sites) == 0 {
		return nil, err
	}
	p := &pacer{q: t.tx, fp: fp, minSandbox: t.s.cfg.FetchMinSandbox, now: now}
	others := waitingGroups(waiting, egressKey)
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

// waitingGroup is one other egress group's waiting downloaders.
type waitingGroup struct {
	key string
	ws  []FetchWaiter
}

// waitingGroups groups the waiting downloaders of keys other than mine, in
// first-seen order.
func waitingGroups(waiting []FetchWaiter, mine string) []waitingGroup {
	var out []waitingGroup
	for _, w := range waiting {
		if w.EgressKey == mine {
			continue
		}
		i := slices.IndexFunc(out, func(g waitingGroup) bool { return g.key == w.EgressKey })
		if i < 0 {
			out = append(out, waitingGroup{key: w.EgressKey})
			i = len(out) - 1
		}
		out[i].ws = append(out[i].ws, w)
	}
	return out
}

// rotationAllows reports whether no group in others should take site
// before egress (see blockedFetchSites).
func (p *pacer) rotationAllows(ctx context.Context, egress, site string, others []waitingGroup) (bool, error) {
	mine, err := p.lastStart(ctx, egress, site)
	if err != nil {
		return false, err
	}
	for _, g := range others {
		k := g.key
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
		if ok, err = p.canServe(ctx, g.ws, site); err != nil {
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

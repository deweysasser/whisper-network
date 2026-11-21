package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/fetchpolicy"
)

// InvalidFetchPolicies returns the keys of fetch policy rows that do not
// parse or validate, in key order. Each blocks what it governs (see
// fetchPolicies); the server should log or meter them.
func (s *Store) InvalidFetchPolicies(ctx context.Context) ([]string, error) {
	rows, err := s.FetchPolicyRows(ctx)
	if err != nil {
		return nil, err
	}
	return InvalidFetchPolicyKeys(rows), nil
}

// InvalidFetchPolicyKeys are the keys of the invalid rows of rows, in order.
func InvalidFetchPolicyKeys(rows []FetchPolicyRow) []string {
	var out []string
	for _, r := range rows {
		if r.Invalid != "" {
			out = append(out, r.Key)
		}
	}
	return out
}

// FetchPolicies returns the valid fetch policies in key order. Rows that do
// not parse or validate are omitted (see InvalidFetchPolicies).
func (s *Store) FetchPolicies(ctx context.Context) ([]fetchpolicy.Policy, error) {
	rows, err := s.FetchPolicyRows(ctx)
	if err != nil {
		return nil, err
	}
	var out []fetchpolicy.Policy
	for _, r := range rows {
		if p, ok := r.policy(); ok {
			out = append(out, p)
		}
	}
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

// FetchCooldown returns the cooldown row of (egressKey, site), in force or
// not (ErrNotFound if there is none).
func (s *Store) FetchCooldown(ctx context.Context, egressKey, site string) (api.FetchCooldown, error) {
	c := api.FetchCooldown{EgressKey: egressKey, Site: site}
	var until int64
	err := s.r.QueryRowContext(ctx, `SELECT until, level, manual, set_by, reason FROM fetch_cooldowns
		WHERE egress_key = ? AND site = ?`, egressKey, site).Scan(&until, &c.Level, &c.Manual, &c.SetBy, &c.Reason)
	if err != nil {
		return c, notFound(err, "cooldown "+egressKey+"/"+site)
	}
	c.Until = fromMS(until)
	return c, nil
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

// ClearCooldown deletes the cooldown row of (egressKey, site), which also
// resets its level. ErrNotFound if there is none.
func (t *Tx) ClearCooldown(ctx context.Context, egressKey, site string) error {
	res, err := t.tx.ExecContext(ctx, `DELETE FROM fetch_cooldowns WHERE egress_key = ? AND site = ?`,
		egressKey, site)
	if err != nil {
		return fmt.Errorf("clear cooldown: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("clear cooldown: %w", err)
	} else if n == 0 {
		return fmt.Errorf("cooldown %s/%s: %w", egressKey, site, ErrNotFound)
	}
	return nil
}

// FetchPolicyRow is a stored fetch policy as it is, valid or not.
type FetchPolicyRow struct {
	Key, Site, EgressKey string
	// Rules are the stored rule strings (nil if the stored list does not
	// parse as JSON).
	Rules                        []string
	Concurrency                  int
	BackoffInitialS, BackoffCapS int64
	UpdatedBy                    string
	UpdatedAt                    time.Time
	// Invalid is why the row does not validate (empty for a valid row).
	Invalid string
	// parsed is the row's policy, built once when the row is read; zero
	// for an invalid row.
	parsed fetchpolicy.Policy
}

// FetchPolicyRows returns every fetch policy row in key order, including the
// invalid ones (which fail their site closed) with the reason.
func (s *Store) FetchPolicyRows(ctx context.Context) ([]FetchPolicyRow, error) {
	return queryFetchPolicyRows(ctx, s.r, "")
}

// FetchPolicyRow returns the stored fetch policy row of key, valid or not
// (ErrNotFound if there is none).
func (s *Store) FetchPolicyRow(ctx context.Context, key string) (FetchPolicyRow, error) {
	rows, err := queryFetchPolicyRows(ctx, s.r, key)
	if err != nil {
		return FetchPolicyRow{}, err
	}
	if len(rows) == 0 {
		return FetchPolicyRow{}, fmt.Errorf("fetch policy %q: %w", key, ErrNotFound)
	}
	return rows[0], nil
}

// queryFetchPolicyRows reads the fetch policy rows in key order: all of
// them, or only key's when key is not empty. Each row's validity is
// worked out once, here (FetchPolicyRow.Invalid, FetchPolicyRow.policy).
func queryFetchPolicyRows(ctx context.Context, q querier, key string) ([]FetchPolicyRow, error) {
	query := `SELECT key, site, COALESCE(egress_key, ''), rules, concurrency, backoff_initial_s, backoff_cap_s,
		updated_by, updated_at FROM fetch_policies`
	var args []any
	if key != "" {
		query += ` WHERE key = ?`
		args = append(args, key)
	}
	rows, err := q.QueryContext(ctx, query+` ORDER BY key`, args...)
	if err != nil {
		return nil, fmt.Errorf("fetch policies: %w", err)
	}
	defer rows.Close()
	var out []FetchPolicyRow
	for rows.Next() {
		var r FetchPolicyRow
		var rules string
		var at int64
		if err := rows.Scan(&r.Key, &r.Site, &r.EgressKey, &rules, &r.Concurrency, &r.BackoffInitialS,
			&r.BackoffCapS, &r.UpdatedBy, &at); err != nil {
			return nil, fmt.Errorf("fetch policies: %w", err)
		}
		r.UpdatedAt = fromMS(at)
		if err := json.Unmarshal([]byte(rules), &r.Rules); err != nil {
			r.Rules, r.Invalid = nil, "rules: "+err.Error()
		} else if p, why := r.validate(); why != "" {
			r.Invalid = why
		} else {
			r.parsed = p
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// validate builds the row's policy and reports why it is invalid ("" for a
// valid one): every rule must parse and the policy validate.
func (r FetchPolicyRow) validate() (fetchpolicy.Policy, string) {
	p := fetchpolicy.Policy{Key: r.Key, Site: r.Site, EgressKey: r.EgressKey, Concurrency: r.Concurrency,
		BackoffInitial: time.Duration(r.BackoffInitialS) * time.Second, BackoffCap: time.Duration(r.BackoffCapS) * time.Second}
	for _, rs := range r.Rules {
		rule, err := fetchpolicy.ParseRule(rs)
		if err != nil {
			return p, "rules: " + err.Error()
		}
		p.Rules = append(p.Rules, rule)
	}
	if err := p.Validate(); err != nil {
		return p, err.Error()
	}
	return p, ""
}

// policy is the row's policy, and whether the row is valid.
func (r FetchPolicyRow) policy() (fetchpolicy.Policy, bool) {
	return r.parsed, r.Invalid == ""
}

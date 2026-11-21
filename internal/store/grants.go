package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
)

// Grant is a stored (principal, role, scope) tuple.
type Grant struct {
	Principal string
	Role      api.Role
	Scope     string
	GrantedBy string
	GrantedAt time.Time
}

// AddGrant grants role on scope to principal. Granting an existing grant is
// a no-op that keeps the original granted_by/at.
func (s *Store) AddGrant(ctx context.Context, principal string, role api.Role, scope, grantedBy string) error {
	return s.InTx(ctx, func(tx *Tx) error { return tx.AddGrant(ctx, principal, role, scope, grantedBy) })
}

// AddGrant is Store.AddGrant inside the transaction.
func (t *Tx) AddGrant(ctx context.Context, principal string, role api.Role, scope, grantedBy string) error {
	return addGrant(ctx, t.tx, principal, role, scope, grantedBy, t.s.now())
}

func addGrant(ctx context.Context, tx *sql.Tx, principal string, role api.Role, scope, grantedBy string, now time.Time) error {
	if principal == "" || role == "" || scope == "" || grantedBy == "" {
		return fmt.Errorf("%w: grant needs principal, role, scope and granter", ErrInvalid)
	}
	_, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO grants (principal, role, scope, granted_by, granted_at)
		VALUES (?,?,?,?,?)`, principal, string(role), scope, grantedBy, toMS(now))
	if err != nil {
		return fmt.Errorf("add grant: %w", err)
	}
	return nil
}

// RemoveGrant removes a grant. ErrNotFound if it did not exist.
//
// Removing a worker's process grant also cancels the worker's live leases
// on every queue it no longer holds process on (outcome cancelled; the
// task requeues without using an attempt, as on rejection), so revoking
// access to a queue's media takes effect at once rather than when the
// running task ends. The cancelled leases' ids are returned; the caller
// should wake waiting claims if there are any.
//
// It is a convenience for tests and tools: it does not report the fetching
// jobs the revocation fails for want of a downloader. The server revokes
// through Tx.RevokeGrant, which does.
func (s *Store) RemoveGrant(ctx context.Context, principal string, role api.Role, scope string) ([]string, error) {
	return inTx1(ctx, s, func(tx *Tx) ([]string, error) {
		r, err := tx.RevokeGrant(ctx, principal, role, scope)
		return r.Cancelled, err
	})
}

// Revocation is the effect of removing a grant.
type Revocation struct {
	// Cancelled are the leases of the principal that the revocation ended.
	Cancelled []string
	// Orphaned are the fetching jobs failed because no eligible downloader
	// was left, and the leases (any worker's) ended by failing them.
	Orphaned OrphanFailures
}

// RevokeGrant is RemoveGrant reporting the whole effect.
func (t *Tx) RevokeGrant(ctx context.Context, principal string, role api.Role, scope string) (Revocation, error) {
	var out Revocation
	res, err := t.tx.ExecContext(ctx, `DELETE FROM grants WHERE principal = ? AND role = ? AND scope = ?`,
		principal, string(role), scope)
	if err := mustAffect(res, err, "remove grant"); err != nil {
		return out, err
	}
	workerID, isWorker := strings.CutPrefix(principal, "worker:")
	if !isWorker || (role != api.RoleProcess && role != api.RoleFetch) {
		return out, nil
	}
	reason, cond := "process grant revoked", `t.kind <> 'fetch' AND NOT `+processGrantSQL
	if role == api.RoleFetch {
		reason, cond = "fetch grant revoked", `t.kind = 'fetch' AND NOT `+fetchGrantSQL
	}
	ls, err := liveLeases(ctx, t.tx, `l.worker_id = ? AND t.job_id IS NOT NULL AND `+cond, workerID, principal)
	if err != nil {
		return out, err
	}
	now := t.s.now()
	for _, l := range ls {
		end, err := t.s.endLease(ctx, t.tx, l, api.OutcomeCancelled, true, reason, "", now)
		if err != nil {
			return out, err
		}
		if end != nil {
			out.Cancelled = append(out.Cancelled, l.ID)
		}
	}
	if role == api.RoleFetch {
		if out.Orphaned, err = t.failAllOrphanedFetchJobs(ctx); err != nil {
			return out, err
		}
	}
	return out, nil
}

// GrantsFor lists a principal's grants ordered by role then scope.
func (s *Store) GrantsFor(ctx context.Context, principal string) ([]Grant, error) {
	return s.queryGrants(ctx, `SELECT principal, role, scope, granted_by, granted_at FROM grants
		WHERE principal = ? ORDER BY role, scope`, principal)
}

// PrincipalsWithRole lists the principals holding role on exactly scope.
func (s *Store) PrincipalsWithRole(ctx context.Context, role api.Role, scope string) ([]string, error) {
	gs, err := s.queryGrants(ctx, `SELECT principal, role, scope, granted_by, granted_at FROM grants
		WHERE role = ? AND scope = ? ORDER BY principal`, string(role), scope)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(gs))
	for i, g := range gs {
		out[i] = g.Principal
	}
	return out, nil
}

func (s *Store) queryGrants(ctx context.Context, q string, args ...any) ([]Grant, error) {
	rows, err := s.r.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query grants: %w", err)
	}
	defer rows.Close()
	var out []Grant
	for rows.Next() {
		var g Grant
		var at int64
		if err := rows.Scan(&g.Principal, &g.Role, &g.Scope, &g.GrantedBy, &at); err != nil {
			return nil, fmt.Errorf("query grants: %w", err)
		}
		g.GrantedAt = fromMS(at)
		out = append(out, g)
	}
	return out, rows.Err()
}

// ProcessQueues returns the ids of queues a worker holds the process role
// on, i.e. the QueueIDs of its claim eligibility. A "*" scope expands to
// every queue.
func (s *Store) ProcessQueues(ctx context.Context, workerID string) ([]int64, error) {
	rows, err := s.r.QueryContext(ctx, `
		SELECT q.id FROM queues q WHERE EXISTS (
			SELECT 1 FROM grants g WHERE g.principal = ? AND g.role = ?
			AND (g.scope = '*' OR g.scope = 'queue:' || q.id))
		ORDER BY q.id`, WorkerPrincipal(workerID), string(api.RoleProcess))
	if err != nil {
		return nil, fmt.Errorf("process queues: %w", err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("process queues: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

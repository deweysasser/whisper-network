package auth

import (
	"errors"
	"fmt"

	"github.com/deweysasser/whisper-network/internal/api"
)

// ErrForbidden is matched (errors.Is) by every authorization failure
// returned from this package.
var ErrForbidden = errors.New("forbidden")

// ForbiddenError describes why a request was refused. It matches
// ErrForbidden.
type ForbiddenError struct{ Reason string }

func (e *ForbiddenError) Error() string { return "forbidden: " + e.Reason }

// Is makes errors.Is(err, ErrForbidden) true.
func (e *ForbiddenError) Is(target error) bool { return target == ErrForbidden }

// queueGrantable reports the roles a queue_admin may hand out on its own
// queue. read_all is deliberately absent: queue_admin does not itself read
// transcripts, so it may not hand that power out (admin only).
func queueGrantable(r api.Role) bool {
	switch r {
	case api.RoleSubmit, api.RoleViewAll, api.RoleDeleteAll, api.RoleProcess,
		api.RoleSubmitURL, api.RoleFetch:
		return true
	}
	return false
}

// CanGrantTo reports whether actor may give the requested grants to an
// existing principal, or take them away from one: revoking a grant needs
// exactly the authority that granting it would. This is the administrative
// rule (DESIGN.md §5.2) and it never depends on what the actor merely holds:
//
//   - admin may grant anything;
//   - queue_admin on queue X may grant submit, view_all, delete_all and
//     process on queue:X, but not queue_admin itself, so queue admins cannot
//     create or remove other queue admins, nor read_all, which it does
//     not hold by virtue of being queue admin;
//   - worker_admin on "*" may grant the worker-scoped lifecycle grant
//     worker_admin on "worker:<id>".
//
// Every requested grant must be well formed. Workers can grant nothing. A
// nil error means every grant is allowed.
func CanGrantTo(actor Principal, requested []api.Grant) error {
	return checkGrants(actor, requested, Principal.mayAdminister)
}

// CanDelegateAtCreation reports whether actor may give the requested grants
// to a client it is creating (POST /client). It allows everything
// CanGrantTo allows and, in addition, re-delegation of a grant the actor
// holds itself (a client_admin may hand on a subset of its own grants, no
// escalation). Re-delegation applies only at creation: later changes to the
// client go through CanGrantTo.
func CanDelegateAtCreation(actor Principal, requested []api.Grant) error {
	return checkGrants(actor, requested, func(p Principal, g api.Grant) bool {
		return p.mayAdminister(g) || p.holds(g.Role, g.Scope)
	})
}

// CanRevokeClient reports whether actor may revoke the client clientID,
// which currently holds held. The actor needs client_admin (or admin) and
// must be able to have created that client: every grant it holds must pass
// CanDelegateAtCreation, so a client_admin cannot revoke clients more
// privileged than itself.
func CanRevokeClient(actor Principal, clientID string, held []api.Grant) error {
	if !Can(actor, ActRevokeClient, Resource{ClientID: clientID}) {
		return &ForbiddenError{Reason: "client_admin required"}
	}
	// A malformed stored grant must not make a client unrevocable by admin.
	if actor.isAdmin() {
		return nil
	}
	return CanDelegateAtCreation(actor, held)
}

func checkGrants(actor Principal, requested []api.Grant, may func(Principal, api.Grant) bool) error {
	if actor.Kind != KindUser && actor.Kind != KindClient {
		return &ForbiddenError{Reason: "principal kind may not grant"}
	}
	for _, g := range requested {
		if err := ValidateGrant(g); err != nil {
			return err
		}
		if !may(actor, g) {
			return &ForbiddenError{Reason: fmt.Sprintf("cannot grant %s on %s", g.Role, g.Scope)}
		}
	}
	return nil
}

// mayAdminister is CanGrantTo's rule for one well-formed grant.
func (p Principal) mayAdminister(g api.Grant) bool {
	switch {
	case p.isAdmin():
		return true
	case queueGrantable(g.Role):
		return p.holds(api.RoleQueueAdmin, g.Scope)
	case g.Role == api.RoleWorkerAdmin && nonEmptySuffix(g.Scope, workerScope):
		return p.holds(api.RoleWorkerAdmin, scopeAll)
	}
	return false
}

// CanApproveWorker checks the two separate acts of approving a worker
// (DESIGN.md §5.2): actor needs worker_admin over workerID (or admin), and
// queue_admin on every queue in queues (or admin). An empty queues list needs
// only the lifecycle right: an active worker with no process grants is legal.
func CanApproveWorker(actor Principal, workerID string, queues []string) error {
	if !Can(actor, ActApproveWorker, Resource{WorkerID: workerID}) {
		return &ForbiddenError{Reason: "worker_admin required for worker " + workerID}
	}
	for _, q := range queues {
		if !Can(actor, ActGrantProcess, Resource{Queue: q}) {
			return &ForbiddenError{Reason: "queue_admin required on queue " + q}
		}
	}
	return nil
}

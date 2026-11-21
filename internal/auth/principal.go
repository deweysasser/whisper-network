package auth

import (
	"strings"

	"github.com/deweysasser/whisper-network/internal/api"
)

// PrincipalKind says what sort of authenticated party a Principal is.
type PrincipalKind string

const (
	// KindUser is a human using the UI (or the API with a session).
	KindUser PrincipalKind = "user"
	// KindClient is an API client holding a wnc_ token.
	KindClient PrincipalKind = "client"
	// KindWorker is a transcription worker holding a wnw_ token.
	KindWorker PrincipalKind = "worker"
)

const (
	scopeAll    = "*"
	queuePrefix = "queue:"
	workerScope = "worker:"
)

// Principal is an authenticated party and the grants it currently holds.
// A principal with no grants is legal and can do almost nothing.
type Principal struct {
	Kind   PrincipalKind
	ID     string
	Grants []api.Grant
}

// QueueScope returns the grant scope string for the named queue.
func QueueScope(queue string) string { return queuePrefix + queue }

// WorkerScope returns the grant scope string for the given worker id.
func WorkerScope(workerID string) string { return workerScope + workerID }

// ValidateGrant reports whether g is a well-formed grant: a known role with a
// scope that role accepts. admin and client_admin take only "*"; worker_admin
// takes "*" or "worker:<id>"; queue_admin, submit, submit_url, view_all, read_all,
// delete_all, process and fetch take only "queue:<name>".
func ValidateGrant(g api.Grant) error {
	ok := false
	switch g.Role {
	case api.RoleAdmin, api.RoleClientAdmin:
		ok = g.Scope == scopeAll
	case api.RoleWorkerAdmin:
		ok = g.Scope == scopeAll || nonEmptySuffix(g.Scope, workerScope)
	case api.RoleQueueAdmin, api.RoleSubmit, api.RoleViewAll, api.RoleReadAll,
		api.RoleDeleteAll, api.RoleProcess, api.RoleFetch, api.RoleSubmitURL:
		ok = nonEmptySuffix(g.Scope, queuePrefix)
	default:
		return &ForbiddenError{Reason: "unknown role " + string(g.Role)}
	}
	if !ok {
		return &ForbiddenError{Reason: "scope " + g.Scope + " is not valid for role " + string(g.Role)}
	}
	return nil
}

func nonEmptySuffix(s, prefix string) bool {
	return strings.HasPrefix(s, prefix) && len(s) > len(prefix)
}

// holds reports whether p holds exactly role at exactly scope. Malformed
// grants never match, so a corrupt row cannot widen access.
func (p Principal) holds(role api.Role, scope string) bool {
	for _, g := range p.Grants {
		if g.Role == role && g.Scope == scope && ValidateGrant(g) == nil {
			return true
		}
	}
	return false
}

// holdsAny reports whether p holds role at any scope.
func (p Principal) holdsAny(role api.Role) bool {
	for _, g := range p.Grants {
		if g.Role == role && ValidateGrant(g) == nil {
			return true
		}
	}
	return false
}

func (p Principal) isAdmin() bool { return p.holds(api.RoleAdmin, scopeAll) }

// onQueue reports whether p holds role on the named queue. An empty queue
// name never matches.
func (p Principal) onQueue(role api.Role, queue string) bool {
	return queue != "" && p.holds(role, QueueScope(queue))
}

// workerAdminFor reports whether p holds worker_admin over workerID.
func (p Principal) workerAdminFor(workerID string) bool {
	if p.holds(api.RoleWorkerAdmin, scopeAll) {
		return true
	}
	return workerID != "" && p.holds(api.RoleWorkerAdmin, WorkerScope(workerID))
}

package ui

import (
	"bytes"
	"errors"
	"html/template"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/auth"
	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/server"
	"github.com/deweysasser/whisper-network/internal/store"
)

// Tunables. They are constants rather than configuration because the
// defaults are the policy; tests exercise them through the public routes.
const (
	// maxFormBytes bounds a form post (forms are small; grants lists are
	// the largest).
	maxFormBytes = 64 << 10
	// loginInterval and loginBurst rate-limit login attempts per client IP:
	// a burst of 10, then one attempt every 6 seconds.
	loginInterval = 6 * time.Second
	loginBurst    = 10
	// minPasswordLen is the shortest new password the UI accepts.
	minPasswordLen = 12
	// onlineWindow is how recently an active worker must have been seen to
	// count as online.
	onlineWindow = 3 * time.Minute
)

// UI is the web UI. Create it with New; it is an http.Handler for /ui/.
type UI struct {
	srv    *server.Server
	log    zerolog.Logger
	pages  map[string]*template.Template
	assets map[string]asset
	logins *auth.RateLimiter
	// pwChecks rate-limits current-password checks on /ui/password per
	// user, on top of the per-IP logins limiter, so a stolen session
	// cannot guess its user's password from many addresses.
	pwChecks *auth.RateLimiter
	share    *shareTracker
	mux      *http.ServeMux
}

// New builds the UI for s. Its signature matches server.Deps.UI. Templates
// and assets are parsed once here; a malformed embedded template panics,
// which the package tests catch at build time.
func New(s *server.Server) http.Handler {
	u := &UI{
		srv:    s,
		log:    s.Logger(),
		assets: loadAssets(),
		logins: auth.NewRateLimiter(auth.LimiterConfig{Interval: loginInterval, Burst: loginBurst, Now: s.Now}),
		pwChecks: auth.NewRateLimiter(auth.LimiterConfig{Interval: loginInterval, Burst: loginBurst,
			Now: s.Now}),
		share: newShareTracker(servedCostReader(s.Metrics())),
	}
	u.pages = parsePages(u.funcs())
	u.mux = u.routes()

	cop := http.NewCrossOriginProtection()
	cop.SetDenyHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.fail(w, r, nil, u.deny(r, nil, "cross-origin request"))
	}))
	return securityHeaders(cop.Handler(u.mux))
}

// routes registers the UI endpoints. Mutations are POST only; the mux
// answers other methods with 405.
func (u *UI) routes() *http.ServeMux {
	m := http.NewServeMux()
	m.HandleFunc("GET /ui/static/{name}", u.handleStatic)
	m.HandleFunc("GET /ui/login", u.handleLoginForm)
	m.HandleFunc("POST /ui/login", u.handleLogin)
	m.Handle("POST /ui/logout", u.authed(u.handleLogout))
	m.Handle("GET /ui/password", u.authed(u.handlePasswordForm))
	m.Handle("POST /ui/password", u.authed(u.handlePassword))

	m.Handle("GET /ui/{$}", u.authed(u.handleDashboard))
	m.Handle("GET /ui/dashboard/stats", u.authed(u.handleDashboardStats))

	m.Handle("GET /ui/workers", u.authed(u.handleWorkers))
	m.Handle("GET /ui/workers/{id}", u.authed(u.handleWorker))
	m.Handle("POST /ui/workers/delete-rejected", u.authed(u.handleDeleteRejectedWorkers))
	m.Handle("POST /ui/workers/{id}/approve", u.authed(u.handleApproveWorker))
	m.Handle("POST /ui/workers/{id}/delete", u.authed(u.handleDeleteWorker))
	m.Handle("POST /ui/workers/{id}/{verb}", u.authed(u.handleWorkerState))

	m.Handle("GET /ui/queues", u.authed(u.handleQueues))
	m.Handle("POST /ui/queues", u.authed(u.handleCreateQueue))
	m.Handle("GET /ui/queues/{q}", u.authed(u.handleQueue))
	m.Handle("POST /ui/queues/{q}", u.authed(u.handlePatchQueue))
	m.Handle("POST /ui/queues/{q}/bulk", u.authed(u.handleBulk))

	m.Handle("GET /ui/jobs/{id}", u.authed(u.handleJob))
	m.Handle("POST /ui/jobs/{id}/delete", u.authed(u.handleDeleteJob))
	m.Handle("POST /ui/jobs/{id}/retry", u.authed(u.handleRetryJob))

	m.Handle("GET /ui/users", u.authed(u.handleUsers))
	m.Handle("POST /ui/users", u.authed(u.handleCreateUser))
	m.Handle("POST /ui/users/{name}/disable", u.authed(u.handleDisableUser))

	m.Handle("GET /ui/clients", u.authed(u.handleClients))
	m.Handle("POST /ui/clients", u.authed(u.handleCreateClient))
	m.Handle("POST /ui/clients/{id}/revoke", u.authed(u.handleRevokeClient))
	m.Handle("GET /ui/clients/{id}", u.authed(u.handleClient))
	m.Handle("POST /ui/clients/{id}", u.authed(u.handleUpdateClient))

	m.Handle("POST /ui/grants", u.authed(u.handleGrant))
	m.Handle("POST /ui/grants/revoke", u.authed(u.handleRevoke))

	m.Handle("GET /ui/fetch", u.authed(u.handleFetch))
	m.Handle("POST /ui/fetch/policies", u.authed(u.handlePutFetchPolicy))
	m.Handle("POST /ui/fetch/policies/{key}/delete", u.authed(u.handleDeleteFetchPolicy))
	m.Handle("POST /ui/fetch/cooldowns", u.authed(u.handleSetCooldown))
	m.Handle("POST /ui/fetch/cooldowns/clear", u.authed(u.handleClearCooldown))

	m.Handle("GET /ui/audit", u.authed(u.handleAudit))
	// No catch-all: unknown paths get the mux's 404, and a known path with
	// the wrong method gets 405 (a GET can never reach a mutation).
	return m
}

// securityHeaders sets the headers every UI response carries (DESIGN.md
// §13): no framing, no sniffing, no cross-origin referrers, and a CSP that
// allows only this origin's scripts and styles (no inline code at all).
func securityHeaders(next http.Handler) http.Handler {
	const csp = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; " +
		"connect-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'; object-src 'none'"
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", csp)
		h.Set("X-Frame-Options", "DENY")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "same-origin")
		h.Set("Cross-Origin-Opener-Policy", "same-origin")
		next.ServeHTTP(w, r)
	})
}

// reqCtx is the authenticated request: the principal (with grants), its
// user and session.
type reqCtx struct {
	P    *server.Principal
	User *store.User
	CSRF string
}

// can is shorthand for auth.Can on the request's principal.
func (rc *reqCtx) can(a auth.Action, res auth.Resource) bool { return auth.Can(rc.P.Principal, a, res) }

// handler is an authenticated UI handler. A returned error is rendered as
// an error page (see fail).
type handler func(w http.ResponseWriter, r *http.Request, rc *reqCtx) error

// authed resolves the session, enforces CSRF on mutations and the forced
// password change, then calls h. Without a session, the browser is sent to
// the login page with a return path.
func (u *UI) authed(h handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		safe := r.Method == http.MethodGet || r.Method == http.MethodHead
		if !safe {
			r.Body = http.MaxBytesReader(w, r.Body, maxFormBytes)
		}
		sess, user, err := u.srv.SessionFromRequest(r)
		if err != nil {
			u.fail(w, r, nil, err)
			return
		}
		if sess == nil {
			u.toLogin(w, r)
			return
		}
		p, err := u.srv.PrincipalForUser(r.Context(), user)
		if err != nil {
			u.fail(w, r, nil, err)
			return
		}
		p.Session = sess
		rc := &reqCtx{P: p, User: user, CSRF: sess.CSRF}
		if !safe && !u.srv.CheckCSRF(r, sess) {
			u.fail(w, r, rc, u.deny(r, rc, "missing or bad CSRF token"))
			return
		}
		if user.MustChangePassword && r.URL.Path != "/ui/password" && r.URL.Path != "/ui/logout" {
			u.redirect(w, r, "/ui/password")
			return
		}
		if err := h(w, r, rc); err != nil {
			u.fail(w, r, rc, err)
		}
	})
}

// toLogin sends a request without a valid session to the login page. GET
// requests keep their path as the return target; htmx requests are told to
// navigate (from the page they were made on) rather than swap in a login form.
func (u *UI) toLogin(w http.ResponseWriter, r *http.Request) {
	next := ""
	if r.Method == http.MethodGet {
		next = r.URL.RequestURI()
	}
	if isHTMX(r) {
		next = ""
		if cur := r.Header.Get("HX-Current-URL"); cur != "" {
			next = localPath(cur)
		}
	}
	target := "/ui/login"
	if next = safeNext(next); next != "/ui/" {
		target += "?next=" + url.QueryEscape(next)
	}
	if isHTMX(r) {
		w.Header().Set("HX-Redirect", target)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}

// redirect navigates after a successful action: 303 for forms, HX-Redirect
// for htmx.
func (u *UI) redirect(w http.ResponseWriter, r *http.Request, target string) {
	if isHTMX(r) {
		w.Header().Set("HX-Redirect", target)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}

func isHTMX(r *http.Request) bool { return r.Header.Get("HX-Request") == "true" }

// uiError is an error page with a status and a message safe to show.
type uiError struct {
	status int
	msg    string
}

func (e *uiError) Error() string { return e.msg }

var errNotFound = &uiError{http.StatusNotFound, "Not found."}

func badRequest(msg string) error { return &uiError{http.StatusBadRequest, msg} }

// deny logs auth.denied (as the API does) and returns a 403 page error.
func (u *UI) deny(r *http.Request, rc *reqCtx, reason string) error {
	u.logDenied(r, rc, reason)
	return &uiError{http.StatusForbidden, "You do not have permission to do that."}
}

// hide logs auth.denied and returns 404, for resources whose existence must
// not leak (jobs, DESIGN.md §5.3).
func (u *UI) hide(r *http.Request, rc *reqCtx, reason string) error {
	u.logDenied(r, rc, reason)
	return errNotFound
}

func (u *UI) logDenied(r *http.Request, rc *reqCtx, reason string) {
	name := ""
	if rc != nil {
		name = rc.P.Name
	}
	obs.Warn(&u.log, obs.EventAuthDenied).Str(obs.FieldPrincipal, name).Str(obs.FieldRoute, routeOf(r)).
		Str(obs.FieldRemote, u.srv.ClientIP(r)).Str(obs.FieldReason, reason).Msg("request denied")
}

// routeOf returns the matched pattern without its method, never the raw
// path (which carries job and worker IDs).
func routeOf(r *http.Request) string {
	if r.Pattern == "" {
		return "unmatched"
	}
	if _, p, ok := strings.Cut(r.Pattern, " "); ok {
		return p
	}
	return r.Pattern
}

// fail renders err as an error page. Server and store errors map to the
// status the API would use; internal errors are logged and shown
// generically. htmx requests get the message swapped into the page's flash
// area instead of a full page.
func (u *UI) fail(w http.ResponseWriter, r *http.Request, rc *reqCtx, err error) {
	status, msg := http.StatusInternalServerError, "Something went wrong."
	var ue *uiError
	var mbe *http.MaxBytesError
	switch {
	case errors.As(err, &ue):
		status, msg = ue.status, ue.msg
	case errors.As(err, &mbe):
		status, msg = http.StatusRequestEntityTooLarge, "The form is too large."
	default:
		status, msg = server.ErrorStatus(err)
		if status >= 500 {
			u.log.Error().Err(err).Str(obs.FieldRoute, routeOf(r)).Str(obs.FieldMethod, r.Method).Msg("ui request failed")
			msg = "Something went wrong."
		}
	}
	if isHTMX(r) {
		w.Header().Set("HX-Retarget", "#flash")
		w.Header().Set("HX-Reswap", "innerHTML")
		u.renderFragment(w, status, "error", "flash", msg)
		return
	}
	u.render(w, r, rc, status, "error", http.StatusText(status), errorView{Status: status, Message: msg})
}

type errorView struct {
	Status  int
	Message string
}

// view is the root of every page template.
type view struct {
	Title  string
	User   string // username; empty on the login page
	CSRF   string
	Nav    navLinks
	HTMX   string // asset URLs
	JS     string
	CSS    string
	Active string // nav section
	Data   any
}

// navLinks says which sections the principal may open.
type navLinks struct {
	Workers, Users, Clients, Fetch, Audit bool
}

func (rc *reqCtx) nav() navLinks {
	if rc == nil {
		return navLinks{}
	}
	return navLinks{
		Workers: mayListWorkers(rc.P),
		Users:   rc.can(auth.ActCreateUser, auth.Resource{}),
		Clients: rc.can(auth.ActCreateClient, auth.Resource{}),
		Fetch:   rc.can(auth.ActManageFetchPolicy, auth.Resource{}),
		Audit:   rc.can(auth.ActViewAudit, auth.Resource{}),
	}
}

// render executes page's layout with data and writes it with status. Pages
// are never cached: some carry one-time secrets.
func (u *UI) render(w http.ResponseWriter, r *http.Request, rc *reqCtx, status int, page, title string, data any) {
	v := view{Title: title, Nav: rc.nav(), HTMX: u.assetURL("htmx.min.js"), JS: u.assetURL("ui.js"), CSS: u.assetURL("style.css"),
		Active: page, Data: data}
	if rc != nil {
		v.User, v.CSRF = rc.User.Username, rc.CSRF
	}
	u.write(w, r, status, page, "layout", v)
}

// renderFragment executes one named template of page (an htmx partial).
func (u *UI) renderFragment(w http.ResponseWriter, status int, page, name string, data any) {
	u.write(w, nil, status, page, name, data)
}

func (u *UI) write(w http.ResponseWriter, r *http.Request, status int, page, name string, data any) {
	var buf bytes.Buffer
	if err := u.pages[page].ExecuteTemplate(&buf, name, data); err != nil {
		ev := u.log.Error().Err(err).Str("template", page+"/"+name)
		if r != nil {
			ev = ev.Str(obs.FieldRoute, routeOf(r))
		}
		ev.Msg("ui template failed")
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = buf.WriteTo(w)
}

// holdsRole reports whether p holds role on any scope (as admin.go does).
func holdsRole(p *server.Principal, role api.Role) bool {
	for _, g := range p.Grants {
		if g.Role == role {
			return true
		}
	}
	return false
}

// mayListWorkers mirrors GET /admin/workers: admin, worker_admin or
// queue_admin on anything.
func mayListWorkers(p *server.Principal) bool {
	return holdsRole(p, api.RoleAdmin) || holdsRole(p, api.RoleWorkerAdmin) || holdsRole(p, api.RoleQueueAdmin)
}

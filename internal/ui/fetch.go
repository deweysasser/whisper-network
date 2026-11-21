package ui

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/auth"
	"github.com/deweysasser/whisper-network/internal/fetchpolicy"
	"github.com/deweysasser/whisper-network/internal/server"
)

// fetchPath is the fetch page, where an admin manages per-site pacing
// policies and cooldowns. Nothing here ever shows a job's URL, only site and
// egress keys.
const fetchPath = "/ui/fetch"

// fetchView is the fetch page. Error and the two forms are set when a
// submission was refused, so the page shows why with the input kept.
type fetchView struct {
	CSRF      string
	Policies  []policyRow
	Cooldowns []cooldownRow
	// Egress lists the egress keys of current downloaders, offered when
	// setting a cooldown (a key may also be typed).
	Egress   []string
	Error    string
	Policy   policyForm
	Cooldown cooldownForm
	// Editing is the key being edited, for the form's heading.
	Editing string
	// AnyInvalid is set when some policy row is invalid.
	AnyInvalid bool
}

// policyForm is the policy form's field values.
type policyForm struct {
	Key, Rules, Concurrency, BackoffInitial, BackoffCap string
}

// cooldownForm is the cooldown form's field values.
type cooldownForm struct {
	EgressKey, Site, Duration, Reason string
}

// policyRow is one stored policy, valid or not.
type policyRow struct {
	policyForm
	UpdatedBy    string
	UpdatedAt    time.Time
	Invalid      string // why it does not validate; empty if valid
	CanDelete    bool
	EditHref     string
	DeleteAction string
	RulesNone    bool
}

// cooldownRow is a cooldown in force.
type cooldownRow struct {
	api.FetchCooldown
	Left time.Duration
}

func (u *UI) fetchView(ctx context.Context, rc *reqCtx, edit string) (*fetchView, error) {
	st := u.srv.Store()
	rows, err := st.FetchPolicyRows(ctx)
	if err != nil {
		return nil, err
	}
	v := &fetchView{CSRF: rc.CSRF}
	for _, r := range rows {
		pr := policyRow{policyForm: policyForm{Key: r.Key, Concurrency: strconv.Itoa(r.Concurrency),
			BackoffInitial: fetchpolicy.CompactDuration(time.Duration(r.BackoffInitialS) * time.Second),
			BackoffCap:     fetchpolicy.CompactDuration(time.Duration(r.BackoffCapS) * time.Second)},
			UpdatedBy: r.UpdatedBy, UpdatedAt: r.UpdatedAt, Invalid: r.Invalid, CanDelete: r.Key != "*",
			EditHref:     fetchPath + "?edit=" + url.QueryEscape(r.Key) + "#policy-form",
			DeleteAction: fetchPath + "/policies/" + url.PathEscape(r.Key) + "/delete"}
		rules := make([]string, len(r.Rules))
		for i, rs := range r.Rules {
			rules[i] = rs
			if ru, err := fetchpolicy.ParseRule(rs); err == nil {
				rules[i] = ru.String()
			}
		}
		pr.Rules = strings.Join(rules, " ")
		pr.RulesNone = len(rules) == 0
		if r.Invalid != "" {
			v.AnyInvalid = true
		}
		v.Policies = append(v.Policies, pr)
		if r.Key == edit {
			v.Policy, v.Editing = pr.policyForm, r.Key
		}
	}
	cs, err := st.FetchCooldowns(ctx, u.srv.Now())
	if err != nil {
		return nil, err
	}
	for _, c := range cs {
		v.Cooldowns = append(v.Cooldowns, cooldownRow{FetchCooldown: c, Left: max(c.Until.Sub(u.srv.Now()), 0)})
	}
	ws, err := st.ListWorkers(ctx)
	if err != nil {
		return nil, err
	}
	for i := range ws {
		if ws[i].Kind == api.WorkerDownloader && ws[i].State != api.WorkerRejected &&
			ws[i].EgressKey != "" && !slices.Contains(v.Egress, ws[i].EgressKey) {
			v.Egress = append(v.Egress, ws[i].EgressKey)
		}
	}
	slices.Sort(v.Egress)
	return v, nil
}

// handleFetch shows policies and cooldowns (admin).
func (u *UI) handleFetch(w http.ResponseWriter, r *http.Request, rc *reqCtx) error {
	if !rc.can(auth.ActManageFetchPolicy, auth.Resource{}) {
		return u.deny(r, rc, "admin required")
	}
	v, err := u.fetchView(r.Context(), rc, r.URL.Query().Get("edit"))
	if err != nil {
		return err
	}
	u.render(w, r, rc, http.StatusOK, "fetch", "Fetch", v)
	return nil
}

// fetchRefused re-renders the fetch page with err's message and the
// submitted form, at the status the API would use. Server errors are passed
// on to the generic error page.
func (u *UI) fetchRefused(w http.ResponseWriter, r *http.Request, rc *reqCtx, err error, pf policyForm, cf cooldownForm) error {
	status, msg := http.StatusBadRequest, ""
	var ue *uiError
	if errors.As(err, &ue) {
		status, msg = ue.status, ue.msg
	} else {
		status, msg = server.ErrorStatus(err)
	}
	if status >= 500 {
		return err
	}
	v, verr := u.fetchView(r.Context(), rc, "")
	if verr != nil {
		return verr
	}
	v.Error, v.Policy, v.Cooldown = msg, pf, cf
	u.render(w, r, rc, status, "fetch", "Fetch", v)
	return nil
}

// handlePutFetchPolicy creates or replaces a policy (admin, as PUT
// /admin/fetch-policies/{key}), parsed by the same code as the API.
func (u *UI) handlePutFetchPolicy(w http.ResponseWriter, r *http.Request, rc *reqCtx) error {
	if !rc.can(auth.ActManageFetchPolicy, auth.Resource{}) {
		return u.deny(r, rc, "admin required")
	}
	if err := r.ParseForm(); err != nil {
		return badRequest("Malformed form.")
	}
	pf := policyForm{Key: strings.TrimSpace(r.PostFormValue("key")), Rules: strings.TrimSpace(r.PostFormValue("rules")),
		Concurrency:    strings.TrimSpace(r.PostFormValue("concurrency")),
		BackoffInitial: strings.TrimSpace(r.PostFormValue("backoff_initial")),
		BackoffCap:     strings.TrimSpace(r.PostFormValue("backoff_cap"))}
	conc, err := strconv.Atoi(pf.Concurrency)
	if err != nil {
		return u.fetchRefused(w, r, rc, badRequest("Concurrency must be a whole number."), pf, cooldownForm{})
	}
	fp, err := server.ParseFetchPolicy(pf.Key, strings.Fields(pf.Rules), conc, pf.BackoffInitial, pf.BackoffCap)
	if err != nil {
		return u.fetchRefused(w, r, rc, err, pf, cooldownForm{})
	}
	if _, err := u.srv.Ops().PutFetchPolicy(r.Context(), rc.P.Actor(), fp); err != nil {
		return u.fetchRefused(w, r, rc, err, pf, cooldownForm{})
	}
	u.redirect(w, r, fetchPath)
	return nil
}

// handleDeleteFetchPolicy deletes a policy (admin; the * default is refused).
func (u *UI) handleDeleteFetchPolicy(w http.ResponseWriter, r *http.Request, rc *reqCtx) error {
	if !rc.can(auth.ActManageFetchPolicy, auth.Resource{}) {
		return u.deny(r, rc, "admin required")
	}
	if err := u.srv.Ops().DeleteFetchPolicy(r.Context(), rc.P.Actor(), r.PathValue("key")); err != nil {
		return u.fetchRefused(w, r, rc, err, policyForm{}, cooldownForm{})
	}
	u.redirect(w, r, fetchPath)
	return nil
}

// handleSetCooldown starts a manual cooldown (admin, as POST
// /admin/fetch-cooldowns).
func (u *UI) handleSetCooldown(w http.ResponseWriter, r *http.Request, rc *reqCtx) error {
	if !rc.can(auth.ActManageFetchPolicy, auth.Resource{}) {
		return u.deny(r, rc, "admin required")
	}
	if err := r.ParseForm(); err != nil {
		return badRequest("Malformed form.")
	}
	cf := cooldownForm{EgressKey: strings.TrimSpace(r.PostFormValue("egress_key")), Site: strings.TrimSpace(r.PostFormValue("site")),
		Duration: strings.TrimSpace(r.PostFormValue("duration")), Reason: strings.TrimSpace(r.PostFormValue("reason"))}
	d, err := server.ParseCooldownDuration(cf.Duration)
	if err != nil {
		return u.fetchRefused(w, r, rc, err, policyForm{}, cf)
	}
	if err := u.srv.Ops().SetCooldown(r.Context(), rc.P.Actor(), cf.EgressKey, cf.Site, u.srv.Now().Add(d), cf.Reason); err != nil {
		return u.fetchRefused(w, r, rc, err, policyForm{}, cf)
	}
	u.redirect(w, r, fetchPath)
	return nil
}

// handleClearCooldown ends a cooldown now (admin).
func (u *UI) handleClearCooldown(w http.ResponseWriter, r *http.Request, rc *reqCtx) error {
	if !rc.can(auth.ActManageFetchPolicy, auth.Resource{}) {
		return u.deny(r, rc, "admin required")
	}
	if err := r.ParseForm(); err != nil {
		return badRequest("Malformed form.")
	}
	if err := u.srv.Ops().ClearCooldown(r.Context(), rc.P.Actor(), r.PostFormValue("egress_key"), r.PostFormValue("site")); err != nil {
		return u.fetchRefused(w, r, rc, err, policyForm{}, cooldownForm{})
	}
	u.redirect(w, r, fetchPath)
	return nil
}

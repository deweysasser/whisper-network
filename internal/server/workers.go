package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/netip"
	"regexp"
	"strconv"
	"unicode/utf8"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/auth"
	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/store"
)

func itoa(n int) string     { return strconv.Itoa(n) }
func itoa64(n int64) string { return strconv.FormatInt(n, 10) }

// validateFacts bounds what a worker may report about itself. The values
// end up in the UI and in metric labels, so they must stay small.
func validateFacts(f *api.WorkerFacts) error {
	// In field order, so a report with several bad fields always names
	// the same one.
	for _, fv := range []struct{ name, v string }{
		{"hostname", f.Hostname}, {"cpu_model", f.CPUModel}, {"os", f.OS}, {"arch", f.Arch},
		{"engine_version", f.EngineVersion}, {"ffmpeg_version", f.FFmpegVersion}, {"version", f.Version},
		{"yt_dlp_version", f.YtDlpVersion},
	} {
		if len(fv.v) > 200 || !utf8.ValidString(fv.v) {
			return badRequest(fv.name + " must be valid UTF-8 of at most 200 bytes")
		}
	}
	if f.Hostname == "" {
		return badRequest("hostname is required")
	}
	if f.Description == "" {
		f.Description = f.Hostname
	}
	if len(f.Description) > 1000 || !utf8.ValidString(f.Description) {
		return badRequest("description must be valid UTF-8 of at most 1000 bytes")
	}
	if f.SandboxLevel.Rank() < 0 {
		return badRequest("sandbox_level must be full, fs-only or none")
	}
	if len(f.Models) > 64 || len(f.Kinds) > 2 {
		return badRequest("too many models or kinds")
	}
	switch f.Kind {
	case "", api.WorkerProcessing, api.WorkerDownloader:
	default:
		return badRequest("kind must be processing or downloader")
	}
	for _, k := range f.Kinds {
		if err := kindAllowed(f.Kind, k); err != nil {
			return err
		}
	}
	if f.Kind != api.WorkerDownloader && (f.EgressGroup != "" || f.EgressIP != "" || f.YtDlpVersion != "") {
		return badRequest("egress_group, egress_ip and yt_dlp_version are for downloaders")
	}
	if f.EgressGroup != "" && !egressGroupRE.MatchString(f.EgressGroup) {
		return badRequest("egress_group must be 1-64 letters, digits, '.', '_' or '-'")
	}
	if f.EgressIP != "" {
		if _, err := netip.ParseAddr(f.EgressIP); err != nil {
			return badRequest("egress_ip must be an IP address")
		}
	}
	for _, m := range f.Models {
		if len(m) > 100 {
			return badRequest("model name too long")
		}
	}
	if f.Cores < 0 || f.MemBytes < 0 || f.Slots < 0 || f.Slots > 1024 {
		return badRequest("cores, mem_bytes and slots must be sensible")
	}
	return nil
}

// egressGroupRE bounds a downloader's --egress-group: it becomes the egress
// key label:<name>, which appears in fetch policy keys (site@label:<name>),
// logs and the UI.
var egressGroupRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// kindAllowed checks that a worker of kind wk may run (list, or claim) task
// kind k: a downloader runs only fetch tasks, a processing worker only
// prepare and transcribe.
func kindAllowed(wk api.WorkerKind, k api.TaskKind) error {
	switch {
	case wk == api.WorkerDownloader && k == api.KindFetch:
		return nil
	case wk == api.WorkerDownloader:
		return badRequest("a downloader runs only fetch tasks, not " + string(k))
	case k == api.KindPrepare || k == api.KindTranscribe:
		return nil
	case k == api.KindFetch:
		return badRequest("only a downloader runs fetch tasks")
	}
	return badRequest("unknown task kind " + string(k))
}

// handleRegister is POST /worker (unauthenticated). Registration grants
// nothing; it is rate limited per client IP, and the number of workers
// awaiting approval is capped both per source address and in total
// (DESIGN.md §13).
func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if !s.regLimiter.Allow(s.clientIP(r)) {
		p := newProblem(http.StatusTooManyRequests, api.ProblemRateLimited, "too many registrations from this address")
		p.retryAfter = int(s.cfg.RegisterInterval.Seconds())
		return p
	}
	var facts api.WorkerFacts
	if err := decodeWorkerJSON(w, r, &facts, false, maxJSONBody); err != nil {
		return err
	}
	if err := validateFacts(&facts); err != nil {
		return err
	}
	token, err := auth.NewWorkerToken()
	if err != nil {
		return err
	}
	source := auth.IPKey(s.clientIP(r))
	wk, err := s.ops.RegisterWorker(ctx, auth.HashToken(token), facts, store.PendingLimits{
		Total: s.cfg.MaxPendingWorkers, PerSource: s.cfg.MaxPendingPerSource, Source: source})
	if errors.Is(err, store.ErrPendingSourceFull) {
		return tooMany("too many workers from this address are awaiting approval; try again later", 600)
	}
	if errors.Is(err, store.ErrPendingFull) {
		p := newProblem(http.StatusServiceUnavailable, api.ProblemRateLimited,
			"too many workers are awaiting approval; try again later")
		p.retryAfter = 600
		return p
	}
	if err != nil {
		return err
	}
	obs.Info(&s.log, obs.EventWorkerRegistered).Str(obs.FieldWorkerID, wk.ID).
		Str(obs.FieldHostname, facts.Hostname).Str(obs.FieldSandboxLevel, string(facts.SandboxLevel)).
		Str(obs.FieldRemote, s.clientIP(r)).Msg("worker registered")
	w.Header().Set("Location", s.publicURL("/worker/"+wk.ID))
	writeJSON(w, http.StatusCreated, api.RegisterResponse{ID: wk.ID, Token: token, State: wk.State})
	return nil
}

// workerAPI builds the wire document of a worker.
func (s *Server) workerAPI(ctx context.Context, wk *store.Worker) (*api.Worker, error) {
	gs, err := s.ops.WireGrants(ctx, wk.Principal())
	if err != nil {
		return nil, err
	}
	out := &api.Worker{ID: wk.ID, State: wk.State, Facts: wk.Facts, Queues: []string{}, LastSeenAt: wk.LastSeenAt}
	role := workerRole(wk.Kind) // process, or fetch for a downloader
	for _, g := range gs {
		if g.Role == role {
			out.Queues = append(out.Queues, g.Scope[len("queue:"):])
		}
	}
	s.workerHints(out)
	return out, nil
}

// targetWorker loads the worker addressed by the path and checks act. A
// worker asking about another worker gets 404.
func (s *Server) targetWorker(r *http.Request, p *Principal, act auth.Action) (*store.Worker, error) {
	id := r.PathValue("id")
	if p.Kind == auth.KindWorker && p.ID != id {
		return nil, s.hide(r, p, "another worker")
	}
	if !auth.Can(p.Principal, act, auth.Resource{WorkerID: id}) {
		return nil, s.deny(r, p, "not allowed on worker "+id)
	}
	return s.st.WorkerByID(r.Context(), id)
}

func (s *Server) handleGetWorker(w http.ResponseWriter, r *http.Request, p *Principal) error {
	wk, err := s.targetWorker(r, p, auth.ActViewWorker)
	if err != nil {
		return err
	}
	// A pending worker polls here until it is approved. Its polls are not
	// recorded: pending expiry counts from registration, and an unapproved
	// (unauthenticated in all but name) caller must not cause writes.
	if p.Worker != nil && p.Worker.State != api.WorkerPending {
		if err := s.st.TouchWorker(r.Context(), wk.ID, nil); err != nil {
			return err
		}
	}
	out, err := s.workerAPI(r.Context(), wk)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) handlePatchWorker(w http.ResponseWriter, r *http.Request, p *Principal) error {
	wk, err := s.targetWorker(r, p, auth.ActUpdateWorker)
	if err != nil {
		return err
	}
	var facts api.WorkerFacts
	if err := decodeWorkerJSON(w, r, &facts, false, maxJSONBody); err != nil {
		return err
	}
	if err := validateFacts(&facts); err != nil {
		return err
	}
	if err := s.ops.UpdateWorkerFacts(r.Context(), p.Actor(), wk.ID, facts); err != nil {
		return err
	}
	if err := s.st.TouchWorker(r.Context(), wk.ID, nil); err != nil {
		return err
	}
	if wk, err = s.st.WorkerByID(r.Context(), wk.ID); err != nil {
		return err
	}
	out, err := s.workerAPI(r.Context(), wk)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

// gaugesOf decodes stored heartbeat gauges, nil if absent or unreadable.
func gaugesOf(raw json.RawMessage) *api.Gauges {
	if len(raw) == 0 {
		return nil
	}
	var g api.Gauges
	if json.Unmarshal(raw, &g) != nil {
		return nil
	}
	return &g
}

// sanitizeGauges bounds what a worker reports in its gauges before it is
// stored and shown: negative readings and loads outside 0–100% are dropped, and an oversized cached
// model list is dropped rather than failing the heartbeat or claim.
func sanitizeGauges(g *api.Gauges) *api.Gauges {
	if g == nil {
		return nil
	}
	for _, p := range []**int64{&g.ModelsBytes, &g.WorkBytes, &g.StateFreeBytes} {
		if *p != nil && **p < 0 {
			*p = nil
		}
	}
	if g.HostLoad != nil && !(*g.HostLoad >= 0 && *g.HostLoad <= 100) {
		g.HostLoad = nil
	}
	if g.ClaimsDeferred != nil && *g.ClaimsDeferred < 0 {
		g.ClaimsDeferred = nil
	}
	if len(g.CachedModels) > api.MaxCachedModels {
		g.CachedModels = nil
	}
	for _, m := range g.CachedModels {
		if len(m.Name) > 100 || !utf8.ValidString(m.Name) || m.Bytes < 0 {
			g.CachedModels = nil
			break
		}
	}
	return g
}

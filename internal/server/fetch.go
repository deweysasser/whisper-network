package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/auth"
	"github.com/deweysasser/whisper-network/internal/blobs"
	"github.com/deweysasser/whisper-network/internal/egress"
	"github.com/deweysasser/whisper-network/internal/fetchpolicy"
	"github.com/deweysasser/whisper-network/internal/media"
	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/store"
)

// URL jobs (POST /queue/{q}/fetch): the server never touches the URL
// itself. It validates it, derives its site key, and hands it to a
// downloader worker in a fetch lease; the downloader streams the media back
// as the lease's result, which then goes through the same ingest as an
// upload. The URL is job content: it never appears in logs, metric labels
// or audit details, which carry the site key instead.

// --- submission ---

// handleFetchSubmit is POST /queue/{q}/fetch: create a URL job (JSON
// api.FetchRequest). It needs submit_url on the queue and an eligible
// downloader (QueueHasDownloader), else 409 no-downloader. Idempotency-Key
// works as for uploads.
func (s *Server) handleFetchSubmit(w http.ResponseWriter, r *http.Request, p *Principal) error {
	ctx := r.Context()
	q, err := s.st.QueueByName(ctx, r.PathValue("q"))
	if err != nil {
		return err
	}
	if !auth.Can(p.Principal, auth.ActSubmitURL, auth.Resource{Queue: q.Name}) {
		return s.deny(r, p, "submit_url not granted on queue "+q.Name)
	}
	key := r.Header.Get("Idempotency-Key")
	if len(key) > maxIdempotencyKey {
		return badRequest("Idempotency-Key is too long")
	}
	if key != "" {
		j, err := s.st.JobByIdempotencyKey(ctx, p.ID, key)
		if err == nil {
			w.Header().Set("Location", s.publicURL("/job/"+j.ID))
			writeJSON(w, http.StatusOK, j.API(s.now()))
			return nil
		}
		if !errors.Is(err, store.ErrNotFound) {
			return err
		}
	}
	var fr api.FetchRequest
	if err := decodeJSON(w, r, &fr, false); err != nil {
		return err
	}
	site, err := validFetchURL(fr.URL)
	if err != nil {
		return err
	}
	if fr.Source == "" {
		fr.Source = fr.URL
	}
	if err := s.validateParams(&fr.JobParams); err != nil {
		return err
	}
	ok, err := s.st.QueueHasDownloader(ctx, q.ID, s.st.FetchMinSandbox())
	if err != nil {
		return err
	}
	if !ok {
		return newProblem(http.StatusConflict, api.ProblemNoDownloader,
			"no downloader may fetch for queue "+q.Name)
	}
	job, created, err := s.ops.CreateFetchJob(ctx, p.Actor(), q.Name, store.NewFetchJob{QueueID: q.ID,
		ClientID: p.ID, IdempotencyKey: key, Params: fr.JobParams, URL: fr.URL, Site: site})
	if err != nil {
		return err
	}
	w.Header().Set("Location", s.publicURL("/job/"+job.ID))
	if !created {
		writeJSON(w, http.StatusOK, job.API(s.now()))
		return nil
	}
	// No source field: it defaults to the URL.
	obs.Info(&s.log, obs.EventJobCreated).Str(obs.FieldJobID, job.ID).Str(obs.FieldQueue, q.Name).
		Str(obs.FieldWeight, string(job.Weight)).Str(obs.FieldModel, job.Model).Str(obs.FieldSite, site).
		Str(obs.FieldClientID, p.Name).Msg("job created")
	obs.Info(&s.log, obs.EventFetchSubmitted).Str(obs.FieldJobID, job.ID).Str(obs.FieldQueue, q.Name).
		Str(obs.FieldSite, site).Msg("URL job submitted")
	s.metrics.JobCreated(q.Name, job.Weight, false)
	s.disp.Notify()
	s.reschedulePacing()
	writeJSON(w, http.StatusCreated, job.API(s.now()))
	return nil
}

// validFetchURL checks a submitted URL (no network access: the downloader's
// egress filter alone judges addresses) and returns its site key. Error
// details never quote the URL.
func validFetchURL(u string) (string, error) {
	if u == "" {
		return "", badRequest("url is required")
	}
	if len(u) > api.MaxSourceLen {
		return "", badRequest("url must be at most " + itoa(api.MaxSourceLen) + " bytes")
	}
	site, err := fetchpolicy.SiteKey(u)
	if err != nil {
		msg := err.Error()
		if strings.HasPrefix(msg, "no registrable domain") {
			msg = "url host has no registrable domain"
		}
		return "", badRequest(msg)
	}
	return site, nil
}

// CreateFetchJob creates a URL job for a on the named queue, audited as
// job.create. The detail records the queue, model, weight and site key,
// never the URL (or the source, which defaults to it), prompt or metadata.
// A replayed idempotency key returns the existing job with created = false
// and is not audited.
func (o *Ops) CreateFetchJob(ctx context.Context, a Actor, queue string, nj store.NewFetchJob) (job *store.Job, created bool, err error) {
	_, err = o.commit(ctx, a, func(tx *store.Tx, rec recorder) error {
		var err error
		if job, created, err = tx.CreateFetchJob(ctx, nj); err != nil || !created {
			return err
		}
		return rec("job.create", "job:"+job.ID, map[string]any{"queue": queue, "model": job.Model,
			"weight": job.Weight, "site": nj.Site})
	})
	if err != nil {
		return nil, false, err
	}
	return job, created, nil
}

// --- claims: egress keys and waiting groups ---

// egressKeyFor is a downloader's effective egress key (its pacing group):
// label:<group> if it reports an egress group; else ip:<addr> for the
// public address its egress probe found; else ip:<remote> if the address
// the server sees it at is public; else "unknown", one shared conservative
// group. "Public" is what a downloader may dial (egress.Refused).
func egressKeyFor(f api.WorkerFacts, remote string) string {
	if f.EgressGroup != "" {
		return "label:" + f.EgressGroup
	}
	for _, ip := range []string{f.EgressIP, remote} {
		if a, err := netip.ParseAddr(ip); err == nil && !egress.Refused(a) {
			return "ip:" + a.WithZone("").Unmap().String()
		}
	}
	return "unknown"
}

// fetchWaiters counts the egress keys of downloaders whose fetch claim is
// long-polling now (a multiset), for the store's rotation between groups.
type fetchWaiters struct {
	mu sync.Mutex
	n  map[string]int
}

// add registers one waiting claim from key. The returned remove func is
// idempotent.
func (f *fetchWaiters) add(key string) (remove func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.n == nil {
		f.n = map[string]int{}
	}
	f.n[key]++
	return sync.OnceFunc(func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.n[key]--; f.n[key] <= 0 {
			delete(f.n, key)
		}
	})
}

// keys returns the distinct waiting keys, sorted.
func (f *fetchWaiters) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.n))
	for k := range f.n {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// downloaderEligibility completes a downloader's claim eligibility: its
// effective egress key, stored on the worker first (so the store's rotation
// and NextFetchEligible, which read the column, agree with the claim).
func (s *Server) downloaderEligibility(r *http.Request, wk *store.Worker, el *store.Eligibility) error {
	key := egressKeyFor(wk.Facts, s.clientIP(r))
	if key != wk.EgressKey {
		if err := s.st.SetWorkerEgressKey(r.Context(), wk.ID, key); err != nil {
			return err
		}
		wk.EgressKey = key
	}
	el.EgressKey = key
	return nil
}

// --- the pacing wake-up ---

// pacingTimer is the one resettable timer that wakes long-polling claims
// when fetch pacing stops holding a pending fetch task back (a rate window
// frees up, a cooldown ends). Concurrency limits need no timer: the lease
// end that frees a slot notifies by itself.
type pacingTimer struct {
	mu     sync.Mutex
	stop   func() bool // nil when no timer is pending
	due    time.Time
	closed bool
	// gen identifies the pending timer, so a timer that fired after it
	// was replaced does not clear its successor.
	gen uint64
	// invalid is the last set of invalid fetch policy keys logged.
	invalid string
}

// FetchPacingChanged recomputes the pacing wake-up and wakes waiting
// claims. Call it after fetch policies or cooldowns change.
func (s *Server) FetchPacingChanged() {
	s.reschedulePacing()
	s.disp.Notify()
}

// reschedulePacing points the pacing timer at the next time pacing frees a
// pending fetch task (store.NextFetchEligible). It only ever brings the
// timer forward or replaces one that has fired: firing early costs a
// spurious wake-up and a recompute, which is cheaper than coordinating
// concurrent recomputes. It also checks the fetch policies.
func (s *Server) reschedulePacing() {
	ctx := s.bg
	s.checkFetchPolicies(ctx)
	now := s.now()
	next, ok, err := s.st.NextFetchEligible(ctx, now)
	if err != nil {
		if ctx.Err() == nil {
			s.log.Warn().Err(err).Msg("computing the next fetch wake-up failed")
		}
		return
	}
	if !ok {
		return
	}
	t := &s.pacing
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || t.stop != nil && !next.Before(t.due) {
		return
	}
	if t.stop != nil {
		t.stop()
	}
	t.due = next
	t.gen++
	gen := t.gen
	t.stop = s.afterFunc(max(next.Sub(now), time.Millisecond), func() { s.pacingFired(gen) })
}

// pacingFired runs when timer gen goes off: wake waiting claims and
// schedule the next wake-up. A timer replaced before it could be stopped
// still wakes claims, but leaves the pending timer alone.
func (s *Server) pacingFired(gen uint64) {
	t := &s.pacing
	t.mu.Lock()
	closed := t.closed
	if t.gen == gen {
		t.stop, t.due = nil, time.Time{}
	}
	t.mu.Unlock()
	if closed {
		return
	}
	s.disp.Notify()
	s.reschedulePacing()
}

// closePacing stops the pacing timer for good.
func (s *Server) closePacing() {
	t := &s.pacing
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	if t.stop != nil {
		t.stop()
		t.stop = nil
	}
}

// checkFetchPolicies meters the stored fetch policies that do not parse or
// validate (each blocks what it governs) and logs their keys once per
// change.
func (s *Server) checkFetchPolicies(ctx context.Context) {
	bad, err := s.st.InvalidFetchPolicies(ctx)
	if err != nil {
		if ctx.Err() == nil {
			s.log.Warn().Err(err).Msg("checking fetch policies failed")
		}
		return
	}
	s.metrics.SetFetchInvalidPolicies(len(bad))
	joined := strings.Join(bad, "\x00")
	t := &s.pacing
	t.mu.Lock()
	changed := joined != t.invalid
	t.invalid = joined
	t.mu.Unlock()
	if changed && len(bad) > 0 {
		s.log.Warn().Strs(obs.FieldPolicyKeys, bad).
			Msg("invalid fetch policies block the sites they govern; fix them on the fetch policies page")
	}
}

// --- results and failures ---

// fetchSiteLabels returns the metric label of a site: the site itself when
// a fetch policy names it (so the label set is bounded by the policy table),
// else "other". Sites are never URLs.
func (s *Server) fetchSiteLabels(ctx context.Context) func(site string) string {
	known := map[string]bool{}
	if ps, err := s.st.FetchPolicies(ctx); err == nil {
		for _, p := range ps {
			known[p.Site] = true
		}
	}
	if bad, err := s.st.InvalidFetchPolicies(ctx); err == nil {
		for _, k := range bad {
			site, _, _ := strings.Cut(k, "@")
			known[site] = true
		}
	}
	return func(site string) string {
		if site != "*" && known[site] {
			return site
		}
		return "other"
	}
}

func (s *Server) siteLabel(ctx context.Context, site string) string {
	return s.fetchSiteLabels(ctx)(site)
}

// notMedia reports whether the leading bytes of a fetch result are a web
// page or an API answer (HTML, XML, JSON, plain text) rather than media,
// and what they look like. It mirrors the downloader's own guard.
func notMedia(head []byte) (string, bool) {
	if knownMedia(head) {
		return "media", false
	}
	ct := media.Sniff(head)
	mt, _, _ := strings.Cut(ct, ";")
	mt = strings.TrimSpace(mt)
	switch {
	case strings.HasPrefix(mt, "text/"), mt == "application/json", mt == "application/xml",
		mt == "application/xhtml+xml":
		return mt, true
	}
	if t := bytes.TrimLeft(bytes.TrimPrefix(head, []byte("\xef\xbb\xbf")), " \t\r\n"); len(t) > 0 && (t[0] == '<' || t[0] == '{') {
		return "markup or JSON", true
	}
	return mt, false
}

// knownMedia reports whether head starts with the signature of a common
// audio or video container, which is media whatever a text sniffer makes of
// it (MPEG audio frames, say, can look like text).
func knownMedia(head []byte) bool {
	switch {
	case len(head) >= 12 && string(head[:4]) == "RIFF" && string(head[8:12]) == "WAVE",
		bytes.HasPrefix(head, []byte("fLaC")),
		bytes.HasPrefix(head, []byte("OggS")),
		bytes.HasPrefix(head, []byte("ID3")),
		bytes.HasPrefix(head, []byte{0x1A, 0x45, 0xDF, 0xA3}),     // EBML: Matroska, WebM
		len(head) >= 8 && string(head[4:8]) == "ftyp",             // ISO BMFF: MP4, M4A
		len(head) >= 2 && head[0] == 0xFF && head[1]&0xE0 == 0xE0: // MPEG audio frame sync, ADTS
		return true
	}
	return false
}

// fetchResult accepts a fetch lease's result: the fetched media, streamed
// (chunked, length unknown) and capped at the lease's byte cap. It is
// received to a private part file outside the job lock, then refused if it
// is a page rather than media; then, under the job lock (which a retry's
// blob removal also holds), published as the original, put through the
// upload fast path, and recorded with AcceptFetch. Anything that fails
// before that commit leaves no original behind. Too large, not media, or
// media no conversion can fix fail the lease non-retryably (the input's
// fault); a body that did not arrive leaves the lease live for the worker
// to report or retry.
func (s *Server) fetchResult(w http.ResponseWriter, r *http.Request, p *Principal, l *store.Lease) error {
	ctx := r.Context()
	method := r.Header.Get(api.HeaderFetchMethod)
	if method != api.FetchMethodYtDlp && method != api.FetchMethodHTTP {
		method = ""
	}
	limit := s.fetchLimit(l)
	s.streamBody(r, limit)

	if !l.Live { // the lease already completed: an idempotent retry
		h := sha256.New()
		if _, err := io.Copy(h, io.LimitReader(r.Body, limit+1)); err != nil {
			return badRequest("reading media: " + err.Error())
		}
		if err := s.st.AcceptFetch(ctx, l.ID, p.ID, store.FetchCompletion{
			Original: store.Media{SHA256: hex.EncodeToString(h.Sum(nil))}}); err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, resultAck{Lease: l.ID, Outcome: api.OutcomeDone})
		return nil
	}

	res, err := s.reserveDisk(0)
	if err != nil {
		return err
	}
	defer res.release()
	if err := s.couldFit(r.ContentLength); err != nil {
		return err
	}
	out, orig, head, err := s.receiveMedia(l.JobID, r.Body, limit, res)
	if err != nil {
		var pr *problem
		if errors.As(err, &pr) && pr.status == http.StatusRequestEntityTooLarge {
			return s.rejectFetch(ctx, l, pr.status, api.ProblemTooLarge, pr.detail, "too_large")
		}
		return err
	}
	defer out.Abort()
	if what, bad := notMedia(head); bad {
		return s.rejectFetch(ctx, l, http.StatusUnprocessableEntity, api.ProblemNotMedia,
			"the fetched content is "+what+", not media", "not_media")
	}

	unlock := s.locks.Lock(l.JobID)
	defer unlock()
	job, err := s.st.Job(ctx, l.JobID)
	if errors.Is(err, store.ErrNotFound) {
		return leaseGone()
	}
	if err != nil {
		return err
	}
	if job.State != api.JobFetching {
		// Accepted already (a concurrent PUT of this lease), or the job
		// moved on. Never touch its files; let the store decide.
		if err := s.st.AcceptFetch(ctx, l.ID, p.ID, store.FetchCompletion{Original: orig}); err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, resultAck{Lease: l.ID, Outcome: api.OutcomeDone})
		return nil
	}
	// The job is fetching and we hold its lock, so no accepted media is in
	// its directory: whatever we publish below is ours to remove.
	committed := false
	defer func() {
		if !committed {
			_ = s.blobs.Remove(l.JobID, blobs.Original, blobs.Canonical, blobs.CanonicalIndex)
		}
	}()
	if err := out.Commit(); err != nil {
		return err
	}
	canon, err := s.canonicalizeUpload(ctx, p.Grant, l.JobID, orig.Type)
	if err != nil {
		var pr *problem
		if errors.As(err, &pr) && pr.typ == api.ProblemMediaInvalid {
			return s.rejectFetch(ctx, l, pr.status, pr.typ, pr.detail, "invalid")
		}
		return err
	}
	fc := store.FetchCompletion{Original: orig}
	if canon != nil {
		fc.Canonical, fc.Ranges = &canon.Canonical, canon.Ranges
	}
	if err := s.st.AcceptFetch(ctx, l.ID, p.ID, fc); err != nil {
		return err
	}
	committed = true
	if canon != nil {
		if err := s.purgeOriginal(ctx, l.JobID); err != nil {
			s.log.Warn().Err(err).Str(obs.FieldJobID, l.JobID).Msg("purging original failed; retention will retry")
		}
	}
	dur := s.now().Sub(l.CreatedAt)
	obs.Info(&s.log, obs.EventTaskDone).Str(obs.FieldLeaseID, l.ID).Str(obs.FieldJobID, l.JobID).
		Int(obs.FieldTaskSeq, l.Seq).Str(obs.FieldTaskKind, string(l.Kind)).Str(obs.FieldWorkerID, p.ID).
		Msg("fetch done")
	obs.Info(&s.log, obs.EventFetchDone).Str(obs.FieldLeaseID, l.ID).Str(obs.FieldJobID, l.JobID).
		Str(obs.FieldWorkerID, p.ID).Str(obs.FieldSite, l.Site).Str(obs.FieldEgressKey, l.EgressKey).
		Str(obs.FieldFetchMethod, method).Int64(obs.FieldBytes, orig.Bytes).
		Str(obs.FieldOriginalType, orig.Type).Bool(obs.FieldFastPath, canon != nil).
		Int64(obs.FieldDurationMS, dur.Milliseconds()).Msg("fetch done")
	if canon != nil {
		s.logPrepared(l.JobID, canon)
	}
	label := s.siteLabel(ctx, l.Site)
	s.metrics.LeaseEnded(api.OutcomeDone, api.KindFetch)
	s.metrics.FetchResult(label, "done")
	s.metrics.FetchBytes(label, orig.Bytes)
	s.disp.Notify()
	s.reschedulePacing()
	writeJSON(w, http.StatusOK, resultAck{Lease: l.ID, Outcome: api.OutcomeDone})
	return nil
}

// fetchLimit is a fetch lease's byte cap: the store's (the queue's upload
// cap, bounded by store.Config.FetchMaxBytes), never above the server's
// --max-upload-bytes.
func (s *Server) fetchLimit(l *store.Lease) int64 {
	if l.MaxBytes > 0 {
		return min(l.MaxBytes, s.cfg.MaxUploadBytes)
	}
	return s.cfg.MaxUploadBytes
}

// rejectFetch refuses a fetch result that is the input's fault (too large,
// not media, unusable media): the lease fails non-retryably, failing the
// job, if it is still live. outcome is the whisper_fetch_total outcome.
func (s *Server) rejectFetch(ctx context.Context, l *store.Lease, status int, typ, reason, outcome string) error {
	if l.Live {
		why := map[string]string{"not_media": fetchReasonNotMedia, "too_large": fetchReasonTooLarge}[outcome]
		if why == "" {
			why = fetchReasonBadMedia
		}
		end, err := s.st.Fail(ctx, l.ID, l.WorkerID, "invalid result: "+why, false)
		if err != nil {
			return err
		}
		obs.Warn(&s.log, obs.EventTaskFailed).Str(obs.FieldLeaseID, l.ID).Str(obs.FieldJobID, l.JobID).
			Int(obs.FieldTaskSeq, l.Seq).Str(obs.FieldTaskKind, string(l.Kind)).Str(obs.FieldWorkerID, l.WorkerID).
			Int(obs.FieldAttempt, end.Attempt).Str(obs.FieldSite, l.Site).
			Str(obs.FieldError, "invalid result: "+why).Msg("result rejected")
		s.metrics.FetchResult(s.siteLabel(ctx, l.Site), outcome)
		s.leaseEnded(ctx, end)
	}
	return newProblem(status, typ, reason)
}

// Classified failure reasons of fetch leases: the only failure text a
// fetch lease's job error and logs ever carry (see handleFailure).
const (
	fetchReasonRateLimited = "rate limited"
	fetchReasonPreWork     = "pre-work failure"
	fetchReasonPermanent   = "permanent failure"
	fetchReasonTransient   = "transient failure"
	fetchReasonNotMedia    = "not media"
	fetchReasonTooLarge    = "too large"
	fetchReasonBadMedia    = "unusable media"
)

// fetchFailureReason classifies a downloader's failure report by its stage
// and retryability alone.
func fetchFailureReason(stage api.FailureStage, retryable bool) string {
	switch {
	case stage == api.StageRateLimited:
		return fetchReasonRateLimited
	case stage == api.StagePreWork:
		return fetchReasonPreWork
	case !retryable:
		return fetchReasonPermanent
	}
	return fetchReasonTransient
}

// jobSiteLabel is the site label (fetchSiteLabels) of a job's fetch site;
// "other" if the job is gone.
func (s *Server) jobSiteLabel(ctx context.Context, jobID string) string {
	site := ""
	if j, err := s.st.Job(ctx, jobID); err == nil {
		site = j.FetchSite
	}
	return s.siteLabel(ctx, site)
}

// fetchFailed does the fetch-specific bookkeeping of a reported failure of
// fetch lease l: the rate_limited and cooldown events and the fetch
// metrics.
func (s *Server) fetchFailed(ctx context.Context, l *store.Lease, stage api.FailureStage) {
	outcome := "failed"
	if stage == api.StageRateLimited {
		outcome = "rate_limited"
		obs.Warn(&s.log, obs.EventFetchRateLimited).Str(obs.FieldLeaseID, l.ID).Str(obs.FieldJobID, l.JobID).
			Str(obs.FieldWorkerID, l.WorkerID).Str(obs.FieldSite, l.Site).Str(obs.FieldEgressKey, l.EgressKey).
			Msg("fetch rate limited")
		cds, err := s.st.FetchCooldowns(ctx, s.now())
		if err != nil {
			s.log.Warn().Err(err).Msg("loading fetch cooldowns failed")
		}
		for _, c := range cds {
			if c.EgressKey == l.EgressKey && c.Site == l.Site {
				obs.Warn(&s.log, obs.EventFetchCooldown).Str(obs.FieldSite, c.Site).
					Str(obs.FieldEgressKey, c.EgressKey).Time(obs.FieldUntil, c.Until).Int(obs.FieldCooldownLevel, c.Level).
					Bool(obs.FieldManual, c.Manual).Msg("fetch cooldown")
			}
		}
	}
	s.metrics.FetchResult(s.siteLabel(ctx, l.Site), outcome)
}

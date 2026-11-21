package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/auth"
	"github.com/deweysasser/whisper-network/internal/blobs"
	"github.com/deweysasser/whisper-network/internal/media"
	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/store"
)

const (
	// maxParamsBytes bounds the params part (prompt, metadata and labels
	// are bounded far below this).
	maxParamsBytes = 64 << 10
	// multipartSlack allows for multipart framing and the params part on
	// top of the media size cap.
	multipartSlack = 1 << 20
	// maxIdempotencyKey bounds the Idempotency-Key header.
	maxIdempotencyKey = 200
	// sniffLen is how many leading bytes the type sniffer sees.
	sniffLen = 512
)

var languageRE = regexp.MustCompile(`^(auto|[a-z]{2,3})$`)

// handleUpload is POST /queue/{q}: create a job from a multipart upload
// (DESIGN.md §6.1). The media part is streamed to disk while it is hashed
// and size-capped; canonical WAV/FLAC is then validated, encoded and split
// before the response, so a 201 means the transcribe tasks already exist.
// That analysis is a full decode (seconds per audio-hour) and is done
// synchronously for simplicity: there is no intermediate "uploaded but not
// analyzed" state to recover after a crash.
func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request, p *Principal) error {
	ctx := r.Context()
	q, err := s.st.QueueByName(ctx, r.PathValue("q"))
	if err != nil {
		return err
	}
	if !auth.Can(p.Principal, auth.ActSubmitJob, auth.Resource{Queue: q.Name}) {
		return s.deny(r, p, "submit not granted on queue "+q.Name)
	}
	key := r.Header.Get("Idempotency-Key")
	if len(key) > maxIdempotencyKey {
		return badRequest("Idempotency-Key is too long")
	}
	if key != "" {
		j, err := s.st.JobByIdempotencyKey(ctx, p.ID, key)
		if err == nil {
			// A retried upload: answer before reading the body.
			w.Header().Set("Location", s.publicURL("/job/"+j.ID))
			writeJSON(w, http.StatusOK, j.API(s.now()))
			return nil
		}
		if !errors.Is(err, store.ErrNotFound) {
			return err
		}
	}
	done, err := s.uploading.begin(p.Grant, s.cfg.UploadsPerPrincipal)
	if err != nil {
		return err
	}
	defer done()
	limit := s.uploadLimit(q)
	// Nothing is reserved before bytes arrive: the writer reserves space a
	// small step ahead of the media it writes, so concurrent uploads cannot
	// together run the disk below MinFreeBytes, yet a slow upload holds no
	// more than it has been sent plus one step. A declared length only
	// refuses, early, a body that could never fit.
	res, err := s.reserveDisk(0)
	if err != nil {
		return err
	}
	defer res.release()
	if err := s.couldFit(r.ContentLength); err != nil {
		return err
	}

	s.streamBody(r, limit+multipartSlack)
	r.Body = http.MaxBytesReader(w, r.Body, limit+multipartSlack)
	mr, err := r.MultipartReader()
	if err != nil {
		return badRequest("body must be multipart/form-data with parts params and media")
	}
	params, err := readParams(mr)
	if err != nil {
		return err
	}
	if err := s.validateParams(&params); err != nil {
		return err
	}
	part, err := mr.NextPart()
	if errors.Is(err, errTooSlow) {
		return tooSlow()
	}
	if err != nil || part.FormName() != "media" {
		return badRequest("second part must be media")
	}

	jobID := s.st.NewJobID()
	// Registered before the directory exists and released after the row
	// is committed (or the directory removed): the orphan sweep skips it.
	defer s.uploads.add(jobID)()
	keep := false
	defer func() {
		if !keep {
			_ = s.blobs.RemoveJob(jobID)
		}
	}()
	orig, err := s.receiveOriginal(jobID, part, limit, res)
	if err != nil {
		return err
	}
	s.metrics.Upload(q.Name, orig.Bytes)

	canon, err := s.canonicalizeUpload(ctx, p.Grant, jobID, orig.Type)
	if err != nil {
		return err
	}
	nj := store.NewJob{ID: jobID, QueueID: q.ID, ClientID: p.ID, IdempotencyKey: key, Params: params, Original: orig}
	if canon != nil {
		nj.Canonical, nj.Ranges = &canon.Canonical, canon.Ranges
	}
	job, created, err := s.ops.CreateJob(ctx, p.Actor(), q.Name, nj)
	if err != nil {
		return err
	}
	if !created {
		// A concurrent retry with the same key won; ours is discarded.
		w.Header().Set("Location", s.publicURL("/job/"+job.ID))
		writeJSON(w, http.StatusOK, job.API(s.now()))
		return nil
	}
	keep = true
	if canon != nil {
		if err := s.purgeOriginal(ctx, jobID); err != nil {
			return err
		}
		if job, err = s.st.Job(ctx, jobID); err != nil {
			return err
		}
	}
	obs.Info(&s.log, obs.EventJobCreated).Str(obs.FieldJobID, job.ID).Str(obs.FieldQueue, q.Name).
		Str(obs.FieldWeight, string(job.Weight)).Str(obs.FieldModel, job.Model).Int64(obs.FieldBytes, orig.Bytes).
		Str(obs.FieldOriginalType, orig.Type).Bool(obs.FieldFastPath, canon != nil).
		Str(obs.FieldSource, job.Source).Str(obs.FieldClientID, p.Name).Msg("job created")
	if canon != nil {
		s.logPrepared(job.ID, canon)
	}
	s.metrics.JobCreated(q.Name, job.Weight, canon != nil)
	s.disp.Notify()

	w.Header().Set("Location", s.publicURL("/job/"+job.ID))
	writeJSON(w, http.StatusCreated, job.API(s.now()))
	return nil
}

func (s *Server) logPrepared(jobID string, c *canonical) {
	obs.Info(&s.log, obs.EventJobPrepared).Str(obs.FieldJobID, jobID).
		Float64(obs.FieldAudioSeconds, float64(c.Samples)/media.SampleRate).Int(obs.FieldTasks, len(c.Ranges)).
		Int64(obs.FieldCanonicalBytes, c.Bytes).Msg("canonical audio accepted")
}

// readParams reads the first multipart part, which must be "params". A
// body that fell below MinTransferRate meanwhile is a 408, not a malformed
// request.
func readParams(mr *multipart.Reader) (api.JobParams, error) {
	var params api.JobParams
	part, err := mr.NextPart()
	if errors.Is(err, errTooSlow) {
		return params, tooSlow()
	}
	if err != nil || part.FormName() != "params" {
		return params, badRequest("first part must be params (JSON)")
	}
	dec := json.NewDecoder(io.LimitReader(part, maxParamsBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&params); err != nil {
		if errors.Is(err, errTooSlow) {
			return params, tooSlow()
		}
		return params, badRequest("invalid params: " + err.Error())
	}
	return params, nil
}

// validateParams checks and normalizes job parameters (DESIGN.md §6.1).
func (s *Server) validateParams(p *api.JobParams) error {
	if _, ok := s.allowed[p.Model]; !ok {
		return badRequest("model " + p.Model + " is not on this server's allowlist")
	}
	if p.Language == "" {
		p.Language = "en"
	}
	if !languageRE.MatchString(p.Language) {
		return badRequest("language must be a 2-3 letter code or auto")
	}
	if p.Weight == "" {
		p.Weight = api.WeightMedium
	}
	if !p.Weight.Valid() {
		return badRequest("weight must be low, medium or high")
	}
	for _, f := range []struct {
		name, v string
		max     int
	}{{"prompt", p.Prompt, api.MaxPromptLen}, {"label", p.Label, api.MaxLabelLen}, {"source", p.Source, api.MaxSourceLen}} {
		if !utf8.ValidString(f.v) || utf8.RuneCountInString(f.v) > f.max {
			return badRequest(f.name + " must be valid UTF-8 of at most " + itoa(f.max) + " characters")
		}
	}
	if strings.ContainsAny(p.Label+p.Source, "\r\n") {
		return badRequest("label and source must be single lines")
	}
	if m := strings.TrimSpace(string(p.Metadata)); m == "null" || m == "" {
		p.Metadata = nil
	} else if len(p.Metadata) > api.MaxMetadataBytes {
		return badRequest("metadata exceeds " + itoa(api.MaxMetadataBytes) + " bytes")
	}
	return nil
}

// uploadLimit is the media size cap on q: the queue's override if it has
// one, but never above the server's --max-upload-bytes, which bounds what a
// queue administrator can make the server accept (and the disk reserve).
func (s *Server) uploadLimit(q *store.Queue) int64 {
	if q.MaxUploadBytes > 0 {
		return min(q.MaxUploadBytes, s.cfg.MaxUploadBytes)
	}
	return s.cfg.MaxUploadBytes
}

// receiveOriginal streams the media part to blobs/{job}/original, hashing
// it and enforcing the size cap as it goes, and sniffs its type from the
// leading bytes (the client's Content-Type is never trusted). Writes draw
// on res and stop with 507 if free space falls below MinFreeBytes, and a
// body slower than MinTransferRate stops with 408.
func (s *Server) receiveOriginal(jobID string, src io.Reader, limit int64, res *reservation) (store.Media, error) {
	out, m, _, err := s.receiveMedia(jobID, src, limit, res)
	if err != nil {
		return store.Media{}, err
	}
	if err := out.Commit(); err != nil {
		return store.Media{}, err
	}
	return m, nil
}

// receiveMedia is receiveOriginal without the commit: on success the
// caller owns the pending original (and must Commit or Abort it), and gets
// the leading bytes the type was sniffed from as well. On error nothing is
// left behind.
func (s *Server) receiveMedia(jobID string, src io.Reader, limit int64, res *reservation) (*blobs.Pending, store.Media, []byte, error) {
	out, err := s.blobs.Create(jobID, blobs.Original)
	if err != nil {
		return nil, store.Media{}, nil, err
	}
	fail := func(err error) (*blobs.Pending, store.Media, []byte, error) {
		out.Abort()
		return nil, store.Media{}, nil, err
	}
	h := sha256.New()
	head := &headBuffer{max: sniffLen}
	disk := &guardedWriter{w: out, res: res}
	n, err := io.Copy(io.MultiWriter(disk, h, head), io.LimitReader(src, limit+1))
	var mbe *http.MaxBytesError
	switch {
	case errors.Is(err, errDiskLow):
		return fail(storageFull("free space fell below the minimum during the upload"))
	case errors.Is(err, errTooSlow):
		return fail(tooSlow())
	case errors.As(err, &mbe) || n > limit:
		return fail(newProblem(http.StatusRequestEntityTooLarge, api.ProblemTooLarge,
			"media exceeds the "+itoa64(limit)+" byte limit"))
	case err != nil:
		return fail(badRequest("reading media: " + err.Error()))
	case n == 0:
		return fail(badRequest("media part is empty"))
	}
	return out, store.Media{Bytes: n, SHA256: hex.EncodeToString(h.Sum(nil)), Type: media.Sniff(head.b)}, head.b, nil
}

// headBuffer keeps the first max bytes written to it.
type headBuffer struct {
	b   []byte
	max int
}

func (h *headBuffer) Write(p []byte) (int, error) {
	if room := h.max - len(h.b); room > 0 {
		h.b = append(h.b, p[:min(room, len(p))]...)
	}
	return len(p), nil
}

// tooSlow is the 408 problem for a streaming body that fell below
// MinTransferRate.
func tooSlow() *problem {
	return newProblem(http.StatusRequestTimeout, api.ProblemBadRequest,
		"the upload fell below the minimum transfer rate")
}

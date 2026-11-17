package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"strconv"
	"unicode/utf8"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/blobs"
	"github.com/deweysasser/whisper-network/internal/media"
	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/store"
)

// Bounds on transcribe results (DESIGN.md §6.2: the server validates).
const (
	maxResultBody     = 16 << 20
	maxSegmentText    = 4 << 10
	endToleranceMS    = 1000 // whisper rounds the last segment's end up
	minSegmentSpanMS  = 200  // segments-per-duration bound: one per 200 ms ...
	segmentCountSlack = 1000 // ... plus this many
)

// liveLease loads the caller's lease. Unknown or foreign leases are 404;
// ended or expired ones 410, unless allowDone and it ended successfully
// (an idempotent result retry).
func (s *Server) liveLease(r *http.Request, p *Principal, allowDone bool) (*store.Lease, error) {
	l, err := s.st.Lease(r.Context(), r.PathValue("id"), p.ID)
	if err != nil {
		return nil, err
	}
	if !l.Live && !(allowDone && l.Outcome == api.OutcomeDone) {
		return nil, leaseGone()
	}
	return l, nil
}

// countingWriter counts bytes written through it.
type countingWriter struct {
	http.ResponseWriter
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.ResponseWriter.Write(p)
	c.n += int64(n)
	return n, err
}

// ReadFrom preserves the underlying writer's sendfile path.
func (c *countingWriter) ReadFrom(r io.Reader) (int64, error) {
	n, err := io.Copy(c.ResponseWriter, r)
	c.n += n
	return n, err
}

// handleInput serves a lease's input: the original upload for prepare
// (with Range support for resumed downloads), or the task's sample range
// of the canonical audio as a WAV stream for transcribe.
func (s *Server) handleInput(w http.ResponseWriter, r *http.Request, p *Principal) error {
	l, err := s.liveLease(r, p, false)
	if err != nil {
		return err
	}
	cw := &countingWriter{ResponseWriter: w}
	defer func() { s.metrics.InputServed(l.Kind, cw.n) }()
	if l.Kind == api.KindPrepare {
		f, err := s.blobs.Open(l.JobID, blobs.Original)
		if errors.Is(err, fs.ErrNotExist) {
			return newProblem(http.StatusGone, api.ProblemLeaseGone, "input no longer available")
		}
		if err != nil {
			return err
		}
		defer f.Close()
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("ETag", strconv.Quote(l.OriginalSHA256))
		if fi, err := f.Stat(); err == nil {
			s.extendWrite(w, fi.Size())
		}
		http.ServeContent(cw, r, "", l.CreatedAt, f)
		return nil
	}

	f, err := s.blobs.Open(l.JobID, blobs.Canonical)
	if errors.Is(err, fs.ErrNotExist) {
		return newProblem(http.StatusGone, api.ProblemLeaseGone, "input no longer available")
	}
	if err != nil {
		return err
	}
	defer f.Close()
	idx := s.loadIndex(l.JobID)
	rng := media.Range{Start: l.StartSample, End: l.EndSample}
	size := 44 + rng.Len()*media.BytesPerSample
	w.Header().Set("Content-Type", "audio/wav")
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	s.extendWrite(w, size)
	if err := media.ServeRange(cw, f, idx, rng); err != nil {
		if cw.n == 0 {
			w.Header().Del("Content-Length")
			return err
		}
		s.log.Error().Err(err).Str(obs.FieldLeaseID, l.ID).Str(obs.FieldJobID, l.JobID).Msg("serving input failed midway")
		panic(http.ErrAbortHandler) // truncate the response so the worker sees an error
	}
	return nil
}

// loadIndex reads a job's seek index. Without one (unreadable or corrupt)
// ServeRange still works by decoding from the start, just slower.
func (s *Server) loadIndex(jobID string) *media.Index {
	b, err := s.blobs.ReadFile(jobID, blobs.CanonicalIndex)
	if err == nil {
		idx, derr := blobs.DecodeIndex(b)
		if derr == nil {
			return idx
		}
		err = derr
	}
	s.log.Warn().Err(err).Str(obs.FieldJobID, jobID).Msg("seek index unavailable; decoding from the start")
	return nil
}

// handleLeaseResult is PUT /lease/{id}/result.
func (s *Server) handleLeaseResult(w http.ResponseWriter, r *http.Request, p *Principal) error {
	l, err := s.liveLease(r, p, true)
	if err != nil {
		return err
	}
	if l.Kind == api.KindPrepare {
		return s.prepareResult(w, r, p, l)
	}
	return s.transcribeResult(w, r, p, l)
}

type resultAck struct {
	Lease   string           `json:"lease"`
	Outcome api.LeaseOutcome `json:"outcome"`
}

// rejectResult refuses an invalid result. If the lease is still live this
// counts as a failed attempt (the task requeues or the job fails).
func (s *Server) rejectResult(ctx context.Context, l *store.Lease, status int, reason string) error {
	if l.Live {
		end, err := s.st.Fail(ctx, l.ID, l.WorkerID, "invalid result: "+reason, true)
		if err != nil {
			return err
		}
		obs.Warn(&s.log, obs.EventTaskFailed).Str(obs.FieldLeaseID, l.ID).Str(obs.FieldJobID, l.JobID).
			Int(obs.FieldTaskSeq, l.Seq).Str(obs.FieldTaskKind, string(l.Kind)).Str(obs.FieldWorkerID, l.WorkerID).
			Int(obs.FieldAttempt, end.Attempt).Str(obs.FieldError, "invalid result: "+reason).Msg("result rejected")
		s.leaseEnded(ctx, end)
	}
	typ := api.ProblemResultInvalid
	if status == http.StatusRequestEntityTooLarge {
		typ = api.ProblemTooLarge
	}
	return newProblem(status, typ, reason)
}

// rejectLoop refuses a transcript stuck repeating one line (issue #99).
// The task is retried on another worker with no_context and no prompt;
// the worker's breaker is not charged.
func (s *Server) rejectLoop(ctx context.Context, l *store.Lease, loop *errLoop) error {
	reason := loop.Error()
	if l.Live {
		end, err := s.st.FailStage(ctx, l.ID, l.WorkerID, reason, true, api.StageLoop)
		if err != nil {
			return err
		}
		obs.Warn(&s.log, obs.EventTaskFailed).Str(obs.FieldLeaseID, l.ID).Str(obs.FieldJobID, l.JobID).
			Int(obs.FieldTaskSeq, l.Seq).Str(obs.FieldTaskKind, string(l.Kind)).Str(obs.FieldWorkerID, l.WorkerID).
			Int(obs.FieldAttempt, end.Attempt).Str("stage", string(api.StageLoop)).Bool("no_context", l.NoContext).
			Str(obs.FieldError, reason).Msg("result rejected: repetition loop")
		s.leaseEnded(ctx, end)
	}
	return newProblem(http.StatusUnprocessableEntity, api.ProblemResultInvalid, reason)
}

// maxCanonicalBytes bounds a prepare result: verbatim FLAC of the longest
// accepted audio plus generous frame overhead.
func (s *Server) maxCanonicalBytes() int64 {
	pcm := s.maxSamples() * media.BytesPerSample
	return pcm + pcm/8 + 1<<20
}

// prepareResult accepts canonical FLAC from a prepare worker: stream it to
// a private part file, validate and analyze it, then — serialized per job —
// publish it and record it with AcceptPrepare, which creates the transcribe
// tasks.
func (s *Server) prepareResult(w http.ResponseWriter, r *http.Request, p *Principal, l *store.Lease) error {
	ctx := r.Context()
	var computeMS int64
	if v := r.Header.Get(api.HeaderComputeMS); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			return badRequest(api.HeaderComputeMS + " must be a non-negative integer")
		}
		computeMS = n
	}
	ffmpeg := r.Header.Get(api.HeaderFFmpegVersion)
	if len(ffmpeg) > 200 || !utf8.ValidString(ffmpeg) {
		return badRequest(api.HeaderFFmpegVersion + " is too long")
	}
	s.streamBody(r, s.maxCanonicalBytes())
	body := http.MaxBytesReader(w, r.Body, s.maxCanonicalBytes())

	if !l.Live { // the lease already completed: an idempotent retry
		h := sha256.New()
		if _, err := io.Copy(h, body); err != nil {
			return err
		}
		return s.ackRepeatPrepare(ctx, w, l, hex.EncodeToString(h.Sum(nil)))
	}

	res, err := s.reserveDisk(0)
	if err != nil {
		return err
	}
	defer res.release()
	out, err := s.blobs.Create(l.JobID, blobs.Canonical)
	if err != nil {
		return err
	}
	defer out.Abort()
	if _, err := io.Copy(&guardedWriter{w: out, res: res}, body); err != nil {
		var mbe *http.MaxBytesError
		switch {
		case errors.As(err, &mbe):
			return s.rejectResult(ctx, l, http.StatusRequestEntityTooLarge, "canonical audio exceeds the size bound")
		case errors.Is(err, errDiskLow):
			return storageFull("free space fell below the minimum during the upload")
		case errors.Is(err, errTooSlow):
			return tooSlow()
		}
		// A transfer failure is not the worker's result being wrong: the
		// lease stays live and the worker may retry the upload.
		return badRequest("reading body: " + err.Error())
	}
	release, err := s.analysisSlot(ctx, p.Grant)
	if err != nil {
		return err
	}
	c, err := s.analyze(ctx, out.File, 0)
	release()
	if err != nil {
		if ctx.Err() != nil {
			return errClientGone // not a verdict on the audio
		}
		if convertible, rej := mediaVerdict(err); convertible || rej != nil {
			return s.rejectResult(ctx, l, http.StatusUnprocessableEntity, err.Error())
		}
		return err
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
	pc := store.PrepareCompletion{Canonical: c.Canonical, Ranges: c.Ranges, ComputeMS: computeMS, FFmpegVersion: ffmpeg}
	if job.Canonical != nil {
		// Accepted already (by this lease, or a newer one after ours
		// expired). Never touch the published files; let the store decide.
		if err := s.st.AcceptPrepare(ctx, l.ID, p.ID, pc); err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, resultAck{Lease: l.ID, Outcome: api.OutcomeDone})
		return nil
	}
	if err := s.blobs.WriteFile(l.JobID, blobs.CanonicalIndex, blobs.EncodeIndex(c.Index)); err != nil {
		return err
	}
	if err := out.Commit(); err != nil {
		return err
	}
	if err := s.st.AcceptPrepare(ctx, l.ID, p.ID, pc); err != nil {
		// Not accepted (the lease expired meanwhile, or the job was
		// deleted). We hold the job lock and no canonical audio is recorded,
		// so the files are ours to remove.
		_ = s.blobs.Remove(l.JobID, blobs.Canonical, blobs.CanonicalIndex)
		return err
	}
	if err := s.purgeOriginal(ctx, l.JobID); err != nil {
		s.log.Warn().Err(err).Str(obs.FieldJobID, l.JobID).Msg("purging original failed; retention will retry")
	}
	obs.Info(&s.log, obs.EventTaskDone).Str(obs.FieldLeaseID, l.ID).Str(obs.FieldJobID, l.JobID).
		Int(obs.FieldTaskSeq, l.Seq).Str(obs.FieldTaskKind, string(l.Kind)).Str(obs.FieldWorkerID, p.ID).
		Int64(obs.FieldComputeMS, computeMS).Float64(obs.FieldAudioSeconds, float64(c.Samples)/media.SampleRate).
		Msg("prepare done")
	s.logPrepared(l.JobID, c)
	s.metrics.LeaseEnded(api.OutcomeDone, api.KindPrepare)
	s.metrics.TaskCompute(api.KindPrepare, job.Model, float64(computeMS)/1000)
	s.disp.Notify()
	writeJSON(w, http.StatusOK, resultAck{Lease: l.ID, Outcome: api.OutcomeDone})
	return nil
}

func (s *Server) ackRepeatPrepare(ctx context.Context, w http.ResponseWriter, l *store.Lease, sha string) error {
	job, err := s.st.Job(ctx, l.JobID)
	if err != nil {
		return err
	}
	if job.Canonical == nil || job.Canonical.SHA256 != sha {
		return conflict("lease already completed with different audio")
	}
	writeJSON(w, http.StatusOK, resultAck{Lease: l.ID, Outcome: api.OutcomeDone})
	return nil
}

// purgeOriginal deletes the original upload once canonical audio exists
// (DESIGN.md §8.1 retention) and records it.
func (s *Server) purgeOriginal(ctx context.Context, jobID string) error {
	if err := s.blobs.Remove(jobID, blobs.Original); err != nil {
		return err
	}
	if err := s.ops.MediaPurged(ctx, jobID, MediaOriginal); err != nil {
		return err
	}
	obs.Info(&s.log, obs.EventJobPurged).Str(obs.FieldJobID, jobID).Str(obs.FieldWhat, "original").Msg("media purged")
	return nil
}

// storedResult is what is kept per transcribe task: the validated result
// minus the fields stored in their own columns. Marshalling a struct is
// deterministic, which CompleteTranscribe's idempotency check relies on.
type storedResult struct {
	Model       string        `json:"model"`
	ModelSHA256 string        `json:"model_sha256,omitempty"`
	VAD         string        `json:"vad,omitempty"`
	Language    string        `json:"language,omitempty"`
	Segments    []api.Segment `json:"segments"`
}

// validateTranscribe checks a result against its lease and normalizes it:
// segment starts are monotonic, times lie within the task (a small overrun
// of the end is clamped), text is bounded UTF-8, and the model matches.
func (s *Server) validateTranscribe(res *api.TranscribeResult, l *store.Lease) error {
	durMS := (l.EndSample - l.StartSample) * 1000 / media.SampleRate
	if res.Model != l.Model {
		return fmt.Errorf("model %q does not match the lease's %q", res.Model, l.Model)
	}
	if m, err := s.lookupModel(l.Model); err == nil && res.ModelSHA256 != "" && res.ModelSHA256 != m.SHA256 {
		return errors.New("model_sha256 does not match the pinned model")
	}
	for name, v := range map[string]string{"engine_version": res.EngineVersion, "vad": res.VAD, "language": res.Language} {
		if len(v) > 200 || !utf8.ValidString(v) {
			return fmt.Errorf("%s is too long or not UTF-8", name)
		}
	}
	if res.ComputeMS < 0 {
		return errors.New("compute_ms is negative")
	}
	if maxSeg := durMS/minSegmentSpanMS + segmentCountSlack; int64(len(res.Segments)) > maxSeg {
		return fmt.Errorf("%d segments for %d ms of audio", len(res.Segments), durMS)
	}
	var prev int64
	for i := range res.Segments {
		sg := &res.Segments[i]
		switch {
		case sg.StartMS < 0 || sg.EndMS < sg.StartMS:
			return fmt.Errorf("segment %d: bad times [%d, %d]", i, sg.StartMS, sg.EndMS)
		case sg.StartMS < prev:
			return fmt.Errorf("segment %d starts before segment %d", i, i-1)
		case sg.StartMS > durMS || sg.EndMS > durMS+endToleranceMS:
			return fmt.Errorf("segment %d [%d, %d] beyond the task's %d ms", i, sg.StartMS, sg.EndMS, durMS)
		case len(sg.Text) > maxSegmentText || !utf8.ValidString(sg.Text):
			return fmt.Errorf("segment %d: text too long or not UTF-8", i)
		}
		sg.EndMS = min(sg.EndMS, durMS)
		prev = sg.StartMS
	}
	return nil
}

func (s *Server) transcribeResult(w http.ResponseWriter, r *http.Request, p *Principal, l *store.Lease) error {
	ctx := r.Context()
	var res api.TranscribeResult
	s.streamBody(r, maxResultBody)
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxResultBody))
	if err := dec.Decode(&res); err != nil {
		var mbe *http.MaxBytesError
		switch {
		case errors.As(err, &mbe):
			return s.rejectResult(ctx, l, http.StatusRequestEntityTooLarge, "result exceeds "+itoa(maxResultBody)+" bytes")
		case invalidJSON(err):
			return s.rejectResult(ctx, l, http.StatusUnprocessableEntity, "result is not valid JSON: "+err.Error())
		}
		// The body did not arrive (connection dropped, client gave up):
		// nothing was judged, so no attempt is spent.
		return badRequest("reading result: " + err.Error())
	}
	if err := s.validateTranscribe(&res, l); err != nil {
		return s.rejectResult(ctx, l, http.StatusUnprocessableEntity, err.Error())
	}
	if loop := detectLoop(res.Segments); loop != nil {
		return s.rejectLoop(ctx, l, loop)
	}
	stored, err := json.Marshal(storedResult{Model: res.Model, ModelSHA256: res.ModelSHA256, VAD: res.VAD,
		Language: res.Language, Segments: nonNilSegments(res.Segments)})
	if err != nil {
		return err
	}
	end, err := s.st.CompleteTranscribe(ctx, l.ID, p.ID, store.TranscribeCompletion{
		Result: stored, EngineVersion: res.EngineVersion, ComputeMS: res.ComputeMS})
	if err != nil {
		return err
	}
	if l.Live { // first completion, not a retry
		audio := float64(l.EndSample-l.StartSample) / media.SampleRate
		compute := float64(res.ComputeMS) / 1000
		rtf := 0.0
		if audio > 0 {
			rtf = compute / audio
		}
		obs.Info(&s.log, obs.EventTaskDone).Str(obs.FieldLeaseID, l.ID).Str(obs.FieldJobID, l.JobID).
			Int(obs.FieldTaskSeq, l.Seq).Str(obs.FieldTaskKind, string(l.Kind)).Str(obs.FieldWorkerID, p.ID).
			Str(obs.FieldModel, l.Model).Int64(obs.FieldComputeMS, res.ComputeMS).
			Float64(obs.FieldAudioSeconds, audio).Float64(obs.FieldRTF, rtf).Msg("transcribe done")
		s.metrics.LeaseEnded(api.OutcomeDone, api.KindTranscribe)
		s.metrics.TaskCompute(api.KindTranscribe, l.Model, compute)
		s.metrics.TaskRealtimeFactor(l.Model, p.ID, rtf)
		s.metrics.WorkerAudio(p.ID, l.Model, audio)
	}
	switch {
	case end.JobComplete:
		if err := s.finishJob(ctx, l.JobID); err != nil {
			// The result is stored; the reaper retries assembly.
			s.log.Error().Err(err).Str(obs.FieldJobID, l.JobID).Msg("assembling transcript failed")
		}
	case end.JobFailed:
		s.jobFinished(ctx, l.JobID)
	}
	writeJSON(w, http.StatusOK, resultAck{Lease: l.ID, Outcome: api.OutcomeDone})
	return nil
}

// invalidJSON reports whether a decode error is about the content (bad
// syntax, wrong types, an empty body) rather than a failed transfer.
func invalidJSON(err error) bool {
	var syn *json.SyntaxError
	var typ *json.UnmarshalTypeError
	return errors.As(err, &syn) || errors.As(err, &typ) || errors.Is(err, io.EOF)
}

func nonNilSegments(s []api.Segment) []api.Segment {
	if s == nil {
		return []api.Segment{}
	}
	return s
}

// finishJob assembles a job whose transcribe tasks are all done and marks
// it done. It is idempotent and serialized per job.
func (s *Server) finishJob(ctx context.Context, jobID string) error {
	unlock := s.locks.Lock(jobID)
	defer unlock()
	job, err := s.st.Job(ctx, jobID)
	if errors.Is(err, store.ErrNotFound) {
		return nil // deleted meanwhile; nothing to assemble
	}
	if err != nil {
		return err
	}
	if job.State == api.JobDone {
		return nil
	}
	if err := s.assemble(ctx, job); err != nil {
		return err
	}
	if _, err := s.st.MarkJobDone(ctx, jobID); err != nil {
		return err
	}
	s.jobFinished(ctx, jobID)
	return nil
}

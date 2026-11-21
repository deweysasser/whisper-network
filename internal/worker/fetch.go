package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/sandbox"
)

// fetchFileSize is yt-dlp's RLIMIT_FSIZE. It writes the media to stdout, so
// files in its work directory are small (the cookies jar it saves back, a
// one-file build's unpacked runtime).
const fetchFileSize = 256 << 20

// ytdlpEnv keeps Python from writing bytecode or reading per-user site
// packages.
var ytdlpEnv = []string{"PYTHONDONTWRITEBYTECODE=1", "PYTHONNOUSERSITE=1"}

// errYtDlpUnsupported: yt-dlp has no extractor for the URL and sent
// nothing, so a plain GET may try.
var errYtDlpUnsupported = errors.New("yt-dlp: unsupported URL")

// errFetchTimeout ends a fetch, upload included, that ran past FetchTimeout.
var errFetchTimeout = errors.New("fetch exceeded its time limit")

// errUploadEnded is what a fetch's producer sees once the PUT has ended.
var errUploadEnded = errors.New("result upload ended")

// fetch downloads a fetch lease's URL and streams it to the server as the
// job's original: through yt-dlp in a sandbox that may only reach the
// egress proxy, or, when yt-dlp has no extractor for the URL, with a plain
// GET through the same filter. Nothing touches the disk.
//
// The URL never reaches the logs or the failure report: the lease's site
// stands for it, and failures are reported by class (stage and
// retryability), not with yt-dlp's stderr or a Go error's text.
func (w *Worker) fetch(ctx context.Context, l *api.Lease, dir string, run *leaseRun, log zerolog.Logger) (taskStats, error) {
	if l.URL == "" {
		return taskStats{}, preWork("fetch lease carries no url", nil)
	}
	if w.proxy == nil || w.fetchClient == nil || w.proxyStopped() {
		return taskStats{}, preWork("the egress proxy is not running", nil)
	}
	if w.noProxyConnect {
		return taskStats{}, preWork("the downloader sandbox cannot permit proxy connects (Landlock ABI 4 needed)", nil)
	}
	// yt-dlp may have been removed or replaced since startup.
	if _, err := exec.LookPath(w.tools.ytdlp); err != nil {
		return taskStats{}, preWork("yt-dlp is no longer installed at "+w.tools.ytdlp, nil)
	}
	cookies := ""
	if w.cfg.YtDlpCookies != "" {
		cookies = filepath.Join(dir, "cookies.txt")
		if err := w.copyForChild(w.cfg.YtDlpCookies, cookies); err != nil {
			return taskStats{}, preWork("copying the yt-dlp cookies file", err)
		}
	}
	start := time.Now()
	method := api.FetchMethodYtDlp
	n, err := w.fetchYtDlp(ctx, l, dir, cookies, run, log)
	if errors.Is(err, errYtDlpUnsupported) {
		log.Info().Msg("yt-dlp has no extractor for the URL; trying a plain GET")
		method = api.FetchMethodHTTP
		n, err = w.fetchGET(ctx, l, run)
	}
	if err != nil {
		return taskStats{}, err
	}
	elapsed := time.Since(start)
	log.Info().Str(obs.FieldFetchMethod, method).Int64(obs.FieldBytes, n).
		Int64(obs.FieldDurationMS, elapsed.Milliseconds()).Msg("fetched")
	return taskStats{computeMS: elapsed.Milliseconds()}, nil
}

// proxyStopped reports whether the egress proxy no longer accepts
// connections.
func (w *Worker) proxyStopped() bool {
	select {
	case <-w.proxy.Done():
		return true
	default:
		return false
	}
}

// copyForChild copies the operator's file src to dst, a new file the child
// may read and write.
func (w *Worker) copyForChild(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := w.createForChild(dst)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	return errors.Join(err, out.Close())
}

// fetchChildSpec is the sandbox of a fetch child (yt-dlp, and the deno it
// may run) working in dir: read-only system paths and the yt-dlp tree,
// read-write dir, TCP connect to proxyPort only (DESIGN.md §7.6), small
// files, lowest priority, and no RLIMIT_AS unless the operator set
// Limits.FetchAddressSpace (deno reserves tens of GiB of address space).
func (w *Worker) fetchChildSpec(dir string, proxyPort uint16) sandbox.Spec {
	lim := w.cfg.Limits
	spec := sandbox.Spec{
		ReadOnly:     slices.Concat(systemReadOnly, childSystemFiles, fetchSystemReadOnly, w.tools.ytdlpRO),
		ReadWrite:    []string{dir},
		DenyNet:      true,
		ConnectPorts: []uint16{proxyPort},
		FileSize:     fetchFileSize,
		OpenFiles:    uint64(lim.OpenFiles),
		UID:          w.cfg.ChildUID,
		GID:          w.cfg.ChildGID,
	}.WithLowestPriority()
	w.limitProcesses(&spec)
	if !lim.NoAddressSpaceLimit && lim.FetchAddressSpace > 0 {
		spec.AddressSpace = uint64(lim.FetchAddressSpace)
	}
	return spec
}

// fetchYtDlp runs yt-dlp with its stdout streamed into the result PUT.
func (w *Worker) fetchYtDlp(ctx context.Context, l *api.Lease, dir, cookies string, run *leaseRun, log zerolog.Logger) (int64, error) {
	lim := w.cfg.Limits
	spec := w.fetchChildSpec(dir, w.proxy.Port())
	// FetchTimeout bounds the whole fetch, upload included: a server that
	// stops reading must not hold the slot past it.
	fetchCtx, cancelFetch := context.WithTimeoutCause(ctx, lim.FetchTimeout, errFetchTimeout)
	defer cancelFetch()
	childCtx, kill := context.WithCancel(fetchCtx)
	defer kill()
	up := w.newFetchUpload(fetchCtx, l, api.FetchMethodYtDlp, run, kill)
	res, err := w.runChild(childCtx, childRun{
		argv: YtDlpArgs(w.tools.ytdlp, l.URL, w.proxy.Port(), cookies), spec: spec, dir: dir,
		timeout: lim.FetchTimeout, stdout: up, env: ytdlpEnv,
	})
	n, uerr := up.finish(err)
	switch {
	case isContentRefusal(uerr):
		return n, w.uploadFailure(ctx, l, uerr)
	case ctx.Err() != nil:
		return n, context.Cause(ctx)
	case errors.Is(context.Cause(fetchCtx), errFetchTimeout), errors.Is(err, errChildTimeout):
		w.requestEgressProbe()
		return n, fetchTransient("yt-dlp timed out")
	case uerr != nil:
		return n, w.uploadFailure(ctx, l, uerr)
	case err == nil && n > 0:
		// Exited 0 and the server took the whole stream: done, whatever
		// the stderr says.
		return n, nil
	}
	if rerr := ytdlpRunError(err); rerr != nil {
		return n, rerr
	}
	var ee *exec.ExitError
	errors.As(err, &ee)
	// Only the class is logged: the stderr quotes the URL, the video's id
	// and its title, even with the URL itself redacted.
	outcome := classifyYtDlp(errOrNil(ee), redactURL(res.stderr, l.URL))
	log.Info().Str(obs.FieldYtDlpOutcome, outcome.String()).Msg("yt-dlp finished")
	switch outcome {
	case ytdlpOK:
		return 0, fetchTransient("yt-dlp produced no output")
	case ytdlpUnsupported:
		if n == 0 {
			return 0, errYtDlpUnsupported
		}
		return n, fetchTransient("yt-dlp failed")
	case ytdlpRateLimited:
		return n, fetchRateLimited("rate limited (yt-dlp)")
	case ytdlpPermanent:
		return n, fetchPermanent("media unavailable (yt-dlp)")
	}
	w.requestEgressProbe()
	return n, fetchTransient("yt-dlp failed")
}

// ytdlpRunError classifies how a yt-dlp child ended before its stderr is
// looked at. A child that never started, or whose sandbox could not be set
// up (sandbox-exec's exit 126), is the worker's own problem: pre-work, and
// nothing to do with the network. Any other error that is not an exit
// status (exec.ErrWaitDelay when a grandchild held stdout open, say) is a
// transient failure mid-run. nil and ordinary exit statuses are left to
// classifyYtDlp.
func ytdlpRunError(err error) error {
	var ee *exec.ExitError
	switch {
	case err == nil:
		return nil
	case errors.Is(err, errChildStart):
		return preWork("starting yt-dlp", err)
	case errors.As(err, &ee) && ee.ExitCode() == sandbox.SetupFailureStatus:
		return preWork("setting up the yt-dlp sandbox", nil)
	case errors.As(err, &ee):
		return nil
	}
	return fetchTransient("yt-dlp failed")
}

// errOrNil returns ee as an error, or nil when it is nil (avoiding a
// non-nil interface holding a nil pointer).
func errOrNil(ee *exec.ExitError) error {
	if ee == nil {
		return nil
	}
	return ee
}

// fetchGET fetches the lease's URL with a plain GET through the egress
// filter (each redirect hop is checked too), within FetchTimeout.
func (w *Worker) fetchGET(ctx context.Context, l *api.Lease, run *leaseRun) (int64, error) {
	getCtx, cancel := context.WithTimeoutCause(ctx, w.cfg.Limits.FetchTimeout, errFetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(getCtx, http.MethodGet, l.URL, nil)
	if err != nil {
		return 0, fetchPermanent("the URL cannot be fetched")
	}
	req.Header.Set("User-Agent", "whisper-network-downloader")
	resp, err := w.fetchClient.Do(req)
	if err != nil {
		return 0, w.getFailure(ctx, err)
	}
	defer resp.Body.Close()
	switch s := resp.StatusCode; {
	case s == http.StatusOK:
	case s == http.StatusTooManyRequests:
		return 0, fetchRateLimited("rate limited (HTTP 429)")
	case s >= 500:
		return 0, fetchTransient(fmt.Sprintf("GET failed (HTTP %d)", s))
	case s >= 400:
		return 0, fetchPermanent(fmt.Sprintf("GET failed (HTTP %d)", s))
	default:
		return 0, fetchTransient(fmt.Sprintf("GET got HTTP %d", s))
	}
	if mt, bad := notMediaType(resp.Header.Get("Content-Type")); bad {
		return 0, fetchNotMedia(errNotMedia{kind: mt})
	}
	if l.MaxOutputBytes > 0 && resp.ContentLength > l.MaxOutputBytes {
		return 0, fetchTooLarge(l.MaxOutputBytes)
	}
	up := w.newFetchUpload(getCtx, l, api.FetchMethodHTTP, run, cancel)
	_, cerr := io.Copy(up, resp.Body)
	n, uerr := up.finish(cerr)
	switch {
	case isContentRefusal(uerr):
		return n, w.uploadFailure(ctx, l, uerr)
	case ctx.Err() != nil:
		return n, context.Cause(ctx)
	case errors.Is(context.Cause(getCtx), errFetchTimeout):
		w.requestEgressProbe()
		return n, fetchTransient("GET timed out")
	case uerr != nil:
		return n, w.uploadFailure(ctx, l, uerr)
	case cerr != nil:
		return n, w.getFailure(ctx, cerr)
	case n == 0:
		return 0, fetchPermanent("GET returned an empty body")
	}
	return n, nil
}

// getFailure classifies a plain GET's request or body error. A refused
// address is the URL's fault; anything else is the network's, which may
// also mean the egress IP changed.
func (w *Worker) getFailure(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	class := errClass(err)
	if class == "refused" {
		return fetchPermanent("GET refused: the URL or a redirect leads to a filtered address or port")
	}
	w.requestEgressProbe()
	return fetchTransient("GET failed (" + class + ")")
}

// isContentRefusal reports the media guard's or the byte cap's verdict.
func isContentRefusal(err error) bool {
	return errors.As(err, new(errNotMedia)) || errors.Is(err, errTooLarge)
}

// uploadFailure classifies a failed result stream: the media guard or byte
// cap tripped, the task was cancelled, or the PUT itself failed.
func (w *Worker) uploadFailure(ctx context.Context, l *api.Lease, err error) error {
	var nm errNotMedia
	switch {
	case errors.As(err, &nm):
		return fetchNotMedia(nm)
	case errors.Is(err, errTooLarge):
		return fetchTooLarge(l.MaxOutputBytes)
	case ctx.Err() != nil:
		return context.Cause(ctx)
	}
	return staged(api.StageInput, transferError("uploading result", err))
}

func fetchTransient(reason string) error {
	return &taskError{reason: reason, retryable: true, stage: api.StageInput}
}

func fetchPermanent(reason string) error {
	return &taskError{reason: reason, stage: api.StageInput}
}

func fetchRateLimited(reason string) error {
	return &taskError{reason: reason, retryable: true, stage: api.StageRateLimited}
}

func fetchNotMedia(nm errNotMedia) error {
	return &taskError{reason: nm.Error(), stage: api.StageInput}
}

func fetchTooLarge(max int64) error {
	return &taskError{reason: fmt.Sprintf("too large: the media exceeds the %d-byte limit", max), stage: api.StageInput}
}

// fetchUpload is the writer a fetch's producer (yt-dlp's stdout, a GET
// body) writes to. The first byte starts the result PUT; the bytes pass
// through the media guard and the byte cap on their way into it. Ending it
// cleanly (finish(nil)) terminates the chunked body; ending it with an error
// aborts the PUT, so the server sees a body that broke off and keeps
// nothing. A PUT that fails first (guard, cap, 410) calls stop, which ends
// the producer.
type fetchUpload struct {
	w      *Worker
	l      *api.Lease
	method string
	run    *leaseRun
	parent context.Context
	stop   func()

	mu      sync.Mutex
	started bool
	n       int64
	// finishing is set once the producer has ended; failedFirst records
	// a PUT that failed before that, which then decides the outcome.
	finishing, failedFirst bool
	cancel                 context.CancelFunc
	pr                     *io.PipeReader
	pw                     *io.PipeWriter
	body                   *errRecorder
	done                   chan error
	// stopAbort unregisters the abort on the parent context's end.
	stopAbort func() bool
}

// newFetchUpload returns the upload for a producer bounded by ctx. When ctx
// ends (the fetch's time limit, the lease lost) the PUT is aborted and the
// pipe closed, so neither a producer blocked writing to a server that stopped
// reading nor the PUT outlives it.
func (w *Worker) newFetchUpload(ctx context.Context, l *api.Lease, method string, run *leaseRun, stop func()) *fetchUpload {
	u := &fetchUpload{w: w, l: l, method: method, run: run, parent: ctx, stop: stop}
	u.stopAbort = context.AfterFunc(ctx, func() { u.abort(context.Cause(ctx)) })
	return u
}

// abort ends a started upload uncleanly with cause.
func (u *fetchUpload) abort(cause error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.started {
		u.cancel()
		u.pr.CloseWithError(cause)
	}
}

func (u *fetchUpload) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	u.mu.Lock()
	if !u.started {
		u.startLocked()
	}
	pw := u.pw
	u.mu.Unlock()
	n, err := pw.Write(p)
	u.mu.Lock()
	u.n += int64(n)
	u.mu.Unlock()
	return n, err
}

func (u *fetchUpload) startLocked() {
	u.started = true
	ctx, cancel := context.WithCancel(u.parent)
	u.cancel = cancel
	u.pr, u.pw = io.Pipe()
	u.body = &errRecorder{r: newCapReader(newMediaGuard(u.pr), u.l.MaxOutputBytes)}
	u.done = make(chan error, 1)
	go func() {
		err := u.w.api.PutFetchResult(ctx, u.l.ID, u.method, u.body)
		// Nothing reads the pipe any more: unblock and fail the producer.
		u.pr.CloseWithError(errUploadEnded)
		if err != nil {
			u.mu.Lock()
			u.failedFirst = !u.finishing
			u.mu.Unlock()
			u.stop()
		}
		u.done <- err
	}()
}

// finish ends the upload after the producer ended with prodErr and returns
// the bytes the producer wrote and, if the upload failed, why: the guard's
// or cap's error first, else the PUT's. A producer error aborts the PUT and
// is the caller's to classify, unless the PUT had failed first (and so
// ended the producer). With no byte written, no PUT was made.
func (u *fetchUpload) finish(prodErr error) (int64, error) {
	defer u.stopAbort()
	u.mu.Lock()
	started, n := u.started, u.n
	u.finishing = true
	u.mu.Unlock()
	if !started {
		return 0, nil
	}
	if prodErr != nil {
		// Abort: the chunked body must never be terminated cleanly.
		u.cancel()
		u.pw.CloseWithError(prodErr)
	} else {
		// From here the server may accept the result before the worker
		// hears back; a heartbeat's 410 must not cancel the task.
		u.run.uploading.Store(true)
		_ = u.pw.Close()
	}
	putErr := <-u.done
	u.cancel()
	u.mu.Lock()
	n, failedFirst := u.n, u.failedFirst
	u.mu.Unlock()
	if berr := u.body.get(); berr != nil && (errors.As(berr, new(errNotMedia)) || errors.Is(berr, errTooLarge)) {
		return n, berr
	}
	if prodErr != nil && !failedFirst {
		return n, nil
	}
	return n, putErr
}

// errRecorder remembers the first error other than io.EOF its reader
// returned.
type errRecorder struct {
	r   io.Reader
	mu  sync.Mutex
	err error
}

func (e *errRecorder) Read(p []byte) (int, error) {
	n, err := e.r.Read(p)
	if err != nil && err != io.EOF {
		e.mu.Lock()
		if e.err == nil {
			e.err = err
		}
		e.mu.Unlock()
	}
	return n, err
}

func (e *errRecorder) get() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.err
}

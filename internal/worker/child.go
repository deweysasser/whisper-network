package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/sandbox"
)

// childRun describes one sandboxed child process.
type childRun struct {
	argv    []string
	spec    sandbox.Spec
	dir     string
	timeout time.Duration
	// stdout receives the child's standard output; nil discards it.
	stdout io.Writer
	// env is appended to the child's environment (after Config.ChildEnv).
	env []string
}

// childResult is what a finished child left behind.
type childResult struct {
	elapsed time.Duration
	stderr  string // tail, at most api.MaxStderrTail bytes
}

// errChildTimeout reports a child killed for exceeding its wall-clock limit.
var errChildTimeout = errors.New("child exceeded its time limit")

// children tracks running child PIDs for the RSS gauge.
type children struct {
	mu   sync.Mutex
	pids map[int]struct{}
}

func (c *children) add(pid int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pids == nil {
		c.pids = map[int]struct{}{}
	}
	c.pids[pid] = struct{}{}
}

func (c *children) remove(pid int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.pids, pid)
}

func (c *children) list() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]int, 0, len(c.pids))
	for p := range c.pids {
		out = append(out, p)
	}
	return out
}

// runChild runs r under sandbox-exec and waits for it. Cancelling ctx kills
// the child; exceeding r.timeout kills it and returns errChildTimeout. A
// non-zero exit returns an *exec.ExitError. The child's stdout goes to
// r.stdout or is discarded (whisper-cli may print transcript text there),
// and the tail of its stderr is returned.
func (w *Worker) runChild(ctx context.Context, r childRun) (childResult, error) {
	ctx, cancel := context.WithTimeoutCause(ctx, r.timeout, errChildTimeout)
	defer cancel()
	cmd := sandbox.Command(ctx, w.cfg.Self, r.spec, r.argv...)
	cmd.Dir = r.dir
	cmd.Env = append(append(childEnv(os.Environ(), r.dir), w.cfg.ChildEnv...), r.env...)
	cmd.Stdin = nil
	cmd.Stdout = io.Discard
	if r.stdout != nil {
		cmd.Stdout = r.stdout
	}
	tail := &tailBuffer{max: 2 * api.MaxStderrTail}
	cmd.Stderr = tail
	// After a kill, do not wait forever for grandchildren holding the pipe.
	cmd.WaitDelay = 5 * time.Second

	start := time.Now()
	if err := cmd.Start(); err != nil {
		return childResult{}, fmt.Errorf("starting %s: %w", filepath.Base(r.argv[0]), err)
	}
	w.children.add(cmd.Process.Pid)
	err := sandbox.Wait(cmd)
	w.children.remove(cmd.Process.Pid)
	res := childResult{elapsed: time.Since(start), stderr: obs.StderrTail(tail.String())}
	if cause := context.Cause(ctx); cause != nil {
		if errors.Is(cause, errChildTimeout) {
			return res, fmt.Errorf("%s killed after %s: %w", filepath.Base(r.argv[0]), r.timeout, errChildTimeout)
		}
		return res, cause
	}
	return res, err
}

// childEnv builds a child's environment from an allow-list: the locale,
// timezone and PATH, and the tuning knobs of ggml, OpenMP and CUDA. Nothing
// else of the worker's environment passes (no tokens, agent sockets, session
// bus or cloud credentials). HOME and TMPDIR point at the task's directory,
// the only place the sandbox lets the child write.
func childEnv(env []string, dir string) []string {
	out := make([]string, 0, 8)
	for _, kv := range env {
		k, _, ok := strings.Cut(kv, "=")
		if ok && childEnvAllowed(k) {
			out = append(out, kv)
		}
	}
	return append(out, "HOME="+dir, "TMPDIR="+dir)
}

// childEnvAllowed reports whether the variable k may pass to a child.
func childEnvAllowed(k string) bool {
	switch k {
	case "PATH", "LANG", "LANGUAGE", "TZ":
		return true
	}
	for _, p := range []string{"LC_", "GGML_", "OMP_", "CUDA_"} {
		if strings.HasPrefix(k, p) {
			return true
		}
	}
	return false
}

// tailBuffer keeps the last max bytes written to it.
type tailBuffer struct {
	mu  sync.Mutex
	max int
	buf []byte
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, p...)
	if over := len(t.buf) - t.max; over > 0 {
		t.buf = append(t.buf[:0], t.buf[over:]...)
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return string(t.buf)
}

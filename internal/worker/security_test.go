//go:build linux

package worker

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/media"
	"github.com/deweysasser/whisper-network/internal/sandbox"
	"github.com/deweysasser/whisper-network/internal/worker/workertest"
)

// A child that replaces its output with a symlink must not make the worker
// read through it: not /dev/zero (endless), not a file only the worker can
// read (exfiltration via the uploaded result).
func TestSymlinkedOutputIsRefused(t *testing.T) {
	secretDir := t.TempDir()
	secretJSON := filepath.Join(secretDir, "secret.json")
	require.NoError(t, os.WriteFile(secretJSON, workertest.WhisperJSON("m", "en", 1000, 1000), 0o600))
	secretFLAC := filepath.Join(secretDir, "secret.flac")
	ff, err := os.Create(secretFLAC)
	require.NoError(t, err)
	require.NoError(t, media.EncodeFLAC(ff, bytes.NewReader(make([]byte, media.MinSamples*2)), media.MinSamples))
	require.NoError(t, ff.Close())

	for _, c := range []struct {
		name   string
		kind   api.TaskKind
		env    string
		reason string
	}{
		{"whisper to /dev/zero", api.KindTranscribe, workertest.WhisperMode(workertest.SymlinkMode("/dev/zero")), "whisper-cli output refused"},
		{"whisper to a secret", api.KindTranscribe, workertest.WhisperMode(workertest.SymlinkMode(secretJSON)), "whisper-cli output refused"},
		{"ffmpeg to /dev/zero", api.KindPrepare, workertest.FFmpegMode(workertest.SymlinkMode("/dev/zero")), "ffmpeg output refused"},
		{"ffmpeg to a secret", api.KindPrepare, workertest.FFmpegMode(workertest.SymlinkMode(secretFLAC)), "ffmpeg output refused"},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t)
			h.cfg.ChildEnv = []string{c.env}
			task := h.srv.AddTask(workertest.Task{Kind: c.kind, Input: workertest.WAV(1), Model: "tiny.test"})
			_, _ = h.run()
			h.activate()
			h.eventually(func() bool { return len(h.srv.Task(task).Failures) > 0 }, "no failure reported")
			f := h.srv.Task(task).Failures[0]
			assert.Contains(t, f.Reason, c.reason)
			assert.Contains(t, f.Reason, "symlink")
			assert.False(t, f.Retryable, "a planted link points at a compromised child")
			assert.Zero(t, h.srv.ResultUploads(), "nothing is uploaded")
			assert.NotContains(t, string(h.logs.Bytes()), "segment 0")
		})
	}
}

func TestOpenOutputChecks(t *testing.T) {
	dir := t.TempDir()
	reg := filepath.Join(dir, "out")
	require.NoError(t, os.WriteFile(reg, []byte("0123456789"), 0o600))

	b, err := readOutput(reg, 10)
	require.NoError(t, err)
	assert.Equal(t, "0123456789", string(b))

	_, err = readOutput(reg, 9)
	assert.ErrorIs(t, err, errBadOutput, "over the size bound")

	link := filepath.Join(dir, "hard")
	require.NoError(t, os.Link(reg, link))
	_, err = readOutput(link, 100)
	assert.ErrorIs(t, err, errBadOutput, "a hard link may point at a file outside the sandbox")

	fifo := filepath.Join(dir, "fifo")
	require.NoError(t, syscall.Mkfifo(fifo, 0o600))
	done := make(chan error, 1)
	go func() { _, err := readOutput(fifo, 100); done <- err }()
	select {
	case err := <-done:
		assert.ErrorIs(t, err, errBadOutput, "a FIFO is not a regular file")
	case <-time.After(5 * time.Second):
		t.Fatal("opening a planted FIFO blocked")
	}

	_, err = readOutput(filepath.Join(dir, "missing"), 100)
	assert.ErrorIs(t, err, os.ErrNotExist)
	assert.NotErrorIs(t, err, errBadOutput)
}

// The input is capped at what the lease advertises.
func TestOversizedInputIsRefused(t *testing.T) {
	for _, c := range []struct {
		name string
		task workertest.Task
	}{
		{"prepare", workertest.Task{Kind: api.KindPrepare, Input: bytes.Repeat([]byte{1, 0}, 16000),
			SendInputHash: true, LeaseInputBytes: 1000}},
		{"transcribe", workertest.Task{Kind: api.KindTranscribe, Input: workertest.WAV(2), Model: "tiny.test",
			LeaseAudioSamples: 16000}},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t)
			task := h.srv.AddTask(c.task)
			_, _ = h.run()
			h.activate()
			h.eventually(func() bool { return len(h.srv.Task(task).Failures) > 0 }, "no failure reported")
			assert.Contains(t, h.srv.Task(task).Failures[0].Reason, "input larger than the lease allows")
			assert.Zero(t, h.srv.ResultUploads())
		})
	}
}

// A lease without the input size is refused, not downloaded unbounded, and
// the failure is not retryable: the lease, not the worker, is at fault.
func TestLeaseWithoutSizeIsRefused(t *testing.T) {
	for _, c := range []struct {
		name, reason string
		task         workertest.Task
	}{
		{"prepare", "input_bytes", workertest.Task{Kind: api.KindPrepare, Input: bytes.Repeat([]byte{1, 0}, 16000), OmitSize: true}},
		{"transcribe", "audio_samples", workertest.Task{Kind: api.KindTranscribe, Input: workertest.WAV(1), Model: "tiny.test", OmitSize: true}},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t)
			task := h.srv.AddTask(c.task)
			_, _ = h.run()
			h.activate()
			h.eventually(func() bool { return len(h.srv.Task(task).Failures) > 0 }, "no failure reported")
			f := h.srv.Task(task).Failures[0]
			assert.Contains(t, f.Reason, c.reason)
			assert.False(t, f.Retryable)
			assert.Zero(t, h.srv.ResultUploads())
		})
	}
}

// A timed-out child is killed with its whole process tree.
func TestChildTimeoutKillsDescendants(t *testing.T) {
	h := newHarness(t)
	h.cfg.ChildEnv = []string{workertest.WhisperMode(workertest.ModeForkTree)}
	h.cfg.Limits.TimeoutFactor = 0.01
	// Long enough for the fake child (a re-executed, possibly race-built
	// test binary) to start and report its grandchild even on a loaded
	// machine; the child then hangs, so the timeout still fires.
	h.cfg.Limits.TimeoutBase = 5 * time.Second
	task := h.srv.AddTask(workertest.Task{Kind: api.KindTranscribe, Input: workertest.WAV(1), Model: "tiny.test"})
	_, _ = h.run()
	h.activate()
	h.eventually(func() bool { return len(h.srv.Task(task).Failures) > 0 }, "no failure")
	f := h.srv.Task(task).Failures[0]
	assert.Contains(t, f.Reason, "timed out")
	m := regexp.MustCompile(`grandchild pid (\d+)`).FindStringSubmatch(f.StderrTail)
	require.NotNil(t, m, "the child was killed before it reported its grandchild (timeout shorter than startup?); stderr: %q", f.StderrTail)
	pid, _ := strconv.Atoi(m[1])
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
	assert.Eventually(t, func() bool { return processGone(pid) }, 5*time.Second, 10*time.Millisecond,
		"the grandchild outlived the timeout kill")
}

// processGone reports whether pid has exited (or is a zombie awaiting its
// reaper).
func processGone(pid int) bool {
	if err := syscall.Kill(pid, 0); err == syscall.ESRCH {
		return true
	}
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return true
	}
	i := bytes.LastIndexByte(b, ')')
	return i >= 0 && i+2 < len(b) && b[i+2] == 'Z'
}

func TestChildIdentityRule(t *testing.T) {
	assert.ErrorContains(t, checkChildIdentity(0, 0, 0), "refusing to run as root")
	assert.ErrorContains(t, checkChildIdentity(0, 10001, 0), "refusing to run as root", "gid 0 is root's group")
	assert.NoError(t, checkChildIdentity(0, 10001, 10001))
	assert.NoError(t, checkChildIdentity(1000, 0, 0))
	assert.ErrorContains(t, checkChildIdentity(1000, 10001, 10001), "need the worker to run as root")
	assert.Error(t, checkChildIdentity(0, -1, 5))
}

func TestRootWorkerWithoutChildUIDRefusesToStart(t *testing.T) {
	h := newHarness(t)
	orig := geteuid
	geteuid = func() int { return 0 }
	t.Cleanup(func() { geteuid = orig })
	_, err := New(h.cfg)
	require.ErrorContains(t, err, "--child-uid")
	h.cfg.ChildUID, h.cfg.ChildGID = 10001, 10001
	_, err = New(h.cfg)
	require.NoError(t, err)
}

// Files the worker creates for a child never follow a planted link or reuse
// a planted file, and stay private.
func TestCreateForChildRefusesPlantedFiles(t *testing.T) {
	w := &Worker{}
	dir := t.TempDir()
	secret := filepath.Join(dir, "secret")
	require.NoError(t, os.WriteFile(secret, []byte("keep"), 0o600))

	link := filepath.Join(dir, "args.rsp")
	require.NoError(t, os.Symlink(secret, link))
	assert.Error(t, w.writeForChild(link, []byte("x")), "symlink to an existing file")
	dangling := filepath.Join(dir, "input")
	require.NoError(t, os.Symlink(filepath.Join(dir, "new-target"), dangling))
	assert.Error(t, w.writeForChild(dangling, []byte("x")), "dangling symlink")
	_, err := os.Stat(filepath.Join(dir, "new-target"))
	assert.ErrorIs(t, err, os.ErrNotExist, "nothing created through the link")
	assert.Error(t, w.writeForChild(secret, []byte("x")), "existing file")
	b, _ := os.ReadFile(secret)
	assert.Equal(t, "keep", string(b))

	fresh := filepath.Join(dir, "fresh")
	require.NoError(t, w.writeForChild(fresh, []byte("data")))
	fi, err := os.Stat(fresh)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), fi.Mode().Perm())
}

func TestLimitProcessesSharesOneAllowance(t *testing.T) {
	w := &Worker{cfg: Config{Limits: Limits{Processes: 100}}, facts: api.WorkerFacts{Slots: 3}}
	var s sandbox.Spec
	w.limitProcesses(&s)
	assert.Equal(t, sandbox.Spec{Processes: 300}, s, "shared uid: headroom for every slot's children")

	w.cfg.ChildUID, w.cfg.ChildGID = 10001, 10001
	s = sandbox.Spec{}
	w.limitProcesses(&s)
	assert.Equal(t, sandbox.Spec{ProcessLimit: 300}, s, "dedicated uid: one absolute limit for all slots")
}

func TestChildUIDMustBeDedicated(t *testing.T) {
	orig := countUIDTasks
	t.Cleanup(func() { countUIDTasks = orig })
	running := uint64(0)
	countUIDTasks = func(uid int) (uint64, error) {
		assert.Equal(t, 10001, uid)
		return running, nil
	}
	w := &Worker{cfg: Config{ChildUID: 10001, ChildGID: 10001}}
	assert.NoError(t, w.checkDedicatedChildUID())
	running = 3
	assert.ErrorContains(t, w.checkDedicatedChildUID(), "already runs 3")
	countUIDTasks = func(int) (uint64, error) { return 0, os.ErrPermission }
	assert.ErrorIs(t, w.checkDedicatedChildUID(), os.ErrPermission)
	assert.NoError(t, (&Worker{}).checkDedicatedChildUID(), "no child uid, nothing to check")
	// The real count works where /proc exists.
	_, err := orig(os.Getuid())
	assert.NoError(t, err)
}

func TestToolReadOnly(t *testing.T) {
	root := t.TempDir()
	opt := filepath.Join(root, "opt", "whisper")
	state := filepath.Join(root, "var", "state")
	home := filepath.Join(root, "home", "u")
	for _, d := range []string{opt, state, home, filepath.Join(home, "bin")} {
		require.NoError(t, os.MkdirAll(d, 0o755))
	}
	touch := func(p string) string {
		require.NoError(t, os.WriteFile(p, nil, 0o755))
		return p
	}

	t.Run("dedicated directory is granted with the binary", func(t *testing.T) {
		tool := touch(filepath.Join(opt, "whisper-cli"))
		ro, err := toolReadOnly(tool, state, home)
		require.NoError(t, err)
		assert.Equal(t, []string{tool, opt}, ro)
	})
	t.Run("symlinked tool grants the real location", func(t *testing.T) {
		link := filepath.Join(root, "whisper-link")
		require.NoError(t, os.Symlink(filepath.Join(opt, "whisper-cli"), link))
		ro, err := toolReadOnly(link, state, home)
		require.NoError(t, err)
		assert.Equal(t, []string{filepath.Join(opt, "whisper-cli"), link, opt}, ro)
	})
	t.Run("system tool grants only the binary", func(t *testing.T) {
		if _, err := os.Stat("/usr/bin/env"); err != nil {
			t.Skip("no /usr/bin/env")
		}
		// /usr/bin/env may itself be a symlink (e.g. to a coreutils
		// multicall binary); then the real binary is granted as well.
		want := []string{"/usr/bin/env"}
		if real, err := filepath.EvalSymlinks("/usr/bin/env"); err == nil && real != "/usr/bin/env" {
			want = []string{real, "/usr/bin/env"}
			if d := filepath.Dir(real); !strings.HasPrefix(d, "/usr/") && !strings.HasPrefix(d, "/lib") {
				t.Skip("env resolves outside system dirs on this host")
			}
		}
		ro, err := toolReadOnly("/usr/bin/env", state, home)
		require.NoError(t, err)
		assert.Equal(t, want, ro)
	})
	t.Run("tool under home is refused", func(t *testing.T) {
		_, err := toolReadOnly(touch(filepath.Join(home, "bin", "ffmpeg")), state, home)
		assert.ErrorContains(t, err, "under the home directory")
	})
	t.Run("tool directory holding the state is refused", func(t *testing.T) {
		tool := touch(filepath.Join(root, "var", "ffmpeg"))
		_, err := toolReadOnly(tool, state, home)
		assert.ErrorContains(t, err, "state directory")
	})
	t.Run("state under a system path is refused", func(t *testing.T) {
		assert.ErrorContains(t, checkStateOutside("/usr/local/wn-state", systemReadOnly), "state directory")
		assert.NoError(t, checkStateOutside("/var/lib/whisper-network", append(systemReadOnly, childSystemFiles...)))
	})
}

// The worker refuses to start when a tool's directory would expose the
// home directory to its children.
func TestWorkerRefusesToolUnderHome(t *testing.T) {
	h := newHarness(t)
	real, err := filepath.EvalSymlinks(h.fakes.WhisperCLI)
	require.NoError(t, err)
	t.Setenv("HOME", filepath.Dir(filepath.Dir(real)))
	_, done := h.run()
	err = waitDone(t, done)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "under the home directory")
	assert.Empty(t, h.srv.Workers(), "it never registered")
}

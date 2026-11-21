// Package workertest provides test doubles for running the worker without a
// real server, whisper-cli, ffmpeg or yt-dlp:
//
//   - Server, an httptest server that speaks the worker API of DESIGN.md
//     §6.2 with injectable faults (cut downloads, failing uploads, cancelled
//     leases, paused or rejected workers);
//   - fake whisper-cli, ffmpeg and yt-dlp executables, which are the test binary
//     itself started under another name.
//
// A package that runs the worker calls Main from its TestMain. The same test
// binary then serves three roles: the tests, the sandbox-exec wrapper that
// the worker re-executes itself as, and the fake tools (selected by the base
// name of argv[0], which InstallFakes arranges with symlinks).
package workertest

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/deweysasser/whisper-network/internal/sandbox"
)

// Tool names of the fakes.
const (
	WhisperName = "whisper-cli"
	FFmpegName  = "ffmpeg"
	YtDlpName   = "yt-dlp"
)

// Main dispatches the test binary's roles and otherwise runs the tests. Call
// it from TestMain: func TestMain(m *testing.M) { workertest.Main(m) }.
func Main(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == sandbox.SubcommandName {
		if err := sandbox.ExecArgs(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(126)
		}
		os.Exit(0)
	}
	switch filepath.Base(os.Args[0]) {
	case WhisperName:
		os.Exit(fakeWhisper(os.Args[1:]))
	case FFmpegName:
		os.Exit(fakeFFmpeg(os.Args[1:]))
	case YtDlpName:
		os.Exit(fakeYtDlp(os.Args[1:]))
	}
	os.Exit(m.Run())
}

// Fakes are the paths of the installed fake tools.
type Fakes struct {
	Dir        string
	WhisperCLI string
	FFmpeg     string
	YtDlp      string
}

// InstallFakes links whisper-cli, ffmpeg and yt-dlp to the running test binary in a
// fresh directory. The test binary must use Main in its TestMain.
func InstallFakes(t testing.TB) Fakes {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	f := Fakes{Dir: dir, WhisperCLI: filepath.Join(dir, WhisperName), FFmpeg: filepath.Join(dir, FFmpegName),
		YtDlp: filepath.Join(dir, YtDlpName)}
	for _, p := range []string{f.WhisperCLI, f.FFmpeg, f.YtDlp} {
		if err := os.Symlink(self, p); err != nil {
			t.Skipf("cannot create fake tool symlink: %v", err)
		}
	}
	return f
}

// Environment variables that steer the fakes. Pass them to the worker's
// children with Config.ChildEnv, built with WhisperMode and FFmpegMode.
const (
	envWhisper = "WN_FAKE_WHISPER"
	envFFmpeg  = "WN_FAKE_FFMPEG"
	envSegment = "WN_FAKE_SEGMENT_MS"
)

// Behaviour modes of the fakes.
const (
	ModeOK        = ""          // succeed
	ModeCrash     = "crash"     // print an error to stderr and exit 3
	ModeHang      = "hang"      // sleep for an hour
	ModeBadJSON   = "badjson"   // whisper-cli: write unparsable JSON
	ModeNoJSON    = "nojson"    // whisper-cli: exit 0 without output
	ModeNoVersion = "noversion" // whisper-cli: --version unsupported
	ModeInvalid   = "invalid"   // ffmpeg: "Invalid data found when processing input"
	ModeForkTree  = "forktree"  // whisper-cli: start a hanging grandchild, print its pid on stderr, hang
)

// SymlinkMode makes a fake replace its output with a symlink to target.
func SymlinkMode(target string) string { return "symlink:" + target }

// FastExit is a child environment entry for race-instrumented test binaries:
// the race runtime otherwise sleeps a second at every exit (GORACE
// atexit_sleep_ms defaults to 1000), so each fake tool run would cost one.
const FastExit = "GORACE=atexit_sleep_ms=0"

// SleepMode makes a fake sleep for d before behaving normally.
func SleepMode(d string) string { return "sleep:" + d }

// WhisperMode returns the env entry selecting the fake whisper-cli's mode.
func WhisperMode(mode string) string { return envWhisper + "=" + mode }

// FFmpegMode returns the env entry selecting the fake ffmpeg's mode.
func FFmpegMode(mode string) string { return envFFmpeg + "=" + mode }

// SegmentEvery returns the env entry setting the fake whisper-cli's segment
// length in milliseconds (default 5000).
func SegmentEvery(ms int) string { return fmt.Sprintf("%s=%d", envSegment, ms) }

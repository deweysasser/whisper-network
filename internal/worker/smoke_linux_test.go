//go:build linux

package worker

import (
	"bytes"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFetchSandboxRealTools runs the installed yt-dlp and deno (skipped when
// absent) under the real fetch child sandbox and default limits: the
// sandbox-exec of this test binary, Landlock, seccomp and rlimits included.
func TestFetchSandboxRealTools(t *testing.T) {
	ytdlp, err := exec.LookPath("yt-dlp")
	if err != nil {
		t.Skip("yt-dlp is not installed")
	}
	_, derr := exec.LookPath("deno")
	h := downloaderHarness(t)
	h.cfg.YtDlp = ytdlp
	h.cfg.Limits = Limits{}            // the real defaults (no RLIMIT_AS)
	h.cfg.ProbeDownloaderSandbox = nil // the real probe
	var out bytes.Buffer
	err = FetchSandboxSmoke(t.Context(), h.cfg, derr == nil, &out)
	t.Log(out.String())
	require.NoError(t, err)
	require.Contains(t, out.String(), "ok: yt-dlp --version")
	if derr != nil {
		t.Log("deno is not installed; only yt-dlp ran")
	} else {
		require.Contains(t, out.String(), "ok: deno eval")
	}
}

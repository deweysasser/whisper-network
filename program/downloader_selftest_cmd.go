package program

import (
	"context"
	"os"
	"runtime"

	"github.com/rs/zerolog"

	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/worker"
)

// DownloaderSelftestCmd is the hidden subcommand the downloader image runs
// at build time: the installed yt-dlp and deno under the exact sandbox and
// limits of a fetch child (worker.FetchSandboxSmoke).
type DownloaderSelftestCmd struct {
	YtDlp string `name:"yt-dlp" default:"yt-dlp" help:"yt-dlp binary"`
	State string `type:"path" help:"Directory for the run's work directories (default: a temporary directory)"`
	Deno  bool   `default:"true" negatable:"" help:"Require deno"`
}

// Run runs the self-test and fails if a tool does not run sandboxed.
func (c *DownloaderSelftestCmd) Run(ctx context.Context, logger zerolog.Logger) error {
	if err := workerSupported(runtime.GOOS); err != nil {
		return err
	}
	state := c.State
	if state == "" {
		dir, err := os.MkdirTemp("", "wn-downloader-selftest-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		state = dir
	}
	return worker.FetchSandboxSmoke(ctx, worker.Config{StateDir: state, YtDlp: c.YtDlp, Version: Version,
		Memory: 1 << 40, Logger: obs.NewLogger(logger, obs.ModeWorker)}, c.Deno, os.Stdout)
}

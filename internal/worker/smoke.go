package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/client"
)

// smokeTimeout bounds each smoke child.
const smokeTimeout = 2 * time.Minute

// FetchSandboxSmoke checks that the real yt-dlp and deno run under the exact
// sandbox of a fetch child, as a downloader would start them: it starts the
// egress proxy, resolves the tools and their read-only paths, probes the
// downloader sandbox, then runs "deno eval" and "yt-dlp --version" each with
// fetchChildSpec (filesystem confinement, TCP connect to the proxy port
// only, the default resource limits) and the fetch child's environment.
// It reports each step to out. cfg is a downloader's Config; Client may be
// nil.
//
// On Linux it fails unless the probe reports the full sandbox, since that is
// what the image is for. requireDeno makes a missing deno an error.
func FetchSandboxSmoke(ctx context.Context, cfg Config, requireDeno bool, out io.Writer) error {
	cfg.Downloader = true
	if cfg.Client == nil {
		// Nothing talks to a server; the worker just insists on a client.
		cl, err := client.New("http://127.0.0.1:1")
		if err != nil {
			return err
		}
		cfg.Client = cl
	}
	w, err := New(cfg)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(w.workRoot(), 0o700); err != nil {
		return err
	}
	if err := w.shareWithChildren(); err != nil {
		return err
	}
	stop, err := w.startEgress(ctx)
	if err != nil {
		return err
	}
	defer stop()
	if err := w.resolveDownloaderTools(ctx); err != nil {
		return err
	}
	w.facts.Slots = w.cfg.Slots
	fmt.Fprintf(out, "yt-dlp %s at %s\n", w.tools.ytdlpVersion, w.tools.ytdlp)
	if w.tools.deno == "" && requireDeno {
		return errors.New("deno not found or not usable in the fetch sandbox")
	}
	res, err := w.cfg.ProbeDownloaderSandbox(ctx, w.proxy.Port())
	if err != nil {
		return fmt.Errorf("downloader sandbox probe: %w", err)
	}
	fmt.Fprintf(out, "sandbox level %s (landlock ABI %d), open: %v\n", res.Level, res.LandlockABI, res.Open())
	if runtime.GOOS == "linux" && res.Level != api.SandboxFull {
		return fmt.Errorf("downloader sandbox is %s, not full", res.Level)
	}

	type step struct {
		name string
		argv []string
		want string
	}
	steps := []step{{"yt-dlp --version", []string{w.tools.ytdlp, "--version"}, w.tools.ytdlpVersion}}
	if w.tools.deno != "" {
		steps = append([]step{{"deno eval", []string{w.tools.deno, "eval", "console.log(6 * 7)"}, "42"}}, steps...)
	}
	for _, s := range steps {
		got, err := w.smokeRun(ctx, s.argv)
		if err != nil {
			return fmt.Errorf("%s under the fetch sandbox: %w", s.name, err)
		}
		if !strings.Contains(got, s.want) {
			return fmt.Errorf("%s under the fetch sandbox printed %q, want %q", s.name, got, s.want)
		}
		fmt.Fprintf(out, "ok: %s under the fetch sandbox: %s\n", s.name, strings.TrimSpace(got))
	}
	return nil
}

// smokeRun runs argv as a fetch child in a fresh work directory and returns
// its stdout.
func (w *Worker) smokeRun(ctx context.Context, argv []string) (string, error) {
	dir, err := os.MkdirTemp(w.workRoot(), "smoke-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(dir)
	if err := w.giveToChild(dir); err != nil {
		return "", err
	}
	var stdout bytes.Buffer
	res, err := w.runChild(ctx, childRun{argv: argv, spec: w.fetchChildSpec(dir, w.proxy.Port()), dir: dir,
		timeout: smokeTimeout, stdout: &stdout, env: ytdlpEnv})
	if err != nil {
		return "", fmt.Errorf("%w; stderr: %s", err, res.stderr)
	}
	return stdout.String(), nil
}

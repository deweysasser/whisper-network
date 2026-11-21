package sandbox

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"github.com/deweysasser/whisper-network/internal/api"
)

// ProbeResult is the measured strength of the sandbox on this machine.
type ProbeResult struct {
	// Level is what the worker reports to the server as its sandbox_level.
	Level api.SandboxLevel
	// LandlockABI is the kernel's Landlock ABI version, 0 when absent.
	LandlockABI int
	// FSBlocked says the forbidden file was unreadable. NetBlocked says
	// every network and IPC check held: each escape refused and, for
	// [ProbeDownloader], the allowed connect open under Landlock ABI 4+.
	FSBlocked, NetBlocked bool
	// Checks maps each check of [Checks] to its outcome.
	Checks map[string]string
}

// Blocked lists the escapes that were refused, in [DownloaderChecks] order.
// A check whose protocol the kernel lacks counts as refused.
// [CheckTCPConnectAllowed] is not an escape and never appears.
func (r ProbeResult) Blocked() []string { return r.filter(true) }

// Open lists the checks that did not hold, in [DownloaderChecks] order:
// escapes that succeeded, and [CheckTCPConnectAllowed] when the allowed
// connect failed.
func (r ProbeResult) Open() []string { return r.filter(false) }

func (r ProbeResult) filter(held bool) []string {
	var out []string
	for _, c := range DownloaderChecks {
		o, ok := r.Checks[c]
		if !ok || isHeld(c, o) != held || (held && c == CheckTCPConnectAllowed) {
			continue
		}
		out = append(out, c)
	}
	return out
}

// isHeld reports whether check c had the outcome the sandbox wants: refused
// (or unsupported) for an escape, open for [CheckTCPConnectAllowed].
func isHeld(c, outcome string) bool {
	if c == CheckTCPConnectAllowed {
		return outcome == OutcomeOpen
	}
	return outcome == OutcomeBlocked || outcome == OutcomeUnsupported
}

// Probe measures what the sandbox really enforces by running a self-test child
// under the spec of a prepare task (read-only input directory, read-write
// work directory, no network). The child tries every escape in [Checks]: read
// a file outside the allowed paths, reach local TCP, MPTCP and UDP
// listeners, connect to pathname and abstract Unix sockets, send a datagram
// from a socketpair to a pathname Unix socket, and signal its parent. self is the path of a binary whose sandbox-exec subcommand calls
// [ExecArgs]. The level reflects observed behaviour, not the kernel version:
// "full" only when every check held.
func Probe(ctx context.Context, self string) (ProbeResult, error) {
	if runtime.GOOS != "linux" {
		return ProbeResult{Level: api.SandboxNone}, nil
	}
	out, err := runProbe(ctx, self, nil, "")
	if err != nil {
		return ProbeResult{}, err
	}
	return interpret(out)
}

// ProbeDownloader measures the sandbox of a fetch task: the spec of [Probe]
// with [Spec.ConnectPorts] set to proxyPort. On top of the checks of [Probe]
// (tcp_connect now targets a listener on another loopback port) the child
// must reach proxyPort ([CheckTCPConnectAllowed]). The level is "full" only
// when every escape is refused, the allowed connect succeeds and the
// Landlock ABI is at least 4 (TCP connect rules); "fs-only" when at least
// the filesystem is confined.
//
// The connect to proxyPort goes to 127.0.0.1. When nothing listens there
// the probe listens on it itself; when the port is taken (the downloader's
// own proxy, already running) the child connects to that listener and
// closes at once. proxyPort 0 uses a fresh port of the probe's own.
func ProbeDownloader(ctx context.Context, self string, proxyPort uint16) (ProbeResult, error) {
	if runtime.GOOS != "linux" {
		return ProbeResult{Level: api.SandboxNone}, nil
	}
	allowed := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(proxyPort)))
	lis, err := acceptAndClose("tcp", allowed)
	switch {
	case err == nil:
		defer lis.Close()
		allowed = lis.Addr().String()
	case proxyPort != 0 && errors.Is(err, syscall.EADDRINUSE):
		// The proxy itself listens there.
	default:
		return ProbeResult{}, err
	}
	ap, err := netip.ParseAddrPort(allowed)
	if err != nil {
		return ProbeResult{}, err
	}
	out, err := runProbe(ctx, self, []uint16{ap.Port()}, allowed)
	if err != nil {
		return ProbeResult{}, err
	}
	return interpretDownloader(out)
}

// runProbe sets up the self-test targets, runs the self-test child under the
// probe spec (with connectPorts, and allowedTCP as the extra target when
// set) and returns its report.
func runProbe(ctx context.Context, self string, connectPorts []uint16, allowedTCP string) ([]byte, error) {
	dir, err := os.MkdirTemp("", "wn-probe-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)

	allowedDir := filepath.Join(dir, "allowed")
	rwDir := filepath.Join(dir, "work")
	allowed := filepath.Join(allowedDir, "ok")
	forbidden := filepath.Join(dir, "secret")
	for _, d := range []string{allowedDir, rwDir} {
		if err := os.Mkdir(d, 0o700); err != nil {
			return nil, err
		}
	}
	for _, f := range []string{allowed, forbidden} {
		if err := os.WriteFile(f, []byte("probe"), 0o600); err != nil {
			return nil, err
		}
	}

	var nonce [8]byte
	_, _ = rand.Read(nonce[:])
	abstract := "wn-probe-" + hex.EncodeToString(nonce[:])
	unixPath := filepath.Join(dir, "probe.sock")
	tcp, err := acceptAndClose("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	defer tcp.Close()
	udp, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("probe listener: %w", err)
	}
	defer udp.Close()
	ux, err := acceptAndClose("unix", unixPath)
	if err != nil {
		return nil, err
	}
	defer ux.Close()
	abs, err := acceptAndClose("unix", "@"+abstract)
	if err != nil {
		return nil, err
	}
	defer abs.Close()
	dgramPath := filepath.Join(dir, "probe.dgram")
	dgram, err := net.ListenPacket("unixgram", dgramPath)
	if err != nil {
		return nil, fmt.Errorf("probe listener: %w", err)
	}
	defer dgram.Close()

	spec := Spec{ReadOnly: []string{allowedDir}, ReadWrite: []string{rwDir}, DenyNet: true, ConnectPorts: connectPorts}
	args := append([]string{SubcommandName}, spec.Args()...)
	args = append(args, "--"+flagSelfTest, "--", forbidden, allowed,
		tcp.Addr().String(), udp.LocalAddr().String(), unixPath, abstract, dgramPath)
	if allowedTCP != "" {
		args = append(args, allowedTCP)
	}
	cmd := exec.CommandContext(ctx, self, args...)
	cmd.Env = []string{} // the self-test needs nothing from the worker's environment
	out, err := cmd.Output()
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return nil, fmt.Errorf("sandbox self-test failed: %w: %s", err, ee.Stderr)
		}
		return nil, fmt.Errorf("sandbox self-test: %w", err)
	}
	return out, nil
}

// acceptAndClose listens on addr and closes every accepted connection.
func acceptAndClose(network, addr string) (net.Listener, error) {
	lis, err := net.Listen(network, addr)
	if err != nil {
		return nil, fmt.Errorf("probe listener: %w", err)
	}
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	return lis, nil
}

// interpret turns the child's JSON report into a ProbeResult.
func interpret(out []byte) (ProbeResult, error) {
	res, err := decodeReport(out, Checks)
	if err != nil {
		return ProbeResult{}, err
	}
	res.Level = level(res.FSBlocked, res.NetBlocked)
	return res, nil
}

// interpretDownloader turns the report of a downloader self-test into a
// ProbeResult. Network confinement counts only when Landlock enforced the
// connect allowlist (ABI 4+) and the allowed port was reachable.
func interpretDownloader(out []byte) (ProbeResult, error) {
	res, err := decodeReport(out, DownloaderChecks)
	if err != nil {
		return ProbeResult{}, err
	}
	res.NetBlocked = res.NetBlocked && res.LandlockABI >= 4
	res.Level = level(res.FSBlocked, res.NetBlocked)
	return res, nil
}

// decodeReport parses a self-test report that must cover checks, and sets
// FSBlocked and NetBlocked (every check but fs_read held).
func decodeReport(out []byte, checks []string) (ProbeResult, error) {
	var rep selfTestReport
	if err := json.Unmarshal(out, &rep); err != nil {
		return ProbeResult{}, fmt.Errorf("sandbox self-test output %q: %w", out, err)
	}
	if !rep.ControlOK {
		return ProbeResult{}, errors.New("sandbox self-test inconclusive: allowed file was unreadable")
	}
	var missing []string
	for _, c := range checks {
		if _, ok := rep.Checks[c]; !ok {
			missing = append(missing, c)
		}
	}
	if len(missing) > 0 {
		return ProbeResult{}, fmt.Errorf("sandbox self-test incomplete: no result for %s", strings.Join(missing, ", "))
	}
	res := ProbeResult{LandlockABI: rep.ABI, Checks: rep.Checks}
	res.FSBlocked = isHeld(CheckFSRead, rep.Checks[CheckFSRead])
	res.NetBlocked = !slices.ContainsFunc(checks, func(c string) bool {
		return c != CheckFSRead && !isHeld(c, rep.Checks[c])
	})
	return res, nil
}

func level(fsBlocked, netBlocked bool) api.SandboxLevel {
	switch {
	case fsBlocked && netBlocked:
		return api.SandboxFull
	case fsBlocked:
		return api.SandboxFSOnly
	}
	return api.SandboxNone
}

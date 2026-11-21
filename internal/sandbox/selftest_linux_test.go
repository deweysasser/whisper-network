//go:build linux

package sandbox

import (
	"context"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
)

// portOf returns the port of a "host:port" address.
func portOf(t *testing.T, addr string) uint16 {
	t.Helper()
	ap, err := netip.ParseAddrPort(addr)
	require.NoError(t, err)
	return ap.Port()
}

// downloaderSpec is the shape of a fetch task's spec: system libraries
// read-only, one read-write directory, TCP connect to port only.
func downloaderSpec(t *testing.T, port uint16) Spec {
	return Spec{ReadOnly: systemRO, ReadWrite: []string{t.TempDir(), "/dev/null"}, DenyNet: true,
		ConnectPorts: []uint16{port}}
}

func TestSelfTestConnectAllowlist(t *testing.T) {
	requireLandlock(t, 4)
	allowed, other := listen(t), listen(t)
	spec := downloaderSpec(t, portOf(t, allowed))

	assert.Equal(t, "ok", runHelperUnder(t, Spec{}, "dial", other), "control: other port reachable unconfined")
	assert.Equal(t, "ok", runHelperUnder(t, spec, "dial", allowed), "allowed port connects")
	assert.Equal(t, "ok", runHelperUnder(t, spec, "dial-nonblock", allowed), "SOCK_NONBLOCK|SOCK_CLOEXEC still a TCP socket")
	assert.Equal(t, "denied", runHelperUnder(t, spec, "dial", other), "other port refused (EACCES)")
	assert.Equal(t, "ok", runHelperUnder(t, Spec{}, "tfo", other), "control: MSG_FASTOPEN connects unconfined")
	assert.Equal(t, "ok", runHelperUnder(t, spec, "tfo", allowed), "MSG_FASTOPEN to the allowed port")
	assert.Equal(t, "denied", runHelperUnder(t, spec, "tfo", other), "MSG_FASTOPEN to another port: Landlock checks it too")
	assert.Equal(t, "denied", runHelperUnder(t, spec, "bind", "127.0.0.1:0"), "bind refused")
	assert.Equal(t, "denied", runHelperUnder(t, spec, "mptcp", allowed), "MPTCP refused even to the allowed port")
	assert.Equal(t, "denied", runHelperUnder(t, spec, "udp", udpListener(t)), "UDP refused")
	assert.Equal(t, "denied", runHelperUnder(t, spec, "udp", "127.0.0.1:"+strconv.Itoa(int(portOf(t, allowed)))), "UDP refused on the allowed port number")
	assert.Equal(t, "denied", runHelperUnder(t, spec, "unix", unixListener(t, filepath.Join(t.TempDir(), "s"))), "unix refused")
	assert.Equal(t, "denied", runHelperUnder(t, spec, "unix", unixListener(t, "@wn-allowlist-"+strconv.Itoa(os.Getpid()))), "abstract unix refused")
	assert.Equal(t, "denied", runHelperUnder(t, spec, "socketpair"), "socketpair refused")
	assert.Equal(t, "denied", runHelperUnder(t, spec, "raw"), "raw socket refused")
	assert.Equal(t, "denied", runHelperUnder(t, spec, "netlink"), "netlink refused")
	assert.Equal(t, "denied", runHelperUnder(t, spec, "sctp"), "SOCK_STREAM with another protocol refused")
	assert.Equal(t, "denied", runHelperUnder(t, spec, "io_uring"), "io_uring refused")
	assert.Equal(t, "denied", runHelperUnder(t, spec, "kill-parent"), "signals still scoped")
}

func TestSelfTestConnectAllowlistIPv6(t *testing.T) {
	requireLandlock(t, 4)
	lis, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	}
	t.Cleanup(func() { lis.Close() })
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	other := listen(t)
	addr := lis.Addr().String()
	spec := downloaderSpec(t, portOf(t, addr))
	assert.Equal(t, "ok", runHelperUnder(t, spec, "dial", addr), "AF_INET6 to the allowed port")
	_, otherPort, _ := net.SplitHostPort(other)
	assert.Equal(t, "denied", runHelperUnder(t, spec, "dial", "[::1]:"+otherPort), "AF_INET6 to another port")
}

func TestProbeDownloader(t *testing.T) {
	self, err := os.Executable()
	require.NoError(t, err)
	for name, port := range map[string]uint16{"own listener": 0, "busy port": portOf(t, listen(t))} {
		t.Run(name, func(t *testing.T) {
			res, err := ProbeDownloader(context.Background(), self, port)
			require.NoError(t, err)
			t.Logf("probe: level=%s abi=%d blocked=%v open=%v checks=%v", res.Level, res.LandlockABI,
				res.Blocked(), res.Open(), res.Checks)
			assert.Len(t, res.Checks, len(DownloaderChecks))
			switch abi := landlockABI(); {
			case abi >= 4:
				assert.Equal(t, api.SandboxFull, res.Level, "open: %v", res.Open())
				assert.Empty(t, res.Open())
			case abi >= 1:
				assert.Equal(t, api.SandboxFSOnly, res.Level)
			default:
				assert.Equal(t, api.SandboxNone, res.Level)
			}
		})
	}
}

func TestProbeDownloaderFailsOnBadBinary(t *testing.T) {
	_, err := ProbeDownloader(context.Background(), "/bin/false", 0)
	assert.Error(t, err)
}

func TestSeccompInetStreamFilterShape(t *testing.T) {
	for _, r := range []seccompRules{{denyNet: true, inetStream: true}, {denyNet: true, inetStream: true, signalPid: 1}} {
		prog, err := buildSeccomp(r)
		require.NoError(t, err)
		assert.Less(t, len(prog), 255)
	}
}

package sandbox

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// Names of the self-test checks, in report order. Each is an operation the
// sandbox of a prepare task must refuse.
const (
	CheckFSRead          = "fs_read"           // read a file outside the allowed paths
	CheckTCPConnect      = "tcp_connect"       // TCP connect to a local listener
	CheckMPTCPConnect    = "mptcp_connect"     // the same over IPPROTO_MPTCP
	CheckUDPSend         = "udp_send"          // sendto a local UDP socket
	CheckUnixConnect     = "unix_connect"      // connect to a pathname Unix socket outside the allowed paths
	CheckAbstractConnect = "abstract_connect"  // connect to an abstract Unix socket
	CheckUnixDgramSendto = "unix_dgram_sendto" // socketpair(AF_UNIX, SOCK_DGRAM), then sendto a pathname socket outside the allowed paths
	CheckSignalParent    = "signal_parent"     // kill(parent, 0): signal a process outside the sandbox

	// CheckTCPConnectAllowed is the one check that must succeed: a TCP
	// connect to a port of [Spec.ConnectPorts]. Only [ProbeDownloader]
	// runs it.
	CheckTCPConnectAllowed = "tcp_connect_allowed"
	// CheckTCPListen is listen(2) on an unbound TCP socket (an autobind to
	// an ephemeral port). Only [ProbeDownloader] runs it: elsewhere no
	// socket can be created at all.
	CheckTCPListen = "tcp_listen"
)

// Checks lists every self-test check.
var Checks = []string{CheckFSRead, CheckTCPConnect, CheckMPTCPConnect, CheckUDPSend,
	CheckUnixConnect, CheckAbstractConnect, CheckUnixDgramSendto, CheckSignalParent}

// DownloaderChecks lists the checks of a downloader self-test: every check of
// [Checks], where tcp_connect targets a port outside the allowlist, plus
// [CheckTCPListen] and [CheckTCPConnectAllowed].
var DownloaderChecks = append(append([]string(nil), Checks...), CheckTCPListen, CheckTCPConnectAllowed)

// Outcomes of a check.
const (
	OutcomeBlocked     = "blocked"     // refused with EACCES or EPERM
	OutcomeOpen        = "open"        // the operation succeeded: an escape
	OutcomeUnsupported = "unsupported" // the kernel lacks the protocol; nothing to escape through
)

// selfTestReport is what the self-test child tells the prober.
type selfTestReport struct {
	// ControlOK is true when the file that should stay readable was readable.
	ControlOK bool `json:"control_ok"`
	// ABI is the Landlock ABI version seen by the child, 0 if none.
	ABI int `json:"landlock_abi"`
	// Checks maps each check name to its outcome: one of the Outcome
	// constants, or "error: …" for an unexpected failure.
	Checks map[string]string `json:"checks"`
}

// selfTestTargets are the self-test's arguments, in order.
type selfTestTargets struct {
	forbidden, allowed, tcp, udp, unixPath, abstract, dgramPath string
	// allowedTCP, when set, is a listener on a port of the spec's
	// ConnectPorts, for [CheckTCPConnectAllowed].
	allowedTCP string
}

// runSelfTest applies the spec to this process, then tries the operations the
// sandbox should forbid. inv.Argv is {forbiddenFile, allowedFile, tcpAddr,
// udpAddr, unixSocketPath, abstractSocketName, unixDgramSocketPath
// [allowedTCPAddr]}. The report goes to stdout as one JSON object.
func runSelfTest(inv Invocation) error {
	if len(inv.Argv) != 7 && len(inv.Argv) != 8 {
		return errors.New("sandbox-exec: selftest wants: forbiddenFile allowedFile tcpAddr udpAddr unixSocket abstractSocket unixDgramSocket [allowedTCPAddr]")
	}
	a := inv.Argv
	tg := selfTestTargets{forbidden: a[0], allowed: a[1], tcp: a[2], udp: a[3], unixPath: a[4], abstract: a[5], dgramPath: a[6]}
	if len(a) == 8 {
		tg.allowedTCP = a[7]
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	warnings, err := apply(inv.Spec, exe)
	if err != nil {
		return fmt.Errorf("sandbox-exec: %w", err)
	}
	_ = warnings // a degraded sandbox is exactly what the probe measures

	rep := selfTestReport{ABI: landlockABI(), Checks: map[string]string{}}
	_, err = os.ReadFile(tg.allowed)
	rep.ControlOK = err == nil
	_, err = os.ReadFile(tg.forbidden)
	rep.Checks[CheckFSRead] = outcome(err)
	runNetChecks(tg, rep.Checks)
	return json.NewEncoder(os.Stdout).Encode(rep)
}

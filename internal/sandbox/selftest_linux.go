//go:build linux

package sandbox

import (
	"errors"
	"net/netip"
	"os"

	"golang.org/x/sys/unix"
)

// runNetChecks tries each network and IPC escape with raw syscalls, so the
// result reflects the kernel, not the Go runtime's fallbacks (net.Dialer
// silently retries MPTCP as TCP).
func runNetChecks(tg selfTestTargets, out map[string]string) {
	out[CheckTCPConnect] = outcome(dialInet(unix.SOCK_STREAM, 0, tg.tcp))
	out[CheckMPTCPConnect] = outcome(dialInet(unix.SOCK_STREAM, unix.IPPROTO_MPTCP, tg.tcp))
	out[CheckUDPSend] = outcome(dialInet(unix.SOCK_DGRAM, 0, tg.udp))
	out[CheckUnixConnect] = outcome(dialUnix(tg.unixPath))
	out[CheckAbstractConnect] = outcome(dialUnix("@" + tg.abstract))
	out[CheckUnixDgramSendto] = outcome(sendUnixDgram(tg.dgramPath))
	out[CheckSignalParent] = outcome(unix.Kill(os.Getppid(), 0))
	if tg.allowedTCP != "" {
		out[CheckTCPListen] = outcome(listenTCP())
		out[CheckTCPConnectAllowed] = outcome(dialInet(unix.SOCK_STREAM, 0, tg.allowedTCP))
	}
}

// listenTCP calls listen(2) on a fresh, unbound TCP socket, which binds it
// to an ephemeral port.
func listenTCP() error {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	return unix.Listen(fd, 1)
}

// dialInet connects (stream) or sends one datagram (dgram) to addr.
func dialInet(typ, proto int, addr string) error {
	ap, err := netip.ParseAddrPort(addr)
	if err != nil {
		return err
	}
	fd, err := unix.Socket(unix.AF_INET, typ|unix.SOCK_CLOEXEC, proto)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	sa := &unix.SockaddrInet4{Port: int(ap.Port()), Addr: ap.Addr().As4()}
	if typ == unix.SOCK_DGRAM {
		return unix.Sendto(fd, []byte("probe"), 0, sa)
	}
	return unix.Connect(fd, sa)
}

// dialUnix connects to a Unix stream socket; a leading "@" names an
// abstract socket.
func dialUnix(name string) error {
	fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	return unix.Connect(fd, &unix.SockaddrUnix{Name: name})
}

// sendUnixDgram sends one datagram to the pathname socket name from a
// socketpair: no socket(2) call, and an address sendto(2) resolves itself.
func sendUnixDgram(name string) error {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fds[0])
	defer unix.Close(fds[1])
	return unix.Sendto(fds[0], []byte("probe"), 0, &unix.SockaddrUnix{Name: name})
}

// outcome classifies the result of one attempted escape.
func outcome(err error) string {
	switch {
	case err == nil:
		return OutcomeOpen
	case errors.Is(err, unix.EACCES), errors.Is(err, unix.EPERM):
		return OutcomeBlocked
	case errors.Is(err, unix.EPROTONOSUPPORT), errors.Is(err, unix.EAFNOSUPPORT),
		errors.Is(err, unix.ENOPROTOOPT), errors.Is(err, unix.ESOCKTNOSUPPORT):
		return OutcomeUnsupported
	}
	return "error: " + err.Error()
}

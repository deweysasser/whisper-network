//go:build linux

package sandbox

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// runHelper implements the sandboxed target program used by the tests. It
// prints one line describing the outcome so tests can assert on it.
func runHelper(args []string) int {
	if len(args) < 1 {
		fmt.Println("error: no helper command")
		return 2
	}
	switch args[0] {
	case "read":
		_, err := os.ReadFile(args[1])
		fmt.Println(classify(err))
	case "write":
		fmt.Println(classify(os.WriteFile(args[1], []byte("x"), 0o600)))
	case "dial":
		c, err := net.DialTimeout("tcp", args[1], 2*time.Second)
		if err == nil {
			c.Close()
		}
		fmt.Println(classify(err))
	case "fsize":
		signal.Ignore(syscall.SIGXFSZ)
		fmt.Println(classify(os.WriteFile(args[1], make([]byte, 1<<20), 0o600)))
	case "dial-nonblock":
		// connect(2) on a SOCK_NONBLOCK socket: EINPROGRESS, then the
		// result from SO_ERROR once writable.
		fmt.Println(classify(dialNonblock(args[1])))
	case "tfo":
		// TCP Fast Open: sendto(2) with MSG_FASTOPEN connects without
		// connect(2).
		fmt.Println(classify(sendFastOpen(args[1])))
	case "listen":
		fmt.Println(classify(listenTCP()))
	case "accept":
		// accept(2) on stdin: ENOTSOCK unless a filter refuses it first.
		_, _, err := unix.Accept(0)
		fmt.Println(classify(err))
	case "bind":
		fmt.Println(classify(bindTCP(args[1])))
	case "raw":
		fmt.Println(classify(socketOnly(unix.AF_INET, unix.SOCK_RAW, unix.IPPROTO_ICMP)))
	case "netlink":
		fmt.Println(classify(socketOnly(unix.AF_NETLINK, unix.SOCK_RAW, unix.NETLINK_ROUTE)))
	case "sctp":
		fmt.Println(classify(socketOnly(unix.AF_INET, unix.SOCK_STREAM, unix.IPPROTO_SCTP)))
	case "mptcp":
		fmt.Println(classify(dialInet(unix.SOCK_STREAM, unix.IPPROTO_MPTCP, args[1])))
	case "udp":
		fmt.Println(classify(dialInet(unix.SOCK_DGRAM, 0, args[1])))
	case "unix":
		fmt.Println(classify(dialUnix(args[1])))
	case "socketpair":
		fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
		if err == nil {
			unix.Close(fds[0])
			unix.Close(fds[1])
		}
		fmt.Println(classify(err))
	case "socketpair-dgram":
		fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_DGRAM, 0)
		if err == nil {
			unix.Close(fds[0])
			unix.Close(fds[1])
		}
		fmt.Println(classify(err))
	case "socketpair-escape":
		// socketpair-escape <path>: from a Unix stream pair, try to
		// connect to and send to a pathname socket. "ok" means it got out.
		fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
		if err != nil {
			fmt.Println(classify(err))
			break
		}
		cerr := unix.Connect(fds[0], &unix.SockaddrUnix{Name: args[1]})
		serr := unix.Sendto(fds[0], []byte("x"), 0, &unix.SockaddrUnix{Name: args[1]})
		switch {
		case cerr == nil:
			fmt.Println("ok: connected")
		case serr == nil:
			fmt.Println("ok: sent")
		default:
			fmt.Println("contained")
		}
	case "dgram":
		fmt.Println(classify(sendUnixDgram(args[1])))
	case "unshare":
		// unshare <flags>: unshare(2) with the given flags.
		flags, _ := strconv.ParseUint(args[1], 0, 32)
		fmt.Println(classify(unix.Unshare(int(flags))))
	case "clone-newuser":
		// Start a child in a new user namespace the way os/exec does it,
		// through clone(2).
		cmd := exec.Command("/bin/true")
		cmd.SysProcAttr = &syscall.SysProcAttr{Cloneflags: syscall.CLONE_NEWUSER}
		fmt.Println(classify(cmd.Run()))
	case "fork":
		fmt.Println(classify(exec.Command("/bin/true").Run()))
	case "clone3":
		// A zero-sized clone_args is EINVAL when the call reaches the kernel.
		_, _, errno := unix.Syscall(unix.SYS_CLONE3, 0, 0, 0)
		var err error
		if errno != 0 {
			err = errno
		}
		fmt.Println(classify(err))
	case "setns":
		fmt.Println(classify(unix.Setns(-1, 0)))
	case "kill-parent":
		fmt.Println(classify(unix.Kill(os.Getppid(), 0)))
	case "setpgid":
		// A no-op for a group leader, so only a filter refuses it. (setsid
		// is always EPERM for a group leader, so it proves nothing here.)
		fmt.Println(classify(unix.Setpgid(0, 0)))
	case "io_uring":
		_, _, errno := unix.Syscall(unix.SYS_IO_URING_SETUP, 1, 0, 0)
		var err error
		if errno != 0 {
			err = errno
		}
		// A nil params pointer gives EFAULT when the call is allowed.
		fmt.Println(classify(err))
	case "caps":
		fmt.Println(capLines())
	case "seccomp-signals":
		// Install the signal fallback directly, as on a kernel without
		// Landlock scoping, and report: parent, self, own group.
		if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
			fmt.Println("error:", err)
			return 1
		}
		if err := installSeccomp(seccompRules{signalPid: os.Getpid()}); err != nil {
			fmt.Println("error:", err)
			return 1
		}
		fmt.Println(classify(unix.Kill(os.Getppid(), 0)), classify(unix.Kill(os.Getpid(), 0)),
			classify(unix.Kill(0, 0)), classify(unix.Tgkill(os.Getpid(), unix.Gettid(), 0)))
	case "spawn-tree":
		// Start a middle process that starts a sleeper and exits at once,
		// orphaning it; print the sleeper's pid; then hang or exit.
		self, _ := os.Executable()
		mid := exec.Command(self, "spawn-mid")
		mid.Stdout = os.Stdout
		if err := mid.Run(); err != nil {
			fmt.Println("error:", err)
			return 1
		}
		if len(args) > 1 && args[1] == "hang" {
			time.Sleep(time.Hour)
		}
	case "spawn-mid":
		self, _ := os.Executable()
		sl := exec.Command(self, "sleeper")
		if err := sl.Start(); err != nil {
			fmt.Println("error:", err)
			return 1
		}
		fmt.Println(sl.Process.Pid)
	case "sleeper":
		time.Sleep(time.Hour)
	case "prio":
		fmt.Println(priorities())
	case "limits":
		b, _ := os.ReadFile("/proc/self/limits")
		fmt.Print(string(b))
	default:
		fmt.Println("error: unknown helper command", args[0])
		return 2
	}
	return 0
}

// classify maps an error to a stable word.
func classify(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, syscall.EACCES), errors.Is(err, syscall.EPERM):
		return "denied"
	case errors.Is(err, syscall.EFBIG):
		return "toobig"
	case errors.Is(err, syscall.ENOSYS):
		return "nosys"
	case outcome(err) == OutcomeUnsupported:
		return "unsupported"
	}
	return "error: " + err.Error()
}

// priorities reports nice, scheduling policy and I/O class of this process,
// read from /proc/self/stat (fields 19 and 41) and ioprio_get.
func priorities() string {
	b, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return "error: " + err.Error()
	}
	s := string(b)
	// The command name may contain spaces; fields resume after the last ")".
	f := strings.Fields(s[strings.LastIndexByte(s, ')')+2:])
	// After ")" the first field is state (original field 3).
	nice, policy := f[19-3], f[41-3]
	io, err := ioprioGet()
	if err != nil {
		return "error: " + err.Error()
	}
	return fmt.Sprintf("nice=%s policy=%s ioclass=%d", nice, policy, io>>ioprioClassShift)
}

// capLines returns the capability lines of /proc/self/status.
func capLines() string {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return "error: " + err.Error()
	}
	var out []string
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "Cap") {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}

// inetSockaddr parses addr into a socket address and its family.
func inetSockaddr(addr string) (int, unix.Sockaddr, error) {
	ap, err := netip.ParseAddrPort(addr)
	if err != nil {
		return 0, nil, err
	}
	if ap.Addr().Is4() {
		return unix.AF_INET, &unix.SockaddrInet4{Port: int(ap.Port()), Addr: ap.Addr().As4()}, nil
	}
	return unix.AF_INET6, &unix.SockaddrInet6{Port: int(ap.Port()), Addr: ap.Addr().As16()}, nil
}

func dialNonblock(addr string) error {
	fam, sa, err := inetSockaddr(addr)
	if err != nil {
		return err
	}
	fd, err := unix.Socket(fam, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, unix.IPPROTO_TCP)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if err := unix.Connect(fd, sa); err != nil && !errors.Is(err, unix.EINPROGRESS) {
		return err
	}
	pfd := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLOUT}}
	if _, err := unix.Poll(pfd, 2000); err != nil {
		return err
	}
	if v, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_ERROR); err != nil {
		return err
	} else if v != 0 {
		return unix.Errno(v)
	}
	return nil
}

func sendFastOpen(addr string) error {
	fam, sa, err := inetSockaddr(addr)
	if err != nil {
		return err
	}
	fd, err := unix.Socket(fam, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	return unix.Sendto(fd, []byte("probe"), unix.MSG_FASTOPEN, sa)
}

func bindTCP(addr string) error {
	fam, sa, err := inetSockaddr(addr)
	if err != nil {
		return err
	}
	fd, err := unix.Socket(fam, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	return unix.Bind(fd, sa)
}

func socketOnly(domain, typ, proto int) error {
	fd, err := unix.Socket(domain, typ|unix.SOCK_CLOEXEC, proto)
	if err == nil {
		unix.Close(fd)
	}
	return err
}

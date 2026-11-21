//go:build linux

package sandbox

import (
	"errors"
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// The seccomp filter closes what Landlock leaves open (DESIGN.md §7.4).
// Landlock's network rights cover TCP from ABI 4, pathname Unix sockets from
// ABI 9 and UDP from ABI 10, and never MPTCP, raw or netlink sockets; its
// signal and abstract-socket scoping starts at ABI 6. Rather than chase each
// protocol, a child that must not use the network may not create sockets at
// all, nor socket pairs. A child with a TCP connect allowlist
// (Spec.ConnectPorts) may create plain TCP sockets and nothing else;
// Landlock's connect rules then limit where they go. Confined children also may not create or enter
// namespaces (clone/unshare with CLONE_NEW*, setns; clone3 reports ENOSYS
// so callers fall back to clone). Denied calls fail with an errno; nothing is killed, so a tool that
// probes for a socket degrades the way it would on a host without network.
//
// The filter is a hand-written classic BPF program (no library): it checks
// the architecture, then the syscall number, and for the signal fallback the
// low 32 bits of the first argument (pid_t is 32 bits on every Linux ABI).

// seccompRules selects the parts of the filter.
type seccompRules struct {
	// denyNet makes socket(2) and socketpair(2) fail with EACCES for every
	// family.
	denyNet bool
	// inetStream relaxes denyNet: socket(2) may create AF_INET and
	// AF_INET6 SOCK_STREAM sockets with protocol 0 or IPPROTO_TCP, so
	// Landlock's TCP connect rules decide where they go. Such a socket
	// still may not listen(2) (an unbound socket autobinds without the
	// Landlock bind check) or accept(2), and sendto(2)/sendmsg(2)/
	// sendmmsg(2) with MSG_FASTOPEN (a connect without connect(2)) fail.
	// socketpair(2) may create AF_UNIX SOCK_STREAM pairs (connected to
	// each other only); other pairs are refused.
	inetStream bool
	// signalPid, when non-zero, confines signals to this process and its
	// process group: the fallback for kernels without Landlock signal
	// scoping (ABI < 6). Signals to other processes fail with EPERM.
	signalPid int
}

// errNoSeccompArch reports an architecture the filter does not know.
var errNoSeccompArch = errors.New("seccomp filter not available on this architecture")

// installSeccomp loads the filter for r into every thread of the process
// (TSYNC), so whichever thread calls execve carries it. no_new_privs must
// already be set.
func installSeccomp(r seccompRules) error {
	prog, err := buildSeccomp(r)
	if err != nil {
		return err
	}
	fprog := unix.SockFprog{Len: uint16(len(prog)), Filter: &prog[0]}
	r1, _, errno := unix.Syscall(unix.SYS_SECCOMP, unix.SECCOMP_SET_MODE_FILTER,
		unix.SECCOMP_FILTER_FLAG_TSYNC, uintptr(unsafe.Pointer(&fprog)))
	switch {
	case errno == unix.ENOSYS:
		// No seccomp(2) (pre-3.17): the per-thread prctl still binds the
		// locked thread that will execve.
		if err := unix.Prctl(unix.PR_SET_SECCOMP, unix.SECCOMP_MODE_FILTER,
			uintptr(unsafe.Pointer(&fprog)), 0, 0); err != nil {
			return fmt.Errorf("prctl(PR_SET_SECCOMP): %w", err)
		}
	case errno != 0:
		return fmt.Errorf("seccomp(SET_MODE_FILTER): %w", errno)
	case r1 != 0:
		return fmt.Errorf("seccomp: thread %d could not be synchronized", r1)
	}
	return nil
}

// seccompRulesFor picks the filter parts for spec. abi is the Landlock ABI
// and pid the process the signal fallback (abi < 6) confines signals to.
// TCP sockets are allowed for ConnectPorts only where Landlock can limit
// their connects (ABI 4+); below that the spec fails closed and refuses
// every socket, as plain DenyNet does.
func seccompRulesFor(spec Spec, abi, pid int) seccompRules {
	r := seccompRules{denyNet: spec.DenyNet, inetStream: spec.DenyNet && len(spec.ConnectPorts) > 0 && abi >= 4}
	if abi < 6 {
		r.signalPid = pid
	}
	return r
}

// restrictSeccomp installs the filter a confining spec needs. abi is the
// Landlock ABI; below 6 the filter also confines signals. A kernel or
// architecture without seccomp yields a warning, not an error, like a
// Landlock right the kernel lacks: the self-test measures what holds.
func restrictSeccomp(spec Spec, abi int) ([]string, error) {
	if !spec.Confines() {
		return nil, nil
	}
	err := installSeccomp(seccompRulesFor(spec, abi, os.Getpid()))
	switch {
	case err == nil:
		return nil, nil
	case errors.Is(err, errNoSeccompArch), errors.Is(err, unix.EINVAL), errors.Is(err, unix.ENOSYS):
		return []string{fmt.Sprintf("seccomp unavailable (%v): sockets, signals and process groups not restricted", err)}, nil
	}
	return nil, err
}

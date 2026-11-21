//go:build linux && (amd64 || arm64)

package sandbox

import "golang.org/x/sys/unix"

// Offsets into struct seccomp_data (linux/seccomp.h): int nr; __u32 arch;
// __u64 instruction_pointer; __u64 args[6]. Both supported architectures are
// little-endian, so an argument's low word comes first.
const (
	seccompOffNR      = 0
	seccompOffArch    = 4
	seccompOffArg0Low = 16
	seccompOffArg1Low = 24
	seccompOffArg2Low = 32
	seccompOffArg3Low = 40

	// x32 syscalls on amd64 carry this bit in nr and use different
	// numbers; the filter refuses them all.
	x32SyscallBit = 0x40000000
)

// seccompDeny returns SECCOMP_RET_ERRNO with errno.
func seccompDeny(errno unix.Errno) uint32 {
	return unix.SECCOMP_RET_ERRNO | uint32(errno)&unix.SECCOMP_RET_DATA
}

func bpfStmt(code uint16, k uint32) unix.SockFilter {
	return unix.SockFilter{Code: code, K: k}
}

func bpfJump(code uint16, k uint32, jt, jf uint8) unix.SockFilter {
	return unix.SockFilter{Code: code, Jt: jt, Jf: jf, K: k}
}

const (
	bpfLoadW = unix.BPF_LD | unix.BPF_W | unix.BPF_ABS
	bpfJEQ   = unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K
	bpfJGE   = unix.BPF_JMP | unix.BPF_JGE | unix.BPF_K
	bpfJSET  = unix.BPF_JMP | unix.BPF_JSET | unix.BPF_K
	bpfAND   = unix.BPF_ALU | unix.BPF_AND | unix.BPF_K
	bpfRet   = unix.BPF_RET | unix.BPF_K
)

// Namespace flags. A confined child may not create namespaces: a user
// namespace gives an unprivileged process a full capability set over fresh
// kernel objects, which is attack surface the tools never need.
// CLONE_NEWTIME is valid only for unshare and clone3: in clone(2)'s flags
// its bit lies in the exit-signal byte.
const (
	cloneNewMask   = unix.CLONE_NEWNS | unix.CLONE_NEWCGROUP | unix.CLONE_NEWUTS | unix.CLONE_NEWIPC | unix.CLONE_NEWUSER | unix.CLONE_NEWPID | unix.CLONE_NEWNET
	unshareNewMask = cloneNewMask | unix.CLONE_NEWTIME
)

// buildSeccomp returns the filter program for r on the running architecture.
func buildSeccomp(r seccompRules) ([]unix.SockFilter, error) {
	prog := []unix.SockFilter{
		bpfStmt(bpfLoadW, seccompOffArch),
		bpfJump(bpfJEQ, seccompArch, 1, 0),
		bpfStmt(bpfRet, seccompDeny(unix.EPERM)), // foreign ABI (e.g. int 0x80)
		bpfStmt(bpfLoadW, seccompOffNR),
		bpfJump(bpfJGE, x32SyscallBit, 0, 1),
		bpfStmt(bpfRet, seccompDeny(unix.EPERM)),
	}
	deny := func(nr uintptr, errno unix.Errno) {
		prog = append(prog,
			bpfJump(bpfJEQ, uint32(nr), 0, 1),
			bpfStmt(bpfRet, seccompDeny(errno)))
	}
	// allowArg0 lets syscall nr through only when its first argument is one
	// of vals. The accumulator is clobbered only on the path that returns.
	allowArg0 := func(nr uintptr, vals ...uint32) {
		n := len(vals)
		prog = append(prog,
			bpfJump(bpfJEQ, uint32(nr), 0, uint8(n+3)),
			bpfStmt(bpfLoadW, seccompOffArg0Low))
		for i, v := range vals {
			prog = append(prog, bpfJump(bpfJEQ, v, uint8(n-i), 0))
		}
		prog = append(prog,
			bpfStmt(bpfRet, seccompDeny(unix.EPERM)),
			bpfStmt(bpfRet, unix.SECCOMP_RET_ALLOW))
	}
	// denyArgBits refuses syscall nr when the low word of the argument at
	// offset off has any bit of mask set, and allows it otherwise.
	denyArgBits := func(nr uintptr, off uint32, mask uint32, errno unix.Errno) {
		prog = append(prog,
			bpfJump(bpfJEQ, uint32(nr), 0, 4),
			bpfStmt(bpfLoadW, off),
			bpfJump(bpfJSET, mask, 0, 1),
			bpfStmt(bpfRet, seccompDeny(errno)),
			bpfStmt(bpfRet, unix.SECCOMP_RET_ALLOW))
	}
	// allowTCPSocket lets socket(2) through only for
	// (AF_INET|AF_INET6, SOCK_STREAM [|SOCK_NONBLOCK|SOCK_CLOEXEC],
	// 0|IPPROTO_TCP) and refuses every other socket with EACCES. The
	// kernel reads all three arguments as int, so the low words decide.
	allowTCPSocket := func() {
		const typeFlags = unix.SOCK_NONBLOCK | unix.SOCK_CLOEXEC
		prog = append(prog,
			bpfJump(bpfJEQ, unix.SYS_SOCKET, 0, 11),
			bpfStmt(bpfLoadW, seccompOffArg0Low),
			bpfJump(bpfJEQ, unix.AF_INET, 1, 0),
			bpfJump(bpfJEQ, unix.AF_INET6, 0, 6), // → deny
			bpfStmt(bpfLoadW, seccompOffArg1Low),
			bpfStmt(bpfAND, ^uint32(typeFlags)),
			bpfJump(bpfJEQ, unix.SOCK_STREAM, 0, 3), // → deny
			bpfStmt(bpfLoadW, seccompOffArg2Low),
			bpfJump(bpfJEQ, 0, 2, 0),                // → allow
			bpfJump(bpfJEQ, unix.IPPROTO_TCP, 1, 0), // → allow
			bpfStmt(bpfRet, seccompDeny(unix.EACCES)),
			bpfStmt(bpfRet, unix.SECCOMP_RET_ALLOW))
	}
	// allowUnixStreamPair lets socketpair(2) through only for (AF_UNIX,
	// SOCK_STREAM [|SOCK_NONBLOCK|SOCK_CLOEXEC], 0) and refuses every
	// other pair with EACCES. Such a pair is connected to itself from
	// birth: connect(2) on it fails with EISCONN and a Unix stream socket
	// refuses a destination address on sendto(2)/sendmsg(2), so it
	// reaches nothing but its other end. Runtimes need it for their
	// in-process wakeups (deno's tokio signal driver, Python asyncio).
	// Datagram pairs stay refused: they can sendto(2) any pathname socket.
	allowUnixStreamPair := func() {
		const typeFlags = unix.SOCK_NONBLOCK | unix.SOCK_CLOEXEC
		prog = append(prog,
			bpfJump(bpfJEQ, unix.SYS_SOCKETPAIR, 0, 9),
			bpfStmt(bpfLoadW, seccompOffArg0Low),
			bpfJump(bpfJEQ, unix.AF_UNIX, 0, 5), // → deny
			bpfStmt(bpfLoadW, seccompOffArg1Low),
			bpfStmt(bpfAND, ^uint32(typeFlags)),
			bpfJump(bpfJEQ, unix.SOCK_STREAM, 0, 2), // → deny
			bpfStmt(bpfLoadW, seccompOffArg2Low),
			bpfJump(bpfJEQ, 0, 1, 0), // → allow
			bpfStmt(bpfRet, seccompDeny(unix.EACCES)),
			bpfStmt(bpfRet, unix.SECCOMP_RET_ALLOW))
	}

	// Process containment: the worker kills the child's whole process
	// group on cancel, so no descendant may leave it. io_uring can create
	// sockets and send signals without the syscalls below.
	deny(unix.SYS_SETSID, unix.EPERM)
	deny(unix.SYS_SETPGID, unix.EPERM)
	deny(unix.SYS_IO_URING_SETUP, unix.EPERM)
	deny(unix.SYS_IO_URING_ENTER, unix.EPERM)
	deny(unix.SYS_IO_URING_REGISTER, unix.EPERM)
	// No new namespaces. clone3 passes its flags in memory, out of a
	// filter's reach, so it reports ENOSYS: glibc (2.34+) then falls back
	// to clone(2), whose flags the filter can read. The Go runtime uses
	// clone(2) directly. Threads and fork never set CLONE_NEW* flags.
	deny(unix.SYS_CLONE3, unix.ENOSYS)
	denyArgBits(unix.SYS_CLONE, seccompOffArg0Low, cloneNewMask, unix.EPERM)
	denyArgBits(unix.SYS_UNSHARE, seccompOffArg0Low, unshareNewMask, unix.EPERM)
	deny(unix.SYS_SETNS, unix.EPERM)
	if r.denyNet {
		// socketpair too: an AF_UNIX datagram socket from socketpair can
		// still sendto(2) a pathname address (/dev/log, the journald
		// socket), which Landlock below ABI 9 does not check. The child's
		// inherited descriptors are /dev/null and pipes, not sockets, so
		// with neither call there is no socket to send from. inetStream
		// allows stream pairs only (allowUnixStreamPair), before this
		// catch-all.
		if r.inetStream {
			allowTCPSocket()
			allowUnixStreamPair()
			// Outbound only: listen(2) on an unbound socket autobinds
			// an ephemeral port without Landlock's bind check. A
			// connect autobinds without bind(2), which is refused so
			// stream pairs cannot claim abstract names (Landlock's
			// abstract scoping covers connect, not bind).
			deny(unix.SYS_BIND, unix.EACCES)
			deny(unix.SYS_LISTEN, unix.EACCES)
			deny(unix.SYS_ACCEPT, unix.EACCES)
			deny(unix.SYS_ACCEPT4, unix.EACCES)
			// TCP Fast Open connects from sendto/sendmsg. The flags are
			// an int (sendto, sendmmsg) or unsigned (sendmsg): low word.
			denyArgBits(unix.SYS_SENDTO, seccompOffArg3Low, unix.MSG_FASTOPEN, unix.EACCES)
			denyArgBits(unix.SYS_SENDMSG, seccompOffArg2Low, unix.MSG_FASTOPEN, unix.EACCES)
			denyArgBits(unix.SYS_SENDMMSG, seccompOffArg3Low, unix.MSG_FASTOPEN, unix.EACCES)
		} else {
			deny(unix.SYS_SOCKET, unix.EACCES)
		}
		deny(unix.SYS_SOCKETPAIR, unix.EACCES)
	}
	if pid := r.signalPid; pid > 0 {
		self, group := uint32(pid), uint32(-int32(pid))
		allowArg0(unix.SYS_KILL, self, 0, group)
		allowArg0(unix.SYS_TGKILL, self)
		allowArg0(unix.SYS_RT_SIGQUEUEINFO, self)
		allowArg0(unix.SYS_RT_TGSIGQUEUEINFO, self)
		deny(unix.SYS_TKILL, unix.EPERM)
		deny(unix.SYS_PIDFD_SEND_SIGNAL, unix.EPERM)
	}
	return append(prog, bpfStmt(bpfRet, unix.SECCOMP_RET_ALLOW)), nil
}

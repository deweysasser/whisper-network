package sandbox

import (
	"errors"
	"strconv"
)

// Spec describes how a child process is to be confined. The zero value
// restricts nothing.
type Spec struct {
	// ReadOnly lists files and directories the child may read and execute.
	// Missing paths are ignored. The target executable itself is added
	// automatically.
	ReadOnly []string
	// ReadWrite lists files and directories the child may read and write.
	ReadWrite []string
	// DenyNet cuts the child off from every socket. On Linux a seccomp
	// filter makes socket(2) and socketpair(2) fail with EACCES for all
	// address families (TCP, MPTCP, UDP, raw, netlink, and Unix sockets,
	// pathname or abstract); Landlock additionally denies TCP (ABI 4+),
	// pathname Unix sockets (ABI 9+) and UDP (ABI 10+). socketpair is
	// denied because a datagram socket from it can sendto(2) any pathname
	// socket the Landlock ABI does not cover (e.g. /dev/log). The child
	// inherits only /dev/null and pipes, so it holds no socket to start
	// from.
	DenyNet bool
	// ConnectPorts relaxes DenyNet for a child that must reach one local
	// service (a downloader's egress proxy): it may open TCP connections
	// to these ports, on any address, and nothing else. Valid only with
	// DenyNet. The seccomp filter then lets socket(2) create AF_INET and
	// AF_INET6 SOCK_STREAM sockets with protocol 0 or IPPROTO_TCP (no
	// MPTCP, UDP, raw, Unix, netlink or socketpair), and Landlock (ABI 4+)
	// refuses bind(2) and every TCP connect(2) except to these ports;
	// listen(2), accept(2) and MSG_FASTOPEN sends are refused too. Below
	// ABI 4 Landlock cannot restrict TCP, so the spec fails closed: every
	// socket is refused as under plain DenyNet, with a warning, and
	// [ProbeDownloader] reports less than full.
	ConnectPorts []uint16

	// AddressSpace is RLIMIT_AS in bytes; 0 leaves it unchanged.
	AddressSpace uint64
	// FileSize is RLIMIT_FSIZE in bytes; 0 leaves it unchanged.
	FileSize uint64
	// OpenFiles is RLIMIT_NOFILE; 0 leaves it unchanged.
	OpenFiles uint64
	// Processes and ProcessLimit set RLIMIT_NPROC (Linux), which the
	// kernel checks at fork against every task (process or thread) of the
	// child's real uid, across all PID namespaces, not just the child's.
	//
	// ProcessLimit is the absolute limit, for a child uid dedicated to
	// this program: everything that uid runs is ours, so one number bounds
	// all children of all slots together. Processes is headroom, for a
	// child that shares its uid with other work (a worker not running as
	// root): sandbox-exec sets the limit to the number of tasks the uid runs
	// at launch, as /proc shows them, plus Processes. Several children
	// started under such a snapshot do not share one bound; the caller
	// passes an allowance covering all of its concurrent children so that
	// one child's tasks do not eat another's headroom. ProcessLimit wins
	// when both are set; 0 leaves the limit unchanged.
	Processes    uint64
	ProcessLimit uint64

	// Nice is the nice value to set (1..19); 0 leaves it unchanged.
	Nice int
	// SchedIdle selects the SCHED_IDLE scheduling policy (Linux).
	SchedIdle bool
	// IOPrioIdle selects the idle I/O scheduling class (Linux).
	IOPrioIdle bool

	// UID and GID are the identity the child drops to. They are used only
	// when sandbox-exec runs as root and are ignored otherwise; 0 means
	// unchanged.
	UID, GID int
}

// Confines reports whether the spec asks for any confinement beyond resource
// limits and priority. Confined children also get the process-containment
// rules: they cannot leave their process group, signal processes outside
// the sandbox, or use io_uring.
func (s Spec) Confines() bool {
	return s.RestrictsFS() || s.DenyNet
}

// Validate reports a spec that cannot be applied as written.
func (s Spec) Validate() error {
	if len(s.ConnectPorts) > 0 && !s.DenyNet {
		return errors.New("sandbox: connect ports require deny-net")
	}
	for _, p := range s.ConnectPorts {
		if p == 0 {
			return errors.New("sandbox: connect port 0")
		}
	}
	return nil
}

// RestrictsFS reports whether the spec asks for a filesystem restriction.
// A spec naming no paths leaves the filesystem alone.
func (s Spec) RestrictsFS() bool {
	return len(s.ReadOnly) > 0 || len(s.ReadWrite) > 0
}

// WithLowestPriority returns a copy of s with the lowest CPU and I/O priority
// settings of DESIGN.md §7.4: nice 19, SCHED_IDLE and idle I/O class.
func (s Spec) WithLowestPriority() Spec {
	s.Nice = 19
	s.SchedIdle = true
	s.IOPrioIdle = true
	return s
}

// Flag names understood by sandbox-exec. Args and ParseArgs share them.
const (
	flagRO         = "ro"
	flagRW         = "rw"
	flagDenyNet    = "deny-net"
	flagConnect    = "connect-port"
	flagAS         = "rlimit-as"
	flagFSize      = "rlimit-fsize"
	flagNoFile     = "rlimit-nofile"
	flagNProc      = "nproc"
	flagNProcLimit = "nproc-limit"
	flagNice       = "nice"
	flagSchedIdle  = "sched-idle"
	flagIOPrioIdle = "ioprio-idle"
	flagUID        = "uid"
	flagGID        = "gid"
	flagSelfTest   = "selftest"
)

// Args encodes the spec as sandbox-exec flags (without the trailing "--").
func (s Spec) Args() []string {
	var a []string
	add := func(name, val string) { a = append(a, "--"+name+"="+val) }
	for _, p := range s.ReadOnly {
		add(flagRO, p)
	}
	for _, p := range s.ReadWrite {
		add(flagRW, p)
	}
	if s.DenyNet {
		a = append(a, "--"+flagDenyNet)
	}
	for _, p := range s.ConnectPorts {
		add(flagConnect, strconv.FormatUint(uint64(p), 10))
	}
	if s.AddressSpace > 0 {
		add(flagAS, strconv.FormatUint(s.AddressSpace, 10))
	}
	if s.FileSize > 0 {
		add(flagFSize, strconv.FormatUint(s.FileSize, 10))
	}
	if s.OpenFiles > 0 {
		add(flagNoFile, strconv.FormatUint(s.OpenFiles, 10))
	}
	if s.Processes > 0 {
		add(flagNProc, strconv.FormatUint(s.Processes, 10))
	}
	if s.ProcessLimit > 0 {
		add(flagNProcLimit, strconv.FormatUint(s.ProcessLimit, 10))
	}
	if s.Nice != 0 {
		add(flagNice, strconv.Itoa(s.Nice))
	}
	if s.SchedIdle {
		a = append(a, "--"+flagSchedIdle)
	}
	if s.IOPrioIdle {
		a = append(a, "--"+flagIOPrioIdle)
	}
	if s.UID != 0 {
		add(flagUID, strconv.Itoa(s.UID))
	}
	if s.GID != 0 {
		add(flagGID, strconv.Itoa(s.GID))
	}
	return a
}

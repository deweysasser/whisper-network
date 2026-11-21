package sandbox

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
)

// Invocation is a parsed sandbox-exec command line.
type Invocation struct {
	Spec Spec
	// SelfTest runs the sandbox self-test instead of exec'ing Argv[0];
	// Argv then carries the self-test's arguments.
	SelfTest bool
	// Argv is the target command and its arguments.
	Argv []string
}

type stringList []string

func (l *stringList) String() string     { return fmt.Sprint([]string(*l)) }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }

type portList []uint16

func (l *portList) String() string { return fmt.Sprint([]uint16(*l)) }
func (l *portList) Set(v string) error {
	p, err := strconv.ParseUint(v, 10, 16)
	if err != nil || p == 0 {
		return fmt.Errorf("invalid TCP port %q", v)
	}
	*l = append(*l, uint16(p))
	return nil
}

// ParseArgs parses the arguments that follow "sandbox-exec": the flags
// produced by [Spec.Args], then "--", then the target command.
func ParseArgs(args []string) (Invocation, error) {
	var (
		inv                 Invocation
		as, fs, nf, np, npl uint64
	)
	var ro, rw stringList
	var ports portList
	set := flag.NewFlagSet(SubcommandName, flag.ContinueOnError)
	set.SetOutput(io.Discard)
	set.Var(&ro, flagRO, "read-only path (repeatable)")
	set.Var(&rw, flagRW, "read-write path (repeatable)")
	set.BoolVar(&inv.Spec.DenyNet, flagDenyNet, false, "deny all sockets")
	set.Var(&ports, flagConnect, "TCP port the child may connect to, with --deny-net (repeatable)")
	set.Uint64Var(&as, flagAS, 0, "RLIMIT_AS bytes")
	set.Uint64Var(&fs, flagFSize, 0, "RLIMIT_FSIZE bytes")
	set.Uint64Var(&nf, flagNoFile, 0, "RLIMIT_NOFILE")
	set.Uint64Var(&np, flagNProc, 0, "additional processes and threads (RLIMIT_NPROC headroom)")
	set.Uint64Var(&npl, flagNProcLimit, 0, "absolute RLIMIT_NPROC for a dedicated child uid")
	set.IntVar(&inv.Spec.Nice, flagNice, 0, "nice value")
	set.BoolVar(&inv.Spec.SchedIdle, flagSchedIdle, false, "SCHED_IDLE")
	set.BoolVar(&inv.Spec.IOPrioIdle, flagIOPrioIdle, false, "idle I/O class")
	set.IntVar(&inv.Spec.UID, flagUID, 0, "child uid (root only)")
	set.IntVar(&inv.Spec.GID, flagGID, 0, "child gid (root only)")
	set.BoolVar(&inv.SelfTest, flagSelfTest, false, "run the sandbox self-test")
	if err := set.Parse(args); err != nil {
		return Invocation{}, err
	}
	inv.Spec.ReadOnly, inv.Spec.ReadWrite = ro, rw
	inv.Spec.AddressSpace, inv.Spec.FileSize, inv.Spec.OpenFiles, inv.Spec.Processes = as, fs, nf, np
	inv.Spec.ProcessLimit = npl
	if len(ports) > 0 {
		inv.Spec.ConnectPorts = ports
	}
	inv.Argv = set.Args()
	if len(inv.Argv) == 0 {
		return Invocation{}, errors.New("sandbox-exec: no command given after --")
	}
	if inv.Spec.Nice < 0 || inv.Spec.Nice > 19 {
		return Invocation{}, fmt.Errorf("sandbox-exec: nice %s out of range 0..19", strconv.Itoa(inv.Spec.Nice))
	}
	if err := inv.Spec.Validate(); err != nil {
		return Invocation{}, fmt.Errorf("sandbox-exec: %w", err)
	}
	return inv, nil
}

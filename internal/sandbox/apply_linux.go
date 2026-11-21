//go:build linux

package sandbox

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/landlock-lsm/go-landlock/landlock"
	ll "github.com/landlock-lsm/go-landlock/landlock/syscall"
	"golang.org/x/sys/unix"
)

// apply confines the calling process (and, for the per-thread attributes, the
// locked OS thread that will execve) according to spec. exe is the resolved
// path of the target, which is always granted read+execute when the
// filesystem is restricted.
//
// The order matters. Priority first, while privileges are intact. Landlock
// next, because its rules open paths and that must happen before dropping to
// an unprivileged uid. Capabilities before the uid change, because emptying
// the bounding set needs CAP_SETPCAP. The seccomp filter after everything
// that makes syscalls it might deny. rlimits last, because a small RLIMIT_AS
// or RLIMIT_NOFILE would starve the Go runtime and Landlock itself.
func apply(spec Spec, exe string) (warnings []string, err error) {
	warnings = setPriority(spec)

	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return warnings, fmt.Errorf("prctl(PR_SET_NO_NEW_PRIVS): %w", err)
	}
	// Count the child uid's tasks for RLIMIT_NPROC now: once Landlock is
	// on, /proc is out of reach.
	nproc := spec.ProcessLimit
	if nproc == 0 && spec.Processes > 0 {
		uid := os.Getuid()
		if os.Geteuid() == 0 && spec.UID != 0 {
			uid = spec.UID
		}
		nproc = countTasks(uid) + spec.Processes
	}
	abi := landlockABI()
	w, err := restrictLandlock(spec, exe, abi)
	warnings = append(warnings, w...)
	if err != nil {
		return warnings, err
	}
	if err := dropCapabilities(); err != nil {
		return warnings, err
	}
	changed, err := dropIdentity(spec)
	if err != nil {
		return warnings, err
	}
	if changed {
		// A credential change clears the parent-death signal the worker
		// asked for; restore it.
		if err := unix.Prctl(unix.PR_SET_PDEATHSIG, uintptr(syscall.SIGKILL), 0, 0, 0); err != nil {
			return warnings, fmt.Errorf("prctl(PR_SET_PDEATHSIG): %w", err)
		}
	}
	w, err = restrictSeccomp(spec, abi)
	warnings = append(warnings, w...)
	if err != nil {
		return warnings, err
	}
	return warnings, setRlimits(spec, nproc)
}

// landlockABI returns the kernel's Landlock ABI version, or 0 when Landlock is
// unavailable.
func landlockABI() int {
	v, err := ll.LandlockGetABIVersion()
	if err != nil || v < 0 {
		return 0
	}
	return v
}

// restrictLandlock enforces the filesystem and network rules of spec in best
// effort mode, and warns about whatever the kernel cannot enforce. It uses
// the newest rights go-landlock knows (V11): on top of V4's filesystem and
// TCP rights that adds device ioctls (ABI 5), signal and abstract Unix
// socket scoping (ABI 6), pathname Unix socket connects (ABI 9) and UDP
// (ABI 10). The kernel enforces the subset it supports.
//
// Under DenyNet every handled network right is denied: no rules. With
// ConnectPorts the handled set is the same (so bind(2) stays refused) plus a
// ConnectTCP rule per port. Below ABI 4 best effort drops those rules along
// with the TCP rights, leaving TCP connects unrestricted; that is a warning,
// and the downloader probe reports it.
func restrictLandlock(spec Spec, exe string, abi int) ([]string, error) {
	if !spec.Confines() {
		return nil, nil
	}
	var warnings []string
	if spec.RestrictsFS() && abi < 1 {
		warnings = append(warnings, "landlock unavailable: filesystem not restricted")
	}
	if abi >= 1 && abi < 6 {
		warnings = append(warnings, fmt.Sprintf("landlock ABI %d: signals and abstract sockets not scoped (needs 6; seccomp covers signals)", abi))
	}
	if len(spec.ConnectPorts) > 0 && abi < 4 {
		warnings = append(warnings, fmt.Sprintf("landlock ABI %d: TCP connect not limited to ports %v (needs 4)", abi, spec.ConnectPorts))
	}

	cfg := landlock.V11.BestEffort()
	if !spec.RestrictsFS() {
		cfg.HandledAccessFS = 0
	}
	if !spec.DenyNet {
		cfg.HandledAccessNet = 0
	}

	var rules []landlock.Rule
	if spec.RestrictsFS() {
		var err error
		if rules, err = fsRules(spec, exe); err != nil {
			return warnings, err
		}
	}
	if spec.DenyNet {
		for _, p := range spec.ConnectPorts {
			rules = append(rules, landlock.ConnectTCP(p))
		}
	}
	if err := cfg.Restrict(rules...); err != nil {
		return warnings, fmt.Errorf("landlock: %w", err)
	}
	return warnings, nil
}

// fsRules turns the path lists into Landlock rules, choosing the directory or
// file variant by what each path is. Missing read-only paths are skipped (the
// set of system library directories differs between distributions); a missing
// read-write path is an error.
func fsRules(spec Spec, exe string) ([]landlock.Rule, error) {
	var rules []landlock.Rule
	add := func(path string, dir, file func(...string) landlock.FSRule) error {
		fi, err := os.Stat(path)
		if err != nil {
			return err
		}
		if fi.IsDir() {
			rules = append(rules, dir(path))
		} else {
			rules = append(rules, file(path))
		}
		return nil
	}
	for _, p := range append([]string{exe}, spec.ReadOnly...) {
		if err := add(p, landlock.RODirs, landlock.ROFiles); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	for _, p := range spec.ReadWrite {
		if err := add(p, landlock.RWDirs, landlock.RWFiles); err != nil {
			return nil, err
		}
	}
	return rules, nil
}

// dropIdentity switches to the spec's uid and gid when running as root, and
// reports whether it did.
func dropIdentity(spec Spec) (bool, error) {
	if os.Geteuid() != 0 || (spec.UID == 0 && spec.GID == 0) {
		return false, nil
	}
	if err := syscall.Setgroups(nil); err != nil {
		return false, fmt.Errorf("setgroups: %w", err)
	}
	if spec.GID != 0 {
		if err := syscall.Setgid(spec.GID); err != nil {
			return false, fmt.Errorf("setgid: %w", err)
		}
	}
	if spec.UID != 0 {
		if err := syscall.Setuid(spec.UID); err != nil {
			return false, fmt.Errorf("setuid: %w", err)
		}
	}
	return true, nil
}

// setRlimits applies the resource limits process-wide. nproc is the
// RLIMIT_NPROC value worked out before the sandbox closed /proc.
func setRlimits(spec Spec, nproc uint64) error {
	for _, l := range []struct {
		res  int
		name string
		v    uint64
	}{
		{syscall.RLIMIT_AS, "RLIMIT_AS", spec.AddressSpace},
		{syscall.RLIMIT_FSIZE, "RLIMIT_FSIZE", spec.FileSize},
		{syscall.RLIMIT_NOFILE, "RLIMIT_NOFILE", spec.OpenFiles},
		{unix.RLIMIT_NPROC, "RLIMIT_NPROC", nproc},
	} {
		if l.v == 0 {
			continue
		}
		if err := lowerRlimit(l.res, l.name, l.v); err != nil {
			return err
		}
	}
	return nil
}

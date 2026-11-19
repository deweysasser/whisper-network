//go:build linux || darwin

package worker

import (
	"errors"
	"io/fs"
	"os"
	"syscall"
)

// openOutputFlags refuse a symlink in the last path component and never
// block on a FIFO.
const openOutputFlags = syscall.O_NOFOLLOW | syscall.O_NONBLOCK

// createNoFollow refuses a symlink in the last path component when the
// worker creates a file for a child.
const createNoFollow = syscall.O_NOFOLLOW

// isSymlinkErr reports the error O_NOFOLLOW gives for a symlink (ELOOP on
// Linux, EMLINK on some BSDs).
func isSymlinkErr(err error) bool {
	return errors.Is(err, syscall.ELOOP) || errors.Is(err, syscall.EMLINK)
}

func linkCount(fi fs.FileInfo) uint64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Nlink)
	}
	return 1
}

// killedByFSZ reports whether ps ended on SIGXFSZ (the signal's name
// differs between Linux and Darwin, so compare the number).
func killedByFSZ(ps *os.ProcessState) bool {
	ws, ok := ps.Sys().(syscall.WaitStatus)
	return ok && ws.Signaled() && ws.Signal() == syscall.SIGXFSZ
}

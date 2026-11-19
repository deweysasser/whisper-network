//go:build !linux && !darwin

package worker

import (
	"io/fs"
	"os"
)

// Children are not confined here, so these checks only catch accidents.
const openOutputFlags = 0

const createNoFollow = 0

func isSymlinkErr(error) bool { return false }

func linkCount(fs.FileInfo) uint64 { return 1 }

// killedByFSZ: there is no RLIMIT_FSIZE here.
func killedByFSZ(*os.ProcessState) bool { return false }

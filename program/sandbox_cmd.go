package program

import (
	"errors"
	"fmt"
	"os"

	"github.com/deweysasser/whisper-network/internal/sandbox"
)

// SandboxExecCmd is the hidden subcommand the worker re-executes itself as to
// confine a child process (DESIGN.md §7.4). It accepts the flags encoded by
// sandbox.Spec.Args, then "--", then the command to run. The raw arguments are
// passed through to the sandbox package so the flag set has a single
// definition, shared by the encoder and the decoder.
type SandboxExecCmd struct {
	Args []string `arg:"" optional:"" passthrough:"" help:"sandbox flags, then -- and the command"`
}

// Run applies the restrictions and execs the command. On platforms without
// execve it runs the child and exits with its status.
func (c *SandboxExecCmd) Run() error {
	err := sandbox.ExecArgs(c.Args)
	var ee *sandbox.ExitError
	if errors.As(err, &ee) {
		exitFunc(ee.Code)
		return nil
	}
	if err != nil {
		// Report on stderr, not through the logger: stdout belongs to the
		// child. 126 mirrors the shell's "found but could not execute".
		fmt.Fprintln(os.Stderr, err)
		exitFunc(sandboxFailureStatus)
	}
	return nil
}

// sandboxFailureStatus is the exit status when the sandbox could not be set up
// or the command could not be started.
const sandboxFailureStatus = sandbox.SetupFailureStatus

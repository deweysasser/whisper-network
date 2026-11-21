package sandbox

import (
	"context"
	"os/exec"
)

// SubcommandName is the hidden CLI subcommand that applies a Spec and execs.
const SubcommandName = "sandbox-exec"

// SetupFailureStatus is sandbox-exec's exit status when it could not apply
// the restrictions or start the command (the shell's "found but could not
// execute").
const SetupFailureStatus = 126

// Command builds the *exec.Cmd that runs argv under sandbox-exec. self is the
// path of the worker binary (normally os.Executable()). The context's
// cancellation kills the process, which is how wall-clock timeouts work.
// Callers may set Dir, Env and the standard streams on the result as usual,
// and should wait for it with [Wait].
//
// On Unix the child leads a new process group, and cancellation kills the
// whole group, so descendants die with it. On Linux the child also gets
// SIGKILL when the worker dies, and a confining spec stops descendants from
// leaving the group (see [Spec.Confines]).
func Command(ctx context.Context, self string, spec Spec, argv ...string) *exec.Cmd {
	args := append([]string{SubcommandName}, spec.Args()...)
	args = append(args, "--")
	args = append(args, argv...)
	cmd := exec.CommandContext(ctx, self, args...)
	setProcessGroup(cmd)
	return cmd
}

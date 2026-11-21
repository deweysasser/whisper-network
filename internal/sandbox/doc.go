// Package sandbox confines the child processes a worker runs (ffmpeg,
// whisper-cli) as described in DESIGN.md §7.4.
//
// The worker never runs a child directly. It builds a command with [Command],
// which re-executes the worker binary as
//
//	<self> sandbox-exec [flags] -- cmd args...
//
// The sandbox-exec subcommand calls [ExecArgs], which applies the restrictions
// in a [Spec] to the calling process and then replaces itself with the target
// using execve(2). Landlock domains, the seccomp filter, rlimits,
// no_new_privs, the emptied capability sets, the nice value, the scheduling
// policy and the I/O priority all survive execve, so they bind the child
// without affecting the worker. [Command] starts the child in its own process
// group, and cancellation and [Wait] kill the whole group. Wall-clock
// timeouts are the worker's job, via the context given to [Command].
//
// On Linux the filesystem rules and IPC scoping use Landlock in best-effort
// mode (rights the running kernel does not know are dropped rather than
// failing), and a hand-written seccomp filter refuses every socket under
// [Spec.DenyNet] (all but plain TCP when [Spec.ConnectPorts] names the
// ports a downloader's child may reach, which Landlock then enforces) and
// keeps descendants in their process group. [Probe] and [ProbeDownloader]
// measure what was actually enforced. On darwin only rlimits, nice and the
// process group are applied; on other systems the child is simply run and
// its exit code forwarded.
package sandbox

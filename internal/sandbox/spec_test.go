package sandbox

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fullSpec() Spec {
	return Spec{
		ReadOnly: []string{"/usr", "/path with space"}, ReadWrite: []string{"/tmp/t"},
		DenyNet: true, AddressSpace: 4 << 30, FileSize: 1 << 30, OpenFiles: 128, Processes: 256, ProcessLimit: 1024,
		Nice: 19, SchedIdle: true, IOPrioIdle: true, UID: 65534, GID: 65534,
	}
}

func TestSpecArgsRoundTrip(t *testing.T) {
	for name, spec := range map[string]Spec{
		"zero": {},
		"full": fullSpec(),
		"prio": Spec{}.WithLowestPriority(),
	} {
		t.Run(name, func(t *testing.T) {
			args := append(spec.Args(), "--", "cmd", "--ro=x", "-n")
			inv, err := ParseArgs(args)
			require.NoError(t, err)
			assert.Equal(t, spec, inv.Spec)
			assert.Equal(t, []string{"cmd", "--ro=x", "-n"}, inv.Argv, "flags after -- belong to the command")
			assert.False(t, inv.SelfTest)
		})
	}
}

func TestSpecArgsRoundTripConnectPorts(t *testing.T) {
	spec := Spec{ReadWrite: []string{"/tmp/t"}, DenyNet: true, ConnectPorts: []uint16{8080, 1, 65535}}
	args := spec.Args()
	assert.Contains(t, args, "--connect-port=8080")
	assert.Contains(t, args, "--connect-port=65535")
	inv, err := ParseArgs(append(args, "--", "yt-dlp"))
	require.NoError(t, err)
	assert.Equal(t, spec, inv.Spec)
}

func TestParseArgsErrors(t *testing.T) {
	for name, args := range map[string][]string{
		"no command":   {"--deny-net", "--"},
		"unknown flag": {"--bogus", "--", "x"},
		"bad number":   {"--rlimit-as=abc", "--", "x"},
		"nice range":   {"--nice=20", "--", "x"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseArgs(args)
			assert.Error(t, err)
		})
	}
}

func TestWithLowestPriorityDoesNotMutateReceiver(t *testing.T) {
	var s Spec
	low := s.WithLowestPriority()
	assert.Equal(t, 19, low.Nice)
	assert.True(t, low.SchedIdle && low.IOPrioIdle)
	assert.Zero(t, s.Nice)
}

func TestCommandShape(t *testing.T) {
	cmd := Command(context.Background(), "/bin/self", Spec{DenyNet: true}, "ffmpeg", "-i", "x")
	assert.Equal(t, []string{"/bin/self", "sandbox-exec", "--deny-net", "--", "ffmpeg", "-i", "x"}, cmd.Args)
}

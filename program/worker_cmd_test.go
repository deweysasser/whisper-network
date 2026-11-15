package program

import (
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkerCmdFlags(t *testing.T) {
	skipOnWindows(t, "the worker is unsupported")
	var opts Options
	_, err := opts.Parse([]string{"worker", "--server", "https://wn.example", "--memory-reserve", "512MiB",
		"--memory", "6g", "--kinds", "transcribe", "--slots", "2", "--drain", "--model-cache-max", "10GiB"})
	require.NoError(t, err)
	c := opts.Worker
	assert.Equal(t, ByteSize(512<<20), c.MemoryReserve)
	assert.Equal(t, ByteSize(6<<30), c.Memory)
	assert.Equal(t, ByteSize(1<<30), c.DiskReserve, "default")
	assert.Equal(t, ByteSize(10<<30), c.ModelCacheMax)
	assert.Equal(t, []string{"transcribe"}, c.Kinds)
	assert.Equal(t, 4, c.ThreadsPerSlot)
	assert.Equal(t, "/var/lib/whisper-network", c.State)
	assert.True(t, c.Drain)

	t.Setenv("WHISPER_SERVER", "https://env.example")
	t.Setenv("WHISPER_THREADS_PER_SLOT", "8")
	opts = Options{}
	_, err = opts.Parse([]string{"worker"})
	require.NoError(t, err)
	assert.Equal(t, "https://env.example", opts.Worker.Server)
	assert.Equal(t, 8, opts.Worker.ThreadsPerSlot)
	assert.Equal(t, []string{"prepare", "transcribe"}, opts.Worker.Kinds)
}

func TestWorkerCmdHostLoadFlags(t *testing.T) {
	skipOnWindows(t, "the worker is unsupported")
	var opts Options
	_, err := opts.Parse([]string{"worker", "--server", "https://wn.example"})
	require.NoError(t, err)
	assert.Equal(t, 70.0, opts.Worker.MaxHostLoad, "default")
	assert.Equal(t, 60.0, opts.Worker.HostLoadResume, "default")

	for _, args := range [][]string{
		{"--max-host-load", "0"},
		{"--max-host-load", "0", "--max-host-load-resume", "90"},
		{"--max-host-load", "90", "--max-host-load-resume", "90"},
	} {
		opts = Options{}
		_, err = opts.Parse(append([]string{"worker", "--server", "https://wn.example"}, args...))
		assert.NoError(t, err, "%v", args)
	}
	for _, args := range [][]string{
		{"--max-host-load", "50"}, // below the default resume
		{"--max-host-load", "120"},
		{"--max-host-load", "-1"},
		{"--max-host-load-resume", "-1"},
	} {
		opts = Options{}
		_, err = opts.Parse(append([]string{"worker", "--server", "https://wn.example"}, args...))
		assert.Error(t, err, "%v", args)
	}
}

func TestWorkerCmdRequiresServer(t *testing.T) {
	var opts Options
	_, err := opts.Parse([]string{"worker"})
	assert.Error(t, err)
	_, err = opts.Parse([]string{"worker", "--server", "x", "--kinds", "bogus"})
	assert.Error(t, err)
}

func TestByteSize(t *testing.T) {
	for in, want := range map[string]int64{"1GiB": 1 << 30, "512MiB": 512 << 20, "6g": 6 << 30, "800M": 800 << 20,
		"1.5GiB": 3 << 29, "1GB": 1e9, "4096": 4096} {
		var b ByteSize
		require.NoError(t, b.UnmarshalText([]byte(in)), in)
		assert.Equal(t, ByteSize(want), b, in)
	}
	var b ByteSize
	assert.Error(t, b.UnmarshalText([]byte("lots")))
	assert.Error(t, b.UnmarshalText([]byte("-1G")))
}

func TestWorkerCmdChildIdentityFlags(t *testing.T) {
	var opts Options
	_, err := opts.Parse([]string{"worker", "--server", "https://wn.example", "--child-uid", "10001", "--child-gid", "10002"})
	require.NoError(t, err)
	assert.Equal(t, 10001, opts.Worker.ChildUID)
	assert.Equal(t, 10002, opts.Worker.ChildGID)
	assert.Empty(t, opts.Worker.MetricsListen, "no metrics listener unless asked for")
}

func TestSecondSignalAborts(t *testing.T) {
	sigs := make(chan os.Signal, 2)
	done := make(chan struct{})
	defer close(done)
	abort := secondSignal(sigs, done)

	sigs <- syscall.SIGTERM
	select {
	case <-abort:
		t.Fatal("the first signal must only drain")
	case <-time.After(100 * time.Millisecond):
	}
	sigs <- syscall.SIGTERM
	select {
	case <-abort:
	case <-time.After(2 * time.Second):
		t.Fatal("the second signal did not abort")
	}
}

func TestReleaseOnTermFlag(t *testing.T) {
	skipOnWindows(t, "the worker is unsupported")
	var opts Options
	_, err := opts.Parse([]string{"worker", "--server", "https://wn.example"})
	require.NoError(t, err)
	assert.False(t, opts.Worker.ReleaseOnTerm, "draining is the default")
	opts = Options{}
	_, err = opts.Parse([]string{"worker", "--server", "https://wn.example", "--release-on-term", "--drain"})
	require.NoError(t, err, "--drain is still accepted")
	assert.True(t, opts.Worker.ReleaseOnTerm)
}

package worker

import (
	"os/exec"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrepareTimeout(t *testing.T) {
	t.Parallel()
	lim := Limits{PrepareTimeout: 2 * time.Hour, PrepareMinRate: mib}
	cases := []struct {
		name  string
		bytes int64
		want  time.Duration
	}{
		{"small input: the floor", 100 * mib, 2 * time.Hour},
		{"at the rate the floor allows", 7200 * mib, 2 * time.Hour},
		{"12 GiB at 1 MiB/s", 12 * gib, 12 * 1024 * time.Second},
		{"unknown size: the floor", 0, 2 * time.Hour},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, prepareTimeout(lim, c.bytes), c.name)
	}
	assert.Equal(t, 2*time.Hour, prepareTimeout(Limits{PrepareTimeout: 2 * time.Hour}, 12*gib),
		"no rate: the fixed timeout")
}

// TestOutputLimitHit: ffmpeg stopped by RLIMIT_FSIZE is recognised whether
// the signal kills it or it reports the refused write itself; any other
// failure is not.
func TestOutputLimitHit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signals are Unix-only")
	}
	t.Parallel()
	exitErr := func(script string) error {
		err := exec.Command("sh", "-c", script).Run()
		require.Error(t, err)
		return err
	}
	assert.True(t, outputLimitHit(childResult{}, exitErr("kill -XFSZ $$")), "killed by SIGXFSZ")
	assert.True(t, outputLimitHit(childResult{stderr: "av_interleaved_write_frame(): File too large"}, exitErr("exit 1")),
		"the write refused with EFBIG")
	assert.False(t, outputLimitHit(childResult{stderr: "Invalid data found when processing input"}, exitErr("exit 1")))
	assert.False(t, outputLimitHit(childResult{}, errChildTimeout), "not an exit")
}

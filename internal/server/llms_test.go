package server

import (
	"flag"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/store"
)

var updateGolden = flag.Bool("update", false, "rewrite golden files")

func llmsConfig() Config {
	c := DefaultConfig()
	c.PublicURL = "https://wn.example/"
	c.Models = []string{"small.en", "large-v3-turbo"}
	return c
}

// TestLLMsGolden pins the rendered llms.txt. Run with -update after an
// intended change and review the diff.
func TestLLMsGolden(t *testing.T) {
	got, err := renderLLMs(llmsConfig())
	require.NoError(t, err)
	golden := filepath.Join("testdata", "llms.golden.md")
	if *updateGolden {
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(golden, got, 0o644))
	}
	want, err := os.ReadFile(golden)
	require.NoError(t, err, "run go test ./internal/server -run TestLLMsGolden -update")
	assert.Equal(t, string(want), string(got), "llms.txt changed; review and run with -update")
}

// TestLLMsCoversClientAPI fails when the client API grows something the
// guide does not mention: every JobParams field, every job state, and every
// client endpoint must appear.
func TestLLMsCoversClientAPI(t *testing.T) {
	b, err := renderLLMs(llmsConfig())
	require.NoError(t, err)
	doc := string(b)

	pt := reflect.TypeOf(api.JobParams{})
	for i := 0; i < pt.NumField(); i++ {
		name := strings.Split(pt.Field(i).Tag.Get("json"), ",")[0]
		assert.Contains(t, doc, "`"+name+"`", "JobParams field %q is not documented", name)
	}
	for _, st := range []api.JobState{api.JobPreparing, api.JobFetching, api.JobQueued, api.JobRunning, api.JobDone, api.JobFailed} {
		assert.Contains(t, doc, "`"+string(st)+"`", "job state %q is not documented", st)
	}
	for _, ep := range []string{
		"POST /queue/{queue}", "GET /queue/{queue}", "GET /job/{id}",
		"GET /job/{id}/transcript", "GET /job/{id}/result", "DELETE /job/{id}",
		"POST /queue/{queue}/fetch", "no-downloader", "submit_url",
	} {
		assert.Contains(t, doc, ep, "endpoint %s is not documented", ep)
	}
	for _, w := range []api.Weight{api.WeightLow, api.WeightMedium, api.WeightHigh} {
		assert.Contains(t, doc, "`"+string(w)+"`")
	}
}

// TestLLMsServed: public, Markdown, instance values filled in, and nothing
// that needs authorization or comes from the request leaks into it.
func TestLLMsServed(t *testing.T) {
	t.Parallel()
	h := newHarness(t, func(c *Config, _ *store.Config) {
		c.PublicURL = "https://wn.example"
		c.Models = []string{"tiny.en", "small.en"}
		c.MaxUploadBytes = 512 << 20
		c.JobRetention = 72 * time.Hour
	})
	_, err := h.st.CreateQueue(h.ctx, store.NewQueue{Name: "board-secret"})
	require.NoError(t, err)

	resp := h.do(req{method: "GET", path: "/llms.txt", header: map[string]string{"Host": "evil.example"}})
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "text/markdown; charset=utf-8", resp.Header.Get("Content-Type"))
	doc := string(body)

	assert.Contains(t, doc, "https://wn.example/queue/public")
	assert.Contains(t, doc, "`tiny.en`, `small.en`")
	assert.Contains(t, doc, "512 MiB")
	assert.Contains(t, doc, "3 days")
	assert.NotContains(t, doc, "evil.example", "the Host header must not be reflected")
	assert.NotContains(t, doc, "board-secret", "queue names need authorization")
	assert.NotContains(t, doc, h.admin, "no tokens")
}

// TestLLMsWithoutPublicURL uses relative paths and a placeholder host.
func TestLLMsWithoutPublicURL(t *testing.T) {
	c := llmsConfig()
	c.PublicURL = ""
	b, err := renderLLMs(c)
	require.NoError(t, err)
	assert.Contains(t, string(b), "relative to this server's base URL")
	assert.Contains(t, string(b), "https://SERVER/queue/public")
}

func TestHumanUnits(t *testing.T) {
	for n, want := range map[int64]string{
		512: "512 bytes", 2 << 30: "2 GiB", 512 << 20: "512 MiB", 3 << 29: "1.5 GiB",
	} {
		assert.Equal(t, want, humanBytes(n), n)
	}
	for d, want := range map[time.Duration]string{
		720 * time.Hour: "30 days", 24 * time.Hour: "24 hours", time.Hour: "1 hour",
		90 * time.Minute: "1h30m0s",
	} {
		assert.Equal(t, want, humanDuration(d), d)
	}
}

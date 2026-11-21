package program

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zenizh/go-capturer"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/sched"
	"github.com/deweysasser/whisper-network/internal/server"
	"github.com/deweysasser/whisper-network/internal/store"
	"github.com/deweysasser/whisper-network/internal/testscale"
)

func parse(t *testing.T, args ...string) (*Options, error) {
	t.Helper()
	var opts Options
	_, err := opts.Parse(args)
	return &opts, err
}

func TestServerCmdDefaultsAndConfig(t *testing.T) {
	opts, err := parse(t, "server", "--data", t.TempDir(), "--max-upload-bytes", "512MiB",
		"--class-weights", "low=1,medium=3,high=9", "--models", "small.en", "--public-url", "https://wn.example/")
	require.NoError(t, err)
	c := opts.Server
	assert.Equal(t, ":8080", c.Listen)
	assert.Equal(t, "127.0.0.1:9090", c.MetricsListen, "metrics stay private unless asked")
	assert.Equal(t, "silero-v5.1.2", c.VADModel)
	cfg, err := c.serverConfig()
	require.NoError(t, err)
	assert.Equal(t, int64(512<<20), cfg.MaxUploadBytes)
	assert.Equal(t, int64(1<<30), cfg.MinFreeBytes)
	assert.Equal(t, []string{"small.en"}, cfg.Models)
	assert.True(t, cfg.CookieSecure, "https public URL")
	assert.Equal(t, Version, cfg.Version, "the server reports its own build")
	assert.Equal(t, 60*time.Minute, cfg.TaskTarget)
	assert.Equal(t, 60*time.Second, cfg.ReadTimeout)
	assert.Equal(t, int64(64<<10), cfg.MinTransferRate)
	assert.Equal(t, int64(512<<20), cfg.PasswordMemoryBudget)
	assert.Equal(t, 720*time.Hour, cfg.RejectedWorkerRetention)
	assert.Equal(t, 24*time.Hour, cfg.RetryWaitLimit)
	assert.Equal(t, 3, cfg.MaxPendingPerSource)
	assert.Equal(t, 2, cfg.UploadsPerPrincipal)
	assert.Equal(t, 1, cfg.AnalysesPerPrincipal)
	assert.Equal(t, 6*time.Hour, cfg.MaxLeaseAge)
	assert.InDelta(t, 4.0, cfg.LeaseAgeFactor, 0)
	assert.Equal(t, 24*time.Hour, cfg.FailedRetention)
	assert.Equal(t, server.DiskCeiling{Percent: 80}, cfg.FailedMaxDisk)
	scfg, err := c.storeConfig()
	require.NoError(t, err)
	assert.Equal(t, 3, scfg.WorkerFailureLimit, "the worker circuit breaker is on by default")
	assert.Equal(t, 2, scfg.MaxAttempts)
	assert.Equal(t, api.SandboxFull, scfg.FetchMinSandbox, "--fetch-min-sandbox defaults to full")
	assert.Equal(t, int64(512<<20), scfg.FetchMaxBytes, "fetches are capped by --max-upload-bytes")
	w, mf, err := c.storeTuning()
	require.NoError(t, err)
	assert.Equal(t, sched.ClassWeights{Low: 1, Medium: 3, High: 9}, w)
	assert.Equal(t, map[string]float64{"small.en": 1, "large-v3-turbo": 3}, mf)
}

func TestServerCmdFetchMinSandbox(t *testing.T) {
	opts, err := parse(t, "server", "--fetch-min-sandbox", "fs-only")
	require.NoError(t, err)
	scfg, err := opts.Server.storeConfig()
	require.NoError(t, err)
	assert.Equal(t, api.SandboxFSOnly, scfg.FetchMinSandbox)
	_, err = parse(t, "server", "--fetch-min-sandbox", "partial")
	assert.Error(t, err)
}

func TestServerCmdFailedRetention(t *testing.T) {
	opts, err := parse(t, "server", "--failed-retention", "48h", "--failed-retention-max-disk", "200GiB")
	require.NoError(t, err)
	cfg, err := opts.Server.serverConfig()
	require.NoError(t, err)
	assert.Equal(t, 48*time.Hour, cfg.FailedRetention)
	assert.Equal(t, server.DiskCeiling{Bytes: 200 << 30}, cfg.FailedMaxDisk)
}

func TestServerCmdEnv(t *testing.T) {
	t.Setenv("WHISPER_LISTEN", "127.0.0.1:1234")
	t.Setenv("WHISPER_MAX_ATTEMPTS", "5")
	t.Setenv("WHISPER_RETRY_WAIT_LIMIT", "2h")
	opts, err := parse(t, "server")
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:1234", opts.Server.Listen)
	assert.Equal(t, 5, opts.Server.MaxAttempts)
	assert.Equal(t, 2*time.Hour, opts.Server.RetryWaitLimit)
}

func TestServerCmdValidation(t *testing.T) {
	for name, args := range map[string][]string{
		"cert without key":     {"--tls-cert", "c.pem"},
		"require-tls no certs": {"--require-tls"},
		"bad class":            {"--class-weights", "urgent=3"},
		"bad factor":           {"--model-factors", "small.en=0"},
		"bad attempts":         {"--max-attempts", "0"},
		"bad public url":       {"--public-url", "wn.example"},
		"bad sandbox":          {"--prepare-min-sandbox", "maximal"},
		"zero per source":      {"--max-pending-per-source", "0"},
		"zero uploads each":    {"--max-uploads-per-principal", "0"},
		"zero analyses each":   {"--max-analyses-per-principal", "0"},
		"zero lease age":       {"--max-lease-age", "0s"},
		"negative age factor":  {"--lease-age-factor", "-1"},
		"zero retry wait":      {"--retry-wait-limit", "0s"},
		"removed retry grace":  {"--retry-elsewhere-grace", "10m"},
		"negative fail limit":  {"--worker-failure-limit", "-1"},
	} {
		_, err := parse(t, append([]string{"server"}, args...)...)
		assert.Error(t, err, name)
	}
	for _, args := range [][]string{
		{"--max-upload-bytes", "lots"},
		{"--min-transfer-rate", "0"},
		{"--password-memory-budget", "much"},
		{"--failed-retention-max-disk", "120%"},
		{"--failed-retention-max-disk", "lots"},
	} {
		opts, err := parse(t, append([]string{"server"}, args...)...)
		require.NoError(t, err)
		_, err = opts.Server.serverConfig()
		assert.Error(t, err, args)
	}
}

// TestPrepareDataDir: the data directory is created private, and an
// existing permissive one is tightened.
func TestPrepareDataDir(t *testing.T) {
	skipOnWindows(t, "POSIX directory modes")
	fresh := filepath.Join(t.TempDir(), "data")
	require.NoError(t, prepareDataDir(fresh, zerolog.Nop()))
	fi, err := os.Stat(fresh)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o700), fi.Mode().Perm())

	existing := t.TempDir()
	require.NoError(t, os.Chmod(existing, 0o755))
	require.NoError(t, prepareDataDir(existing, zerolog.Nop()))
	fi, err = os.Stat(existing)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o700), fi.Mode().Perm())
}

// TestAdminDataDirIsPrivate: the admin command, which may be the first to
// create the data directory, makes it as private as the server does, and
// blobs the server writes there are owner-only.
func TestAdminDataDirIsPrivate(t *testing.T) {
	skipOnWindows(t, "POSIX directory modes")
	data := filepath.Join(t.TempDir(), "data")
	opts, err := parse(t, "admin", "list-workers", "--data", data)
	require.NoError(t, err)
	capturer.CaptureStdout(func() { require.NoError(t, opts.Admin.ListWorkers.Run(context.Background(), zerolog.Nop())) })
	fi, err := os.Stat(data)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o700), fi.Mode().Perm())
}

// TestServerCmdRun starts the real command on loopback and stops it.
func TestServerCmdRun(t *testing.T) {
	data := t.TempDir()
	opts, err := parse(t, "server", "--data", data, "--listen", "127.0.0.1:0", "--metrics-listen", "",
		"--admin-password", "first-password")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- opts.Server.Run(ctx, zerolog.Nop()) }()

	// The database appears once the server has bootstrapped.
	require.Eventually(t, func() bool {
		select {
		case err := <-done:
			t.Fatalf("server exited early: %v", err)
		default:
		}
		st, err := store.Open(context.Background(), store.Config{Path: server.DBPath(data)})
		if err != nil {
			return false
		}
		defer st.Close()
		_, err = st.UserByUsername(context.Background(), store.AdminUsername)
		return err == nil
	}, testscale.D(30*time.Second), 20*time.Millisecond)
	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("server did not stop")
	}
}

func TestServerCmdServesHTTP(t *testing.T) {
	data := t.TempDir()
	addr, metricsAddr := freeAddr(t), freeAddr(t)
	opts, err := parse(t, "server", "--data", data, "--listen", addr, "--metrics-listen", metricsAddr)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- opts.Server.Run(ctx, zerolog.Nop()) }()
	// Startup hashes the bootstrap password with the production Argon2
	// parameters, which takes a while on a loaded runner.
	require.Eventually(t, func() bool {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == 200
	}, testscale.D(30*time.Second), 20*time.Millisecond)
	resp, err := http.Get("http://" + metricsAddr + "/metrics")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, 200, resp.StatusCode)
	cancel()
	require.NoError(t, <-done)
}

// freeAddr returns a loopback address with a port that was free a moment
// ago, for a command that takes an address rather than a listener.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

func TestAdminCommands(t *testing.T) {
	data := filepath.Join(t.TempDir(), "data")
	ctx := context.Background()
	run := func(args ...string) string {
		t.Helper()
		opts, err := parse(t, append([]string{"admin"}, append(args, "--data", data)...)...)
		require.NoError(t, err)
		var runErr error
		out := capturer.CaptureStdout(func() {
			switch args[0] {
			case "create-client":
				runErr = opts.Admin.CreateClient.Run(ctx, zerolog.Nop())
			case "create-user":
				runErr = opts.Admin.CreateUser.Run(ctx, zerolog.Nop())
			case "reset-password":
				runErr = opts.Admin.ResetPassword.Run(ctx, zerolog.Nop())
			case "grant":
				runErr = opts.Admin.Grant.Run(ctx, zerolog.Nop())
			case "list-workers":
				runErr = opts.Admin.ListWorkers.Run(ctx, zerolog.Nop())
			case "approve-worker":
				runErr = opts.Admin.ApproveWorker.Run(ctx, zerolog.Nop())
			}
		})
		require.NoError(t, runErr, args)
		return out
	}

	out := run("create-client", "--name", "harness", "--grant", "submit=queue:public", "--grant", "view_all=queue:public")
	assert.Regexp(t, `(?m)^client cli_[0-9a-f]+\ntoken wnc_[0-9A-Za-z]{43}$`, strings.TrimSpace(out))
	out = run("create-user", "--username", "alice", "--grant", "queue_admin=queue:public")
	assert.Regexp(t, `password [0-9A-Za-z]{24}`, out)
	out = run("reset-password", "--username", "alice")
	assert.Regexp(t, `password [0-9A-Za-z]{24}`, out)
	run("grant", "--principal", "user:alice", "--grant", "worker_admin=*")

	st, err := store.Open(ctx, store.Config{Path: server.DBPath(data)})
	require.NoError(t, err)
	w, err := st.RegisterWorker(ctx, "token-hash", api.WorkerFacts{Hostname: "nuc", SandboxLevel: api.SandboxFull})
	require.NoError(t, err)
	require.NoError(t, st.Close())

	out = run("list-workers")
	assert.Contains(t, out, w.ID)
	assert.Contains(t, out, "pending")
	run("approve-worker", "--id", w.ID, "--queue", "public")
	out = run("list-workers")
	assert.Regexp(t, w.ID+`\s+active\s+nuc\s+full\s+public`, out)

	st, err = store.Open(ctx, store.Config{Path: server.DBPath(data)})
	require.NoError(t, err)
	defer st.Close()
	alice, err := st.UserByUsername(ctx, "alice")
	require.NoError(t, err)
	assert.True(t, alice.MustChangePassword, "a generated reset password must be changed at next login")
	audit, err := st.ListAudit(ctx, 0, 100)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(audit), 5)
	cliActions := map[string]int{}
	for _, e := range audit {
		if e.Actor == "admin-cli" {
			cliActions[e.Action]++
		}
	}
	// Exactly one audit entry per CLI change (the worker registered through
	// the store directly above is not a CLI change).
	assert.Equal(t, map[string]int{"client.create": 1, "user.create": 1, "user.reset_password": 1,
		"grant.add": 1, "worker.approve": 1}, cliActions)
	for _, e := range audit {
		if e.Actor == "system" { // bootstrap's own entries
			assert.Contains(t, []string{"user.create", "queue.create"}, e.Action)
			continue
		}
		assert.Equal(t, "admin-cli", e.Actor)
	}

	_, err = parse(t, "admin", "create-client", "--name", "x", "--grant", "nonsense", "--data", data)
	require.NoError(t, err, "grant syntax is checked when the command runs")
	opts, _ := parse(t, "admin", "create-client", "--name", "x", "--grant", "submit", "--data", data)
	assert.Error(t, opts.Admin.CreateClient.Run(ctx, zerolog.Nop()))
}

package worker

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"

	"github.com/rs/zerolog"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/backoff"
	"github.com/deweysasser/whisper-network/internal/client"
	"github.com/deweysasser/whisper-network/internal/diskfree"
	"github.com/deweysasser/whisper-network/internal/models"
	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/sandbox"
)

// Config is everything a Worker needs. Fields marked "default" may be left
// zero.
type Config struct {
	// Client talks to the server. It must not carry a token; the worker adds
	// its own from the credentials file.
	Client *client.Client
	// StateDir holds credentials.json, the model cache and per-lease work
	// directories. It should be a persistent volume.
	StateDir string

	// Description is reported to the server; default the hostname.
	Description string
	// ThreadsPerSlot is whisper-cli's -t (default 4).
	ThreadsPerSlot int
	// Slots overrides the derived slot count (DESIGN.md §7.3) when > 0.
	Slots int
	// MemoryReserve is kept free for the system (default 1 GiB).
	MemoryReserve int64
	// Memory overrides the probed memory limit when > 0. Required where the
	// platform offers no probe.
	Memory int64
	// DiskReserve is kept free on the state filesystem: a lease whose input
	// and expected output would eat into it is released unrun (default
	// 1 GiB).
	DiskReserve int64
	// ModelCacheMax caps the model cache; least recently used models not
	// in use are evicted to stay under it. 0 means unlimited.
	ModelCacheMax int64
	// FreeBytes reports free space on the filesystem holding a path
	// (default diskfree.Free).
	FreeBytes func(path string) (int64, error)
	// Kinds the operator allows (default prepare and transcribe). The
	// worker offers only those whose tool is installed.
	Kinds []api.TaskKind
	// WhisperCLI and FFmpeg name the tools, as paths or names on $PATH
	// (default "whisper-cli" and "ffmpeg").
	WhisperCLI, FFmpeg string
	// EngineVersion overrides the version reported by whisper-cli.
	EngineVersion string
	// MaxHostLoad is the host CPU utilisation, in percent and excluding
	// this worker's own process tree, above which slots stop claiming new
	// work (DESIGN.md §7.3). 0 disables the check.
	MaxHostLoad float64
	// MaxHostLoadResume is the utilisation below which a slot stopped by
	// MaxHostLoad claims again; at most MaxHostLoad.
	MaxHostLoadResume float64
	// HostLoad returns the host CPU utilisation in percent, excluding the
	// worker's own process tree, and whether it is known (default: measured
	// from /proc over the last minute on Linux, unknown elsewhere).
	HostLoad func() (float64, bool)
	// Drain makes shutdown finish running tasks instead of releasing them.
	Drain bool
	// ChildUID and ChildGID are the identity children run as. A worker
	// running as root must set both (non-zero): its children would
	// otherwise run as root, able to read credentials.json. A worker not
	// running as root cannot switch identity, so it must leave them zero.
	// The uid must be dedicated to this worker's children: Run refuses to
	// start while anything else runs as it, and the children's
	// RLIMIT_NPROC is an absolute count of that uid's tasks.
	ChildUID, ChildGID int

	// Self is the worker binary, re-executed as sandbox-exec (default
	// os.Executable()).
	Self string
	// Version is the whisper-network version reported in facts.
	Version string
	// Catalog is the pinned model table (default models.All()).
	Catalog Catalog

	// Logger receives the worker's events; it should already be scoped with
	// obs.NewLogger(…, obs.ModeWorker).
	Logger zerolog.Logger
	// Metrics, if non-nil, records task compute time and realtime factor.
	Metrics *obs.Metrics

	// ClaimWait is the long-poll wait sent to the server (default 30 s).
	ClaimWait time.Duration
	// PendingBackoff spaces GET /worker polls while pending (default
	// 30 s → 5 min).
	PendingBackoff backoff.Policy
	// RetryBackoff spaces retries after transient errors (default 1 s → 1 min).
	RetryBackoff backoff.Policy
	// PausedPoll is the claim interval while paused, when the server gives no
	// hint (default 60 s).
	PausedPoll time.Duration
	// Sleep performs every delay (default backoff.Sleep). Tests scale it.
	Sleep backoff.Sleeper

	// Limits bound each child process.
	Limits Limits
	// SysRoot is the filesystem /proc and /sys are read from (default "/").
	SysRoot fs.FS
	// NumCPU is the CPU count before cgroup limits (default runtime.NumCPU).
	NumCPU int
	// ChildEnv is appended to the scrubbed environment of every child.
	ChildEnv []string
	// Downloader makes this a downloader worker: it runs fetch tasks only
	// (yt-dlp behind a local SSRF-filtering proxy) and needs neither ffmpeg,
	// whisper-cli nor models.
	Downloader bool
	// YtDlp names the yt-dlp binary (default "yt-dlp").
	YtDlp string
	// YtDlpCookies is an operator-supplied cookies file, copied into each
	// lease directory.
	YtDlpCookies string
	// EgressGroup names the shared egress this downloader sits behind; the
	// server rate-limits per group.
	EgressGroup string
	// EgressProbe is a URL returning the caller's public IP as plain text.
	// Empty leaves the egress IP unreported (the server falls back to the
	// worker's remote address).
	EgressProbe string
	// EgressProbeInterval is the period of the egress probe (default 10 min).
	EgressProbeInterval time.Duration
	// AllowPrivateEgress lets the downloader reach loopback, private and
	// link-local addresses.
	AllowPrivateEgress bool
	// ProbeDownloaderSandbox measures the downloader sandbox level with the
	// proxy port allowed (default sandbox.ProbeDownloader(Self, port)).
	ProbeDownloaderSandbox func(ctx context.Context, proxyPort uint16) (sandbox.ProbeResult, error)
	// ProbeSandbox measures the sandbox level (default sandbox.Probe(Self)).
	ProbeSandbox func(ctx context.Context) (sandbox.ProbeResult, error)
}

// Limits bound child processes (DESIGN.md §7.4).
type Limits struct {
	// Transcribe timeout = TimeoutFactor × audio duration + TimeoutBase
	// (defaults 4 and 10 min).
	TimeoutFactor float64
	TimeoutBase   time.Duration
	// PrepareTimeout is the least time one ffmpeg run gets (default 2 h).
	// Its duration is unknown in advance, so a larger input gets
	// input_bytes / PrepareMinRate if that is longer.
	PrepareTimeout time.Duration
	// PrepareMinRate is the slowest input rate, in bytes per second, a
	// prepare run is allowed before it times out (default 1 MiB/s).
	PrepareMinRate int64
	// AddressSpaceHeadroom is added to 2 × model peak RSS + model size for
	// whisper-cli's RLIMIT_AS (default 1 GiB).
	AddressSpaceHeadroom int64
	// PrepareAddressSpace is ffmpeg's RLIMIT_AS (default 4 GiB).
	PrepareAddressSpace int64
	// NoAddressSpaceLimit disables RLIMIT_AS (race-instrumented test
	// binaries reserve terabytes of address space).
	NoAddressSpaceLimit bool
	// TranscribeFileSize and PrepareFileSize are RLIMIT_FSIZE (defaults
	// 256 MiB and 4 GiB). A prepare child gets the lower of PrepareFileSize
	// and the server's bound on the result (prepareOutputCap).
	TranscribeFileSize, PrepareFileSize int64
	// OpenFiles is RLIMIT_NOFILE (default 256).
	OpenFiles int64
	// Processes is the per-slot allowance of processes and threads
	// (default 256). The children of all slots share one RLIMIT_NPROC of
	// Processes × slots: an absolute limit with a dedicated child uid,
	// headroom over the uid's tasks at launch without one (DESIGN.md §7.4).
	Processes int64
	// PrepareMemory is the memory reserved for a prepare task in the slot
	// accounting (default 512 MiB).
	PrepareMemory int64
	// FetchTimeout bounds one fetch (default 2 h).
	FetchTimeout time.Duration
	// FetchMemory is the memory reserved for a fetch in the slot accounting
	// (default 1 GiB).
	FetchMemory int64
	// FetchAddressSpace is yt-dlp's RLIMIT_AS (default 4 GiB).
	FetchAddressSpace int64
}

const (
	kib = int64(1) << 10
	mib = int64(1) << 20
	gib = int64(1) << 30
)

func (c *Config) setDefaults() error {
	if c.Client == nil {
		return errors.New("worker: Config.Client is required")
	}
	if c.StateDir == "" {
		return errors.New("worker: Config.StateDir is required")
	}
	if err := checkChildIdentity(geteuid(), c.ChildUID, c.ChildGID); err != nil {
		return err
	}
	if err := CheckHostLoad(c.MaxHostLoad, c.MaxHostLoadResume); err != nil {
		return fmt.Errorf("worker: %w", err)
	}
	def := func(p *int, v int) {
		if *p <= 0 {
			*p = v
		}
	}
	def64 := func(p *int64, v int64) {
		if *p <= 0 {
			*p = v
		}
	}
	defDur := func(p *time.Duration, v time.Duration) {
		if *p <= 0 {
			*p = v
		}
	}
	def(&c.ThreadsPerSlot, 4)
	def64(&c.MemoryReserve, gib)
	def64(&c.DiskReserve, gib)
	if c.ModelCacheMax < 0 {
		c.ModelCacheMax = 0
	}
	if c.FreeBytes == nil {
		c.FreeBytes = diskfree.Free
	}
	if c.Downloader {
		c.Kinds = []api.TaskKind{api.KindFetch}
		def(&c.Slots, 2)
		if c.YtDlp == "" {
			c.YtDlp = "yt-dlp"
		}
		defDur(&c.EgressProbeInterval, 10*time.Minute)
	}
	if len(c.Kinds) == 0 {
		c.Kinds = []api.TaskKind{api.KindPrepare, api.KindTranscribe}
	}
	if c.WhisperCLI == "" {
		c.WhisperCLI = "whisper-cli"
	}
	if c.FFmpeg == "" {
		c.FFmpeg = "ffmpeg"
	}
	if c.Self == "" {
		self, err := os.Executable()
		if err != nil {
			return err
		}
		c.Self = self
	}
	if c.Catalog == nil {
		c.Catalog = models.All()
	}
	defDur(&c.ClaimWait, 30*time.Second)
	if c.PendingBackoff.Initial <= 0 {
		c.PendingBackoff = backoff.Policy{Initial: 30 * time.Second, Max: 5 * time.Minute, Jitter: 0.1}
	}
	if c.RetryBackoff.Initial <= 0 {
		c.RetryBackoff = backoff.Policy{Initial: time.Second, Max: time.Minute, Jitter: 0.2}
	}
	defDur(&c.PausedPoll, 60*time.Second)
	if c.Sleep == nil {
		c.Sleep = backoff.Sleep
	}
	if c.SysRoot == nil {
		c.SysRoot = os.DirFS("/")
	}
	if c.ProbeDownloaderSandbox == nil {
		self := c.Self
		c.ProbeDownloaderSandbox = func(ctx context.Context, port uint16) (sandbox.ProbeResult, error) {
			return sandbox.ProbeDownloader(ctx, self, port)
		}
	}
	if c.ProbeSandbox == nil {
		self := c.Self
		c.ProbeSandbox = func(ctx context.Context) (sandbox.ProbeResult, error) { return sandbox.Probe(ctx, self) }
	}
	l := &c.Limits
	if l.TimeoutFactor <= 0 {
		l.TimeoutFactor = 4
	}
	defDur(&l.TimeoutBase, 10*time.Minute)
	defDur(&l.PrepareTimeout, 2*time.Hour)
	def64(&l.PrepareMinRate, mib)
	def64(&l.AddressSpaceHeadroom, gib)
	def64(&l.PrepareAddressSpace, 4*gib)
	def64(&l.TranscribeFileSize, 256*mib)
	def64(&l.PrepareFileSize, 4*gib)
	def64(&l.OpenFiles, 256)
	def64(&l.Processes, 256)
	def64(&l.PrepareMemory, 512*mib)
	defDur(&l.FetchTimeout, 2*time.Hour)
	def64(&l.FetchMemory, gib)
	def64(&l.FetchAddressSpace, 4*gib)
	return nil
}

// geteuid is os.Geteuid, replaceable in tests.
var geteuid = os.Geteuid

// checkChildIdentity enforces the rule of Config.ChildUID for a worker
// running with effective uid euid.
func checkChildIdentity(euid, uid, gid int) error {
	switch {
	case uid < 0 || gid < 0:
		return fmt.Errorf("worker: invalid child uid %d / gid %d", uid, gid)
	case euid == 0 && (uid == 0 || gid == 0):
		return errors.New("worker: refusing to run as root without a separate child identity: " +
			"set --child-uid and --child-gid to an unprivileged user and group (or run the worker as a non-root user)")
	case euid != 0 && (uid != 0 || gid != 0):
		return errors.New("worker: --child-uid and --child-gid need the worker to run as root; " +
			"a non-root worker's children already run as its own unprivileged user")
	}
	return nil
}

// Catalog is the table of model files the worker may use, pinned by hash.
type Catalog []models.Model

// Lookup finds a model by API name.
func (c Catalog) Lookup(name string) (models.Model, bool) {
	for _, m := range c {
		if m.Name == name {
			return m, true
		}
	}
	return models.Model{}, false
}

// Whisper returns the transcription models (not VAD models).
func (c Catalog) Whisper() []models.Model {
	var out []models.Model
	for _, m := range c {
		if m.Kind == models.KindWhisper {
			out = append(out, m)
		}
	}
	return out
}

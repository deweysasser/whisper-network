// Package store owns all SQLite access for the server (DESIGN.md §4.3).
//
// A Store holds two pools on one database file: a single-connection writer,
// whose transactions all begin with BEGIN IMMEDIATE, and a read-only pool for
// queries. Every write goes through the writer, so writes are serialized and a
// claim transaction can never see a stale snapshot.
//
// Timestamps are stored as Unix milliseconds UTC. The clock and the ID
// generator are injected through Config so tests are deterministic.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"runtime"
	"strings"
	"time"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/models"
	"github.com/deweysasser/whisper-network/internal/sched"
)

// Errors returned by Store methods. Callers test with errors.Is.
var (
	// ErrNotFound means the addressed row does not exist (or is not visible
	// to the caller, e.g. a lease held by another worker).
	ErrNotFound = errors.New("store: not found")
	// ErrConflict means the change collides with existing state: a duplicate
	// name, a forbidden state transition, or a different result for a lease
	// that already completed.
	ErrConflict = errors.New("store: conflict")
	// ErrLeaseGone means the lease exists but is no longer live (expired,
	// released, failed, cancelled, or its job was deleted). The holder must
	// stop work. It is the fencing signal (HTTP 410).
	ErrLeaseGone = errors.New("store: lease gone")
	// ErrInvalid means an argument failed validation.
	ErrInvalid = errors.New("store: invalid argument")
	// ErrWorkerInactive means a worker that is not active tried to claim.
	ErrWorkerInactive = errors.New("store: worker not active")
)

// IDKind selects which kind of identifier Config.NewID generates.
type IDKind int

// Identifier kinds.
const (
	IDJob    IDKind = iota // UUIDv7 by default
	IDWorker               // UUIDv4 by default
	IDLease                // "lse_" + 128 random bits by default
	IDClient               // "cli_" + 128 random bits by default
)

// Defaults applied by Open when the corresponding Config field is zero.
const (
	DefaultPrepareCost  = 60.0
	DefaultMaxAttempts  = 2
	DefaultQueueWeight  = 100
	DefaultBusyTimeout  = 5 * time.Second
	PublicQueue         = "public"
	AdminUsername       = "admin"
	canonicalSampleRate = 16000
)

// Config configures a Store.
type Config struct {
	// Path is the SQLite database file. Required.
	Path string
	// Now is the clock. Defaults to time.Now.
	Now func() time.Time
	// NewID generates identifiers. Defaults to DefaultNewID.
	NewID func(IDKind) string
	// ClassWeights are the job weight-class shares. Defaults to 1/2/4.
	ClassWeights sched.ClassWeights
	// ModelFactors maps model name to scheduling cost factor. Models not
	// listed fall back to the built-in table (internal/models), then to 1.
	ModelFactors map[string]float64
	// PrepareCost is the fixed scheduling cost of a prepare task. Default 60.
	PrepareCost float64
	// MaxAttempts is how many distinct workers a task may fail on (fail or
	// let its lease expire) before it fails its job. A worker never gets a
	// task it failed back, so each retry runs elsewhere. Default 2.
	MaxAttempts int
	// WorkerFailureLimit is the worker circuit breaker (DESIGN.md §4.2): an
	// active worker whose failure streak reaches this many failures, across
	// at least two jobs, is paused by the system in the transaction that
	// ends the lease. Zero disables it. The server's --worker-failure-limit
	// defaults to 3.
	WorkerFailureLimit int
	// PrepareMinSandbox is the minimum worker sandbox level for prepare
	// tasks. Defaults to full.
	PrepareMinSandbox api.SandboxLevel
	// FetchMinSandbox is the minimum worker sandbox level for a downloader
	// to count as eligible for a queue's fetch jobs, and to be offered
	// fetch tasks. Defaults to full.
	FetchMinSandbox api.SandboxLevel
	// FetchCost is the fixed scheduling cost of a fetch task. Defaults to
	// PrepareCost.
	FetchCost float64
	// FetchMaxBytes is the server's upload cap (--max-upload-bytes): a
	// fetch lease's byte cap is the queue's max_upload_bytes, never above
	// this. Zero means no server cap (the queue's alone, or none).
	FetchMaxBytes int64
	// ReadConns caps the read pool. Defaults to max(4, GOMAXPROCS).
	ReadConns int
}

// Store is the SQLite-backed state of the server. It is safe for concurrent use.
type Store struct {
	w, r *sql.DB
	cfg  Config
}

// Open opens (creating if needed) the database at cfg.Path, applies pending
// migrations and returns a Store.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("%w: empty database path", ErrInvalid)
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.NewID == nil {
		cfg.NewID = DefaultNewID
	}
	if cfg.ClassWeights == (sched.ClassWeights{}) {
		cfg.ClassWeights = sched.DefaultClassWeights()
	}
	if err := cfg.ClassWeights.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	if cfg.PrepareCost <= 0 {
		cfg.PrepareCost = DefaultPrepareCost
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = DefaultMaxAttempts
	}
	if cfg.PrepareMinSandbox == "" {
		cfg.PrepareMinSandbox = api.SandboxFull
	}
	if cfg.FetchCost <= 0 {
		cfg.FetchCost = cfg.PrepareCost
	}
	if cfg.FetchMinSandbox == "" {
		cfg.FetchMinSandbox = api.SandboxFull
	}
	if cfg.ReadConns <= 0 {
		cfg.ReadConns = max(4, runtime.GOMAXPROCS(0))
	}

	// SQLite creates the -wal and -shm files with the database file's mode,
	// so creating the database private keeps all three private.
	if err := createPrivate(cfg.Path); err != nil {
		return nil, err
	}

	common := fmt.Sprintf("_pragma=busy_timeout(%d)&_pragma=foreign_keys(1)&_pragma=synchronous(NORMAL)",
		DefaultBusyTimeout.Milliseconds())
	w, err := sql.Open("sqlite", "file:"+cfg.Path+"?"+common+"&_pragma=journal_mode(WAL)&_txlock=immediate")
	if err != nil {
		return nil, fmt.Errorf("open writer: %w", err)
	}
	w.SetMaxOpenConns(1)
	w.SetMaxIdleConns(1)
	w.SetConnMaxLifetime(0)
	w.SetConnMaxIdleTime(0)

	s := &Store{w: w, cfg: cfg}
	if err := s.migrate(ctx); err != nil {
		_ = w.Close()
		return nil, err
	}

	// The read pool opens after migration so WAL mode is already set on the file.
	r, err := sql.Open("sqlite", "file:"+cfg.Path+"?"+common+"&_pragma=query_only(1)")
	if err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("open reader: %w", err)
	}
	r.SetMaxOpenConns(cfg.ReadConns)
	r.SetMaxIdleConns(cfg.ReadConns)
	s.r = r
	// Tighten files left world-readable by earlier versions.
	if err := tightenDBFiles(cfg.Path); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

// dbFileMode is the mode of the database and its -wal and -shm files: they
// hold token and password hashes, so only the server's user may read them.
const dbFileMode = 0o600

// createPrivate creates an empty database file with dbFileMode unless one
// already exists.
func createPrivate(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, dbFileMode)
	if errors.Is(err, fs.ErrExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("create database: %w", err)
	}
	return f.Close()
}

// tightenDBFiles sets dbFileMode on the database and its -wal and -shm
// files, those that exist.
func tightenDBFiles(path string) error {
	for _, p := range []string{path, path + "-wal", path + "-shm"} {
		if err := os.Chmod(p, dbFileMode); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("database permissions: %w", err)
		}
	}
	return nil
}

// Close closes both pools.
func (s *Store) Close() error {
	return errors.Join(s.r.Close(), s.w.Close())
}

func (s *Store) now() time.Time { return s.cfg.Now() }

// write runs fn in a BEGIN IMMEDIATE transaction on the writer connection.
// fn must not call other Store methods (the writer has one connection).
func (s *Store) write(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// querier is satisfied by *sql.DB and *sql.Tx so readers can run in either.
type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// toMS converts t to Unix milliseconds.
func toMS(t time.Time) int64 { return t.UnixMilli() }

// fromMS converts Unix milliseconds to a UTC time.
func fromMS(ms int64) time.Time { return time.UnixMilli(ms).UTC() }

// optTime converts a nullable millisecond column to *time.Time.
func optTime(v sql.NullInt64) *time.Time {
	if !v.Valid {
		return nil
	}
	t := fromMS(v.Int64)
	return &t
}

// isUnique reports whether err is a UNIQUE or PRIMARY KEY constraint violation.
func isUnique(err error) bool {
	var se *sqlite.Error
	if !errors.As(err, &se) {
		return false
	}
	return se.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE || se.Code() == sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY
}

// isBusy reports whether err is SQLITE_BUSY (any extended code).
func isBusy(err error) bool {
	var se *sqlite.Error
	if !errors.As(err, &se) {
		return false
	}
	return se.Code()&0xff == sqlite3.SQLITE_BUSY
}

// notFound maps sql.ErrNoRows to ErrNotFound with context.
func notFound(err error, what string) error {
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%s: %w", what, ErrNotFound)
	}
	return fmt.Errorf("%s: %w", what, err)
}

// mustAffect returns ErrNotFound unless res changed at least one row.
func mustAffect(res sql.Result, err error, what string) error {
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	if n == 0 {
		return fmt.Errorf("%s: %w", what, ErrNotFound)
	}
	return nil
}

// placeholders returns "?,?,…" with n marks.
func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.Repeat("?,", n-1) + "?"
}

// modelFactor returns the scheduling cost factor of a model.
func (s *Store) modelFactor(model string) float64 {
	if f, ok := s.cfg.ModelFactors[model]; ok && f > 0 {
		return f
	}
	if m, err := models.Lookup(model); err == nil && m.CostFactor > 0 {
		return m.CostFactor
	}
	return 1
}

// TranscribeCost returns the scheduling cost of transcribing the given number
// of canonical (16 kHz) samples with model: audio seconds × model factor.
func (s *Store) TranscribeCost(model string, samples int64) float64 {
	return float64(samples) / canonicalSampleRate * s.modelFactor(model)
}

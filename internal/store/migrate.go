package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
	"time"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// migration is one numbered schema step; version N is file NNNN_*.sql.
type migration struct {
	version int
	name    string
	sql     string
}

func loadMigrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}
	var out []migration
	for _, e := range entries {
		prefix, _, ok := strings.Cut(e.Name(), "_")
		if !ok {
			return nil, fmt.Errorf("migration %q: name must be NNNN_description.sql", e.Name())
		}
		v, err := strconv.Atoi(prefix)
		if err != nil {
			return nil, fmt.Errorf("migration %q: %w", e.Name(), err)
		}
		b, err := migrationFS.ReadFile("migrations/" + e.Name())
		if err != nil {
			return nil, fmt.Errorf("migration %q: %w", e.Name(), err)
		}
		out = append(out, migration{version: v, name: e.Name(), sql: string(b)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	for i, m := range out {
		if m.version != i+1 {
			return nil, fmt.Errorf("migration %q: expected version %d", m.name, i+1)
		}
	}
	return out, nil
}

// migrate applies every migration newer than PRAGMA user_version, each in its
// own transaction together with the user_version bump. The version is
// re-read inside each (BEGIN IMMEDIATE) transaction, so two processes
// opening a fresh database at once (the server and the admin command)
// cannot both apply the same step.
func (s *Store) migrate(ctx context.Context) error {
	ms, err := loadMigrations()
	if err != nil {
		return err
	}
	for _, m := range ms {
		if err := retryBusy(ctx, migrationBusyLimit, func() error { return s.applyMigration(ctx, m, len(ms)) }); err != nil {
			return err
		}
	}
	return nil
}

// migrationBusyLimit bounds how long an opener keeps trying to take the
// write lock for a migration step. Each attempt already waits busy_timeout;
// the lock is not fair, so while another opener applies the steps one after
// another (each in its own transaction) a waiter can lose every race for
// longer than that, and the steps themselves are slow under load. A waiter
// that keeps trying finds the database migrated once the other opener is
// done (applyMigration checks the version before locking), so the limit only
// has to outlast a whole migration run, not one step.
const migrationBusyLimit = 60 * time.Second

// retryBusy runs op until it returns something other than SQLITE_BUSY or
// limit has passed. It exists for one case the busy_timeout handler does not
// cover: while two connections create the same database file at once, one
// of them switches it from rollback to WAL mode, and the other's next BEGIN
// fails with SQLITE_BUSY immediately (SQLite does not invoke the busy handler
// there). The failure is transient; a retry a few milliseconds later sees
// the file in WAL mode and proceeds.
func retryBusy(ctx context.Context, limit time.Duration, op func() error) error {
	deadline := time.Now().Add(limit)
	delay := 2 * time.Millisecond
	for {
		err := op()
		if !isBusy(err) || time.Now().After(deadline) {
			return err
		}
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return err
		case <-t.C:
		}
		delay = min(2*delay, 100*time.Millisecond)
	}
}

// restoreForeignKeys turns foreign key enforcement back on for conn. If
// that fails, conn is marked bad so the pool discards it rather than hand
// out a connection with enforcement off.
func restoreForeignKeys(conn *sql.Conn) error {
	_, err := conn.ExecContext(context.Background(), "PRAGMA foreign_keys=ON")
	if err == nil {
		return nil
	}
	_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	return fmt.Errorf("restoring foreign_keys (connection discarded): %w", err)
}

// foreignKeysOffDirective, as the first line of a migration, runs it with
// foreign key enforcement off, which SQLite's table rebuild procedure needs
// (https://sqlite.org/lang_altertable.html, "Making Other Kinds Of Table
// Schema Changes"). PRAGMA foreign_keys cannot change inside a transaction,
// so it is set on the dedicated connection before BEGIN; foreign_key_check
// must come back empty before COMMIT; enforcement is restored in every path.
const foreignKeysOffDirective = "-- whisper:foreign-keys-off"

func (m migration) foreignKeysOff() bool {
	first, _, _ := strings.Cut(m.sql, "\n")
	return strings.TrimSpace(first) == foreignKeysOffDirective
}

func (s *Store) applyMigration(ctx context.Context, m migration, latest int) (err error) {
	conn, err := s.w.Conn(ctx)
	if err != nil {
		return fmt.Errorf("migration %s: conn: %w", m.name, err)
	}
	defer func() { _ = conn.Close() }()
	// Lock-free fast path: in WAL mode this read never waits for a writer.
	// Anything already applied (by this process earlier, or by a concurrent
	// opener) needs no transaction. The version is read again inside the
	// transaction below, which is the check that counts.
	var seen int
	if err := conn.QueryRowContext(ctx, "PRAGMA user_version").Scan(&seen); err == nil && seen >= m.version && seen <= latest {
		return nil
	}
	if m.foreignKeysOff() {
		if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys=OFF"); err != nil {
			return fmt.Errorf("migration %s: foreign_keys off: %w", m.name, err)
		}
		defer func() {
			// Use a fresh context: ctx may be cancelled, and a pooled
			// connection must never go back with enforcement off.
			if rerr := restoreForeignKeys(conn); rerr != nil {
				err = errors.Join(err, fmt.Errorf("migration %s: %w", m.name, rerr))
			}
		}()
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("migration %s: begin: %w", m.name, err)
	}
	defer func() { _ = tx.Rollback() }() // no-op after commit
	var current int
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&current); err != nil {
		return fmt.Errorf("read user_version: %w", err)
	}
	if current > latest {
		return fmt.Errorf("database schema version %d is newer than this binary (%d)", current, latest)
	}
	if current >= m.version {
		return nil
	}
	if _, err := tx.ExecContext(ctx, m.sql); err != nil {
		return fmt.Errorf("migration %s: %w", m.name, err)
	}
	if m.foreignKeysOff() {
		rows, err := tx.QueryContext(ctx, "PRAGMA foreign_key_check")
		if err != nil {
			return fmt.Errorf("migration %s: foreign_key_check: %w", m.name, err)
		}
		violations := rows.Next()
		_ = rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("migration %s: foreign_key_check: %w", m.name, err)
		}
		if violations {
			return fmt.Errorf("migration %s: foreign_key_check found violations", m.name)
		}
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", m.version)); err != nil {
		return fmt.Errorf("migration %s: set version: %w", m.name, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("migration %s: commit: %w", m.name, err)
	}
	return nil
}

// SchemaVersion returns the applied schema version (PRAGMA user_version).
func (s *Store) SchemaVersion(ctx context.Context) (int, error) {
	var v int
	if err := s.r.QueryRowContext(ctx, "PRAGMA user_version").Scan(&v); err != nil {
		return 0, fmt.Errorf("read user_version: %w", err)
	}
	return v, nil
}

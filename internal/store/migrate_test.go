package store

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/auth"
)

// TestMigrationReadAll: upgrading a database from before read_all (schema
// 4) gives every view_all and queue_admin grant on a queue a matching
// read_all grant, so those principals keep the transcript access they had.
// Other grants are untouched, and an existing read_all is not duplicated.
func TestMigrationReadAll(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	other := f.queue("other", 1)
	pub, oth := QueueScope(f.pubID), QueueScope(other)
	type row struct {
		principal string
		role      api.Role
		scope     string
	}
	before := []row{
		{"client:viewer", api.RoleViewAll, pub},
		{"user:qa", api.RoleQueueAdmin, oth},
		{"client:both", api.RoleViewAll, pub},
		{"client:both", api.RoleReadAll, pub}, // already migrated by hand
		{"client:submitter", api.RoleSubmit, pub},
		{"client:deleter", api.RoleDeleteAll, pub},
		{"user:root", api.RoleAdmin, ScopeAll},
		{"worker:w", api.RoleProcess, pub},
	}
	for _, r := range before {
		require.NoError(t, f.s.AddGrant(f.ctx, r.principal, r.role, r.scope, "user:granter"))
	}

	// Rewind to the schema before read_all (version 4) and reopen: 0005
	// runs again on these rows exactly as it would on an upgraded
	// deployment. The read_all migration is data-only; 0006 then re-adds
	// leases.heartbeat_at and 0007 the failure streak table and
	// workers.pause_reason, 0008 leases.stage, 0009 the retry columns and
	// state-order index, 0010 tasks.pending_since and 0011 the throughput
	// table, so those are dropped first.
	_, err := f.s.w.ExecContext(f.ctx, undo0012+`
		ALTER TABLE leases DROP COLUMN heartbeat_at;
		ALTER TABLE leases DROP COLUMN stage;
		DROP TABLE worker_failure_streak;
		ALTER TABLE workers DROP COLUMN pause_reason;
		DROP INDEX jobs_queue_state_order;
		ALTER TABLE jobs DROP COLUMN retries;
		ALTER TABLE leases DROP COLUMN retry;
		ALTER TABLE tasks DROP COLUMN pending_since;
		DROP TABLE throughput;
		PRAGMA user_version = 4`)
	require.NoError(t, err)
	require.NoError(t, f.s.Close())
	s := f.open()
	v, err := s.SchemaVersion(f.ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, v, 7)

	held := func(principal string) []api.Grant {
		gs, err := s.GrantsFor(f.ctx, principal)
		require.NoError(t, err)
		out := make([]api.Grant, len(gs))
		for i, g := range gs {
			out[i] = api.Grant{Role: g.Role, Scope: g.Scope}
			if g.Role == api.RoleReadAll {
				assert.Equal(t, "user:granter", g.GrantedBy, "attributed to the original granter")
			}
		}
		return out
	}
	assert.ElementsMatch(t, []api.Grant{{Role: api.RoleViewAll, Scope: pub}, {Role: api.RoleReadAll, Scope: pub}},
		held("client:viewer"))
	assert.ElementsMatch(t, []api.Grant{{Role: api.RoleQueueAdmin, Scope: oth}, {Role: api.RoleReadAll, Scope: oth}},
		held("user:qa"))
	assert.ElementsMatch(t, []api.Grant{{Role: api.RoleViewAll, Scope: pub}, {Role: api.RoleReadAll, Scope: pub}},
		held("client:both"))
	assert.Equal(t, []api.Grant{{Role: api.RoleSubmit, Scope: pub}}, held("client:submitter"))
	assert.Equal(t, []api.Grant{{Role: api.RoleDeleteAll, Scope: pub}}, held("client:deleter"))
	assert.Equal(t, []api.Grant{{Role: api.RoleAdmin, Scope: ScopeAll}}, held("user:root"))
	assert.Equal(t, []api.Grant{{Role: api.RoleProcess, Scope: pub}}, held("worker:w"))

	// The migrated principals can still read someone else's transcript.
	// Stored scopes name queues by id; auth only compares the two, so the
	// id serves as the queue name here.
	foreign := func(id int64) auth.Resource {
		return auth.Resource{Queue: strconv.FormatInt(id, 10), OwnerID: "client:someone"}
	}
	viewer := auth.Principal{Kind: auth.KindClient, ID: "viewer", Grants: held("client:viewer")}
	assert.True(t, auth.Can(viewer, auth.ActDownloadTranscript, foreign(f.pubID)))
	assert.True(t, auth.Can(viewer, auth.ActViewJobContent, foreign(f.pubID)))
	qa := auth.Principal{Kind: auth.KindUser, ID: "qa", Grants: held("user:qa")}
	assert.True(t, auth.Can(qa, auth.ActDownloadTranscript, foreign(other)))
	assert.False(t, auth.Can(qa, auth.ActDownloadTranscript, foreign(f.pubID)), "only on its own queue")
}

// openAtVersion builds a database at schema version v from the real
// migrations only (no Store), so tests can seed old-shape rows before the
// remaining migrations run.
func openAtVersion(t *testing.T, v int) string {
	t.Helper()
	ms, err := loadMigrations()
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "wn.db")
	db, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	defer db.Close()
	for _, m := range ms[:v] {
		_, err := db.ExecContext(t.Context(), m.sql)
		require.NoError(t, err, m.name)
	}
	_, err = db.ExecContext(t.Context(), fmt.Sprintf("PRAGMA user_version = %d", v))
	require.NoError(t, err)
	return path
}

// dump returns every row of a query as strings, for row-for-row comparison.
func dump(t *testing.T, db *sql.DB, q string) [][]string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), q)
	require.NoError(t, err)
	defer rows.Close()
	cols, err := rows.Columns()
	require.NoError(t, err)
	var out [][]string
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		require.NoError(t, rows.Scan(ptrs...))
		row := make([]string, len(cols))
		for i, v := range vals {
			row[i] = "NULL"
			if v.Valid {
				row[i] = v.String
			}
		}
		out = append(out, row)
	}
	require.NoError(t, rows.Err())
	return out
}

func indexSQL(t *testing.T, db *sql.DB, table string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, r := range dump(t, db, "SELECT name, COALESCE(sql,'') FROM sqlite_master WHERE type='index' AND tbl_name='"+table+"'") {
		out[r[0]] = r[1]
	}
	return out
}

// TestMigrationForeignKeysOffDirective: a migration headed by the directive
// can drop and recreate a parent table without cascading into (or orphaning)
// its children, and one that leaves violations is rolled back.
func TestMigrationForeignKeysOffDirective(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	ctx := f.ctx
	_, err := f.s.w.ExecContext(ctx, `
		CREATE TABLE tparent (id TEXT PRIMARY KEY, n INTEGER NOT NULL);
		CREATE TABLE tchild (id INTEGER PRIMARY KEY, p TEXT NOT NULL REFERENCES tparent ON DELETE CASCADE);
		INSERT INTO tparent VALUES ('a', 1), ('b', 2);
		INSERT INTO tchild VALUES (1, 'a'), (2, 'b')`)
	require.NoError(t, err)
	ver, err := f.s.SchemaVersion(ctx)
	require.NoError(t, err)

	rebuild := migration{version: ver + 1, name: "9999_rebuild.sql", sql: foreignKeysOffDirective + `
		CREATE TABLE tparent_new (id TEXT PRIMARY KEY, n INTEGER NOT NULL, extra TEXT NOT NULL DEFAULT 'x');
		INSERT INTO tparent_new (id, n) SELECT id, n FROM tparent;
		DROP TABLE tparent;
		ALTER TABLE tparent_new RENAME TO tparent;`}
	require.NoError(t, f.s.applyMigration(ctx, rebuild, ver+1))
	assert.Equal(t, [][]string{{"a", "1", "x"}, {"b", "2", "x"}}, dump(t, f.s.w, "SELECT * FROM tparent ORDER BY id"))
	assert.Equal(t, [][]string{{"1", "a"}, {"2", "b"}}, dump(t, f.s.w, "SELECT * FROM tchild ORDER BY id"))
	assert.Equal(t, [][]string{{"1"}}, dump(t, f.s.w, "PRAGMA foreign_keys"), "enforcement restored")
	_, err = f.s.w.ExecContext(ctx, "INSERT INTO tchild VALUES (3, 'nope')")
	require.Error(t, err, "restored enforcement rejects orphans")

	// A migration that leaves an orphan fails and changes nothing.
	bad := migration{version: ver + 2, name: "9998_bad.sql", sql: foreignKeysOffDirective + `
		DELETE FROM tparent WHERE id = 'a';
		CREATE TABLE marker (x INTEGER);`}
	err = f.s.applyMigration(ctx, bad, ver+2)
	require.ErrorContains(t, err, "foreign_key_check")
	assert.Len(t, dump(t, f.s.w, "SELECT * FROM tparent"), 2)
	assert.Empty(t, dump(t, f.s.w, "SELECT name FROM sqlite_master WHERE name='marker'"))
	v2, err := f.s.SchemaVersion(ctx)
	require.NoError(t, err)
	assert.Equal(t, ver+1, v2)
	assert.Equal(t, [][]string{{"1"}}, dump(t, f.s.w, "PRAGMA foreign_keys"), "enforcement restored after failure")
}

// TestMigration12PreservesData: upgrading a v11 database keeps every jobs,
// tasks, leases and grants row, including retry state, and leaves no foreign
// key violations.
func TestMigration12PreservesData(t *testing.T) {
	t.Parallel()
	path := openAtVersion(t, 11)
	raw, err := sql.Open("sqlite", "file:"+path+"?_pragma=foreign_keys(1)")
	require.NoError(t, err)
	_, err = raw.ExecContext(t.Context(), `
		INSERT INTO queues (id, name, created_at) VALUES (1, 'public', 5);
		INSERT INTO workers (id, token_hash, state, registered_at) VALUES ('w1', 'th', 'active', 5);
		INSERT INTO jobs (id, queue_id, client_id, idempotency_key, state, model, language, created_at, updated_at,
		    completed_at, error, retries, original_bytes, metadata)
		  VALUES ('j1', 1, 'c', 'k1', 'failed', 'small.en', 'en', 10, 20, 30, 'boom', 1, 99, '{"a":1}'),
		         ('j2', 1, 'c', NULL, 'queued', 'small.en', 'en', 11, 21, NULL, NULL, 0, 0, NULL);
		INSERT INTO tasks (job_id, seq, kind, cost, state, attempts, pending_since)
		  VALUES ('j1', 0, 'prepare', 1.5, 'failed', 2, 777), ('j2', 0, 'prepare', 1, 'pending', 0, 888),
		         ('j2', 1, 'transcribe', 2.5, 'pending', 0, NULL);
		INSERT INTO leases (id, job_id, seq, worker_id, created_at, expires_at, ended_at, outcome, retry)
		  VALUES ('l1', 'j1', 0, 'w1', 12, 99, 40, 'failed', 1);
		INSERT INTO grants (principal, role, scope, granted_by, granted_at) VALUES ('worker:w1', 'process', '*', 'u', 1);
		INSERT INTO worker_failure_streak (worker_id, job_id, failures) VALUES ('w1', 'j1', 3)`)
	require.NoError(t, err)
	queries := []string{
		"SELECT id, queue_id, client_id, idempotency_key, state, weight, model, language, prompt, label, source, metadata, " +
			"original_bytes, original_sha256, original_type, original_purged_at, canonical_bytes, canonical_sha256, " +
			"audio_samples, canonical_purged_at, created_at, updated_at, started_at, completed_at, compute_ms, error, " +
			"first_downloaded_at, last_downloaded_at, download_count, retries FROM jobs ORDER BY id",
		"SELECT * FROM tasks ORDER BY job_id, seq",
		"SELECT id, job_id, seq, worker_id, created_at, expires_at, ended_at, outcome, heartbeat_at, stage, retry FROM leases ORDER BY id",
		"SELECT * FROM grants",
		"SELECT * FROM worker_failure_streak",
	}
	before := make([][][]string, len(queries))
	for i, q := range queries {
		before[i] = dump(t, raw, q)
	}
	require.NotEmpty(t, before[0])
	require.NoError(t, raw.Close())

	s, err := Open(t.Context(), Config{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	v, err := s.SchemaVersion(t.Context())
	require.NoError(t, err)
	assert.GreaterOrEqual(t, v, 12)
	for i, q := range queries {
		assert.Equal(t, before[i], dump(t, s.w, q), q)
	}
	assert.Equal(t, [][]string{{"1", "NULL"}}, dump(t, s.w, "SELECT retries, fetch_url FROM jobs WHERE id='j1'"))
	assert.Equal(t, [][]string{{"777"}}, dump(t, s.w, "SELECT pending_since FROM tasks WHERE job_id='j1'"))
	assert.Empty(t, dump(t, s.w, "PRAGMA foreign_key_check"))
	assert.Equal(t, [][]string{{"1"}}, dump(t, s.w, "PRAGMA foreign_keys"))
	assert.Equal(t, [][]string{{"processing", "unknown", "", "", ""}},
		dump(t, s.w, "SELECT kind, egress_key, ytdlp_version, egress_group, egress_ip FROM workers"))

	// The children still point at jobs: a cascade works.
	_, err = s.w.ExecContext(t.Context(), "DELETE FROM jobs WHERE id = 'j1'")
	require.NoError(t, err)
	assert.Empty(t, dump(t, s.w, "SELECT 1 FROM tasks WHERE job_id='j1'"))
	assert.Empty(t, dump(t, s.w, "SELECT 1 FROM worker_failure_streak WHERE job_id='j1'"))
}

// TestMigration12Schema: the rebuilt tables keep every index, accept the new
// states and kinds, and still reject others.
func TestMigration12Schema(t *testing.T) {
	t.Parallel()
	old := openAtVersion(t, 11)
	odb, err := sql.Open("sqlite", "file:"+old)
	require.NoError(t, err)
	oldJobs, oldTasks := indexSQL(t, odb, "jobs"), indexSQL(t, odb, "tasks")
	require.NoError(t, odb.Close())

	f := newFixture(t)
	gotJobs, gotTasks := indexSQL(t, f.s.w, "jobs"), indexSQL(t, f.s.w, "tasks")
	keys := func(m map[string]string) (out []string) {
		for k := range m {
			out = append(out, k)
		}
		return out
	}
	assert.ElementsMatch(t, keys(oldJobs), keys(gotJobs))
	assert.ElementsMatch(t, keys(oldTasks), keys(gotTasks))
	for k, v := range oldTasks {
		assert.Equal(t, v, gotTasks[k], k)
	}
	for _, k := range []string{"jobs_queue_state", "jobs_queue_created", "jobs_completed"} {
		assert.Equal(t, oldJobs[k], gotJobs[k], k)
	}
	assert.Contains(t, gotJobs["jobs_queue_state_order"], "WHEN 'fetching' THEN 1")

	ins := func(state, kind string) (error, error) {
		id := "j-" + state
		_, e1 := f.s.w.ExecContext(f.ctx, `INSERT INTO jobs (id, queue_id, client_id, state, model, language, created_at, updated_at,
			fetch_url, fetch_site) VALUES (?, ?, 'c', ?, 'm', 'en', 1, 1, 'https://x/y', 'x')`, id, f.pubID, state)
		_, e2 := f.s.w.ExecContext(f.ctx, `INSERT INTO tasks (job_id, seq, kind, cost, state) VALUES (?, ?, ?, 1, 'pending')`,
			id, FetchSeq, kind)
		return e1, e2
	}
	e1, e2 := ins("fetching", "fetch")
	require.NoError(t, e1)
	require.NoError(t, e2)
	e1, _ = ins("bogus", "prepare")
	require.Error(t, e1)
	_, err = f.s.w.ExecContext(f.ctx, `INSERT INTO tasks (job_id, seq, kind, cost, state) VALUES ('j-fetching', 5, 'bogus', 1, 'pending')`)
	require.Error(t, err)

	_, err = f.s.w.ExecContext(f.ctx, `INSERT INTO workers (id, token_hash, state, registered_at, kind) VALUES ('wx','t','pending',1,'bogus')`)
	require.Error(t, err)
	_, err = f.s.w.ExecContext(f.ctx, `INSERT INTO workers (id, token_hash, state, registered_at, kind) VALUES ('wd','t2','pending',1,'downloader')`)
	require.NoError(t, err)
	_, err = f.s.w.ExecContext(f.ctx, `INSERT INTO leases (id, job_id, seq, worker_id, created_at, expires_at, egress_key)
		VALUES ('l', 'j-fetching', -1, 'wd', 1, 2, 'e1')`)
	require.NoError(t, err)
	_, err = f.s.w.ExecContext(f.ctx, `INSERT INTO fetch_starts VALUES ('e1','x',1)`)
	require.NoError(t, err)
	_, err = f.s.w.ExecContext(f.ctx, `INSERT INTO fetch_cooldowns (egress_key, site, until, set_by) VALUES ('e1','x',9,'system')`)
	require.NoError(t, err)
	assert.Equal(t, [][]string{{"fetch_starts_window"}},
		dump(t, f.s.w, "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='fetch_starts'"))
}

func TestMigration12SeedsPolicies(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	got := dump(t, f.s.w, "SELECT key, site, egress_key, rules, concurrency, backoff_initial_s, backoff_cap_s, updated_by FROM fetch_policies ORDER BY key")
	assert.Equal(t, [][]string{
		{"*", "*", "NULL", "[]", "2", "300", "21600", "system"},
		{"youtube.com", "youtube.com", "NULL", `["1/3m"]`, "1", "43200", "86400", "system"},
	}, got)
	var at int64
	require.NoError(t, f.s.w.QueryRowContext(f.ctx, "SELECT MIN(updated_at) FROM fetch_policies").Scan(&at))
	assert.InDelta(t, time.Now().UnixMilli(), at, float64(time.Minute.Milliseconds()))
}

// undo0012 reverses migration 0012's additive changes (the jobs/tasks
// rebuild is idempotent), for tests that rewind to an earlier schema version
// and reopen so later migrations run again.
const undo0012 = `
	DROP TABLE fetch_policies;
	DROP TABLE fetch_starts;
	DROP TABLE fetch_cooldowns;
	ALTER TABLE leases DROP COLUMN egress_key;
	ALTER TABLE workers DROP COLUMN kind;
	ALTER TABLE workers DROP COLUMN egress_key;
	ALTER TABLE workers DROP COLUMN ytdlp_version;
	ALTER TABLE workers DROP COLUMN egress_group;
	ALTER TABLE workers DROP COLUMN egress_ip;
	ALTER TABLE jobs DROP COLUMN fetch_url;
	ALTER TABLE jobs DROP COLUMN fetch_site;
`

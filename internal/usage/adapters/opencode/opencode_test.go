package opencode

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/nabutabu/herdr-observr/internal/snapshot"
	"github.com/nabutabu/herdr-observr/internal/usage"
)

// fixtureSchema mirrors the confirmed opencode.db session-table columns the
// adapter reads (U0.1 spike): cost REAL plus the five cumulative token
// counters. The real table has more columns; the adapter never selects them.
const fixtureSchema = `CREATE TABLE session (
    id                   TEXT PRIMARY KEY,
    cost                 REAL,
    tokens_input         INTEGER,
    tokens_output        INTEGER,
    tokens_reasoning     INTEGER,
    tokens_cache_read    INTEGER,
    tokens_cache_write   INTEGER
)`

// openFixtureDB builds a fresh journal_mode=WAL opencode.db in a temp dir with
// one committed session row and returns the db path. The WAL journal mode is
// what the real opencode.db runs in, so every U2.x test exercises the same
// storage layout the live probe confirmed.
func openFixtureDB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "opencode.db")
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("opening fixture db: %v", err)
	}

	var mode string
	if err := db.QueryRow("PRAGMA journal_mode=WAL").Scan(&mode); err != nil {
		db.Close()
		t.Fatalf("setting journal_mode=WAL: %v", err)
	}
	if mode != "wal" {
		db.Close()
		t.Fatalf("journal_mode = %q, want wal", mode)
	}
	if _, err := db.Exec(fixtureSchema); err != nil {
		db.Close()
		t.Fatalf("creating session table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO session
		(id, cost, tokens_input, tokens_output, tokens_reasoning, tokens_cache_read, tokens_cache_write)
		VALUES ('ses-1', 0.42, 100, 50, 10, 5, 2)`); err != nil {
		db.Close()
		t.Fatalf("inserting fixture row: %v", err)
	}
	db.Close()
	return path
}

func TestDefaultDBPathHonorsXDG(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", filepath.Join("", "xdg"))
	t.Setenv("HOME", filepath.Join("", "home", "me"))
	want := filepath.Join("", "xdg", "opencode", "opencode.db")
	if got := defaultDBPath(); got != want {
		t.Errorf("defaultDBPath() = %q, want %q", got, want)
	}
}

func TestDefaultDBPathFallsBackToHome(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", "")
	t.Setenv("HOME", filepath.Join("", "home", "me"))
	want := filepath.Join("", "home", "me", ".local", "share", "opencode", "opencode.db")
	if got := defaultDBPath(); got != want {
		t.Errorf("defaultDBPath() = %q, want %q", got, want)
	}
}

func TestDefaultDBPathIgnoresEmptyXDG(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", "")
	t.Setenv("HOME", filepath.Join("", "home", "me"))
	if got := defaultDBPath(); got == "" {
		t.Fatal("defaultDBPath() = \"\"")
	}
}

func TestNewExplicitPathOverrides(t *testing.T) {
	a := New(filepath.Join("", "custom", "opencode.db"))
	want := filepath.Join("", "custom", "opencode.db")
	if a.dbPath != want {
		t.Errorf("New(explicit).dbPath = %q, want %q", a.dbPath, want)
	}

	a = New("")
	if a.dbPath == "" {
		t.Error("New(\"\" ) left dbPath empty, want environment default")
	}
}

func TestNewFallsBackToEnvDefault(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", filepath.Join("", "xdg"))
	a := New()
	if want := filepath.Join("", "xdg", "opencode", "opencode.db"); a.dbPath != want {
		t.Errorf("New().dbPath = %q, want %q", a.dbPath, want)
	}
}

func TestOpenReadOnlyRejectsWrites(t *testing.T) {
	a := New(openFixtureDB(t))
	db, err := a.openReadOnly(context.Background())
	if err != nil {
		t.Fatalf("openReadOnly: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`INSERT INTO session (id) VALUES ('no-writes-here')`); err == nil {
		t.Fatal("INSERT succeeded on a query_only connection")
	}
}

func TestOpenReadOnlyMissingDBErrorsAndCreatesNothing(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist.db")
	a := New(missing)
	if db, err := a.openReadOnly(context.Background()); err == nil {
		db.Close()
		t.Fatalf("openReadOnly(%s) succeeded, want error", missing)
	}
	if _, err := os.Stat(missing); err == nil {
		t.Errorf("openReadOnly created %s; a bad path must not materialize an empty db", missing)
	}
}

func TestWALConcurrentRead(t *testing.T) {
	// The U2.1 concurrency case, in-test: a writer holds an open transaction
	// with an uncommitted row in the WAL while a fresh read-only connection
	// reads the committed snapshot. WAL snapshot isolation means the reader
	// sees only committed data and is not blocked by the writer.
	path := openFixtureDB(t)

	writer, err := sql.Open("sqlite", "file:"+path+"?_busy_timeout=5000")
	if err != nil {
		t.Fatalf("opening writer: %v", err)
	}
	defer writer.Close()

	tx, err := writer.Begin()
	if err != nil {
		t.Fatalf("begin writer tx: %v", err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO session
		(id, cost, tokens_input, tokens_output, tokens_reasoning, tokens_cache_read, tokens_cache_write)
		VALUES ('ses-2', 9.99, 200, 100, 20, 10, 4)`); err != nil {
		t.Fatalf("inserting uncommitted row: %v", err)
	}

	a := New(path)
	ro, err := a.openReadOnly(context.Background())
	if err != nil {
		t.Fatalf("openReadOnly while writer active: %v", err)
	}
	defer ro.Close()

	var ids []string
	rows, err := ro.Query("SELECT id FROM session ORDER BY id")
	if err != nil {
		t.Fatalf("reading while writer active: %v", err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scanning id: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	if len(ids) != 1 || ids[0] != "ses-1" {
		t.Fatalf("read-only view = %v, want [ses-1] (ses-2 is uncommitted)", ids)
	}

	var input int64
	if err := ro.QueryRow("SELECT tokens_input FROM session WHERE id = 'ses-1'").Scan(&input); err != nil {
		t.Fatalf("reading committed row: %v", err)
	}
	if input != 100 {
		t.Errorf("tokens_input for ses-1 = %d, want 100", input)
	}
}

func TestOpenPerPollRepeatedCycles(t *testing.T) {
	// Open-per-poll really means it: every poll opens, queries, closes. A run
	// of cycles must all succeed with no leaked handle preventing later opens.
	a := New(openFixtureDB(t))
	for i := 1; i <= 5; i++ {
		db, err := a.openReadOnly(context.Background())
		if err != nil {
			t.Fatalf("cycle %d: openReadOnly: %v", i, err)
		}
		var n int
		if err := db.QueryRow("SELECT count(*) FROM session").Scan(&n); err != nil {
			db.Close()
			t.Fatalf("cycle %d: query: %v", i, err)
		}
		if n != 1 {
			db.Close()
			t.Errorf("cycle %d: count = %d, want 1", i, n)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("cycle %d: close: %v", i, err)
		}
	}
}

func TestPollUsageReturnsTotals(t *testing.T) {
	a := New(openFixtureDB(t))

	got, err := a.PollUsage(context.Background(), usage.AgentSessionRef{
		Source: "herdr:opencode",
		Kind:   snapshot.AgentSessionRefKindID,
		Value:  "ses-1",
	})
	if err != nil {
		t.Fatalf("PollUsage: %v", err)
	}

	want := usage.UsageTotals{
		SessionID:        "ses-1",
		CostUSD:          0.42,
		InputTokens:      100,
		OutputTokens:     50,
		ReasoningTokens:  10,
		CacheReadTokens:  5,
		CacheWriteTokens: 2,
	}
	if got != want {
		t.Errorf("PollUsage = %+v, want %+v", got, want)
	}
}

func TestPollUsageSessionNotFound(t *testing.T) {
	a := New(openFixtureDB(t))

	_, err := a.PollUsage(context.Background(), usage.AgentSessionRef{
		Source: "herdr:opencode",
		Kind:   snapshot.AgentSessionRefKindID,
		Value:  "ses-nope",
	})
	if !errors.Is(err, ErrSessionNotFound) {
		t.Errorf("PollUsage(missing id) err = %v, want wrapping ErrSessionNotFound", err)
	}
}

func TestPollUsageNullColumnsCoalesceToZero(t *testing.T) {
	// A fresh session row can legally hold NULL usage columns (no NOT NULL
	// constraint on the schema). They must read as zeros, not fail the scan.
	path := openFixtureDB(t)
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("opening fixture db: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO session (id) VALUES ('ses-null')`); err != nil {
		db.Close()
		t.Fatalf("inserting NULL-column row: %v", err)
	}
	db.Close()

	a := New(path)
	got, err := a.PollUsage(context.Background(), usage.AgentSessionRef{
		Source: "herdr:opencode",
		Kind:   snapshot.AgentSessionRefKindID,
		Value:  "ses-null",
	})
	if err != nil {
		t.Fatalf("PollUsage(NULL columns): %v", err)
	}

	want := usage.UsageTotals{SessionID: "ses-null"}
	if got != want {
		t.Errorf("PollUsage(NULL columns) = %+v, want %+v", got, want)
	}
}

func TestPollUsageRejectsPathKind(t *testing.T) {
	a := New(filepath.Join("", "unused", "opencode.db"))

	_, err := a.PollUsage(context.Background(), usage.AgentSessionRef{
		Source: "herdr:opencode",
		Kind:   snapshot.AgentSessionRefKindPath,
		Value:  "/some/session/dir",
	})
	if !errors.Is(err, ErrUnsupportedKind) {
		t.Errorf("PollUsage(path) err = %v, want wrapping ErrUnsupportedKind", err)
	}
}

func TestAgentType(t *testing.T) {
	if got := New().AgentType(); got != "opencode" {
		t.Errorf("AgentType() = %q, want opencode", got)
	}
}

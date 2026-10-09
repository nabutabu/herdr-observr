// Package opencode implements the usage.UsageAdapter seam for opencode's own
// SQLite store (PLAN.md Usage & Cost Telemetry Plan, U2.1).
//
// Privacy bar, same as the rest of herdr-observr: the adapter's contracts read
// only the session row's cumulative cost/token columns. Nothing here ever
// reads the message table or any prompt/response content.
//
// U2.2 scope: complete — PollUsage reads the session row's cumulative cost and
// five token counters, NULL-coalescing to zero. The message table is never
// touched.
package opencode

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite" // pure-Go driver, same one the U0 spike used

	"github.com/nabutabu/herdr-observr/internal/snapshot"
	"github.com/nabutabu/herdr-observr/internal/usage"
)

// ErrUnsupportedKind marks an AgentSessionRef whose Kind the adapter cannot
// resolve (U2 only implements Kind == "id"; "path" — unconfirmed on the wire —
// is rejected rather than guessed at).
var ErrUnsupportedKind = errors.New("unsupported agent session ref kind")

// ErrSessionNotFound marks a session id that resolves to no row in opencode.db.
// It wraps sql.ErrNoRows so callers can distinguish "session genuinely absent
// (pruned, or the pane's attribution is stale)" from a database-level failure.
var ErrSessionNotFound = errors.New("opencode session not found")

// OpencodeAdapter polls opencode's cumulative per-session cost/token totals
// out of opencode.db. It carries one piece of constructor-injected state — the
// database path — and is otherwise stateless across PollUsage calls, per the
// UsageAdapter contract: all cursor/diff state lives in the collector, keyed
// by SessionID (never by pane).
type OpencodeAdapter struct {
	// dbPath is the opencode.db filesystem path. Set explicitly via New or
	// resolved from the environment (XDG_DATA_HOME / $HOME); never read from
	// the AgentSessionRef, which carries a session id (Kind "id"), not a path.
	dbPath string
}

// New builds an OpencodeAdapter over opencode.db. With no args (or an empty
// first arg) it resolves the standard opencode data location:
//
//	$XDG_DATA_HOME/opencode/opencode.db   if XDG_DATA_HOME is set
//	$HOME/.local/share/opencode/opencode.db  otherwise (the U0-confirmed path)
//
// An explicit dbPath is both the test seam (fixture opencode.db in tests) and
// the future hook for 4.2-style config.
func New(dbPath ...string) *OpencodeAdapter {
	if len(dbPath) > 0 && dbPath[0] != "" {
		return &OpencodeAdapter{dbPath: dbPath[0]}
	}
	return &OpencodeAdapter{dbPath: defaultDBPath()}
}

// AgentType returns the pane agent type this adapter serves ("opencode"), the
// key the collector dispatches on (U1.2).
func (a *OpencodeAdapter) AgentType() string { return "opencode" }

var _ usage.UsageAdapter = (*OpencodeAdapter)(nil)

// maxParentWalkDepth bounds resolveRoot's upward walk. Real trees are depth 2
// today (root + direct children, confirmed live and by an offline census that
// found zero grandchildren), so this is cheap insurance against a corrupt or
// cyclic parent_id chain, never a real depth limit.
const maxParentWalkDepth = 64

// PollUsage resolves ref (Kind == "id") to the root of its session tree and
// returns one UsageTotals per session in that tree — the root plus every
// parent_id descendant. Resolving up from the referenced session (rather than
// assuming ref names the root) means children, which are wire-invisible, are
// captured no matter which session the pane's frontmost agent_session points
// at.
//
// The message table is never read; only the session row's cost/token columns
// participate. NULL usage columns scan as zero (a fresh session can legally
// hold NULL), and a NULL parent_id scans to "" (root). A referenced session
// with no row is ErrSessionNotFound; a broken ancestor chain stops at the last
// existing id rather than failing the poll. The returned SessionIDs are the
// authoritative ids scanned from the rows themselves, never echoed from the
// lookup key.
func (a *OpencodeAdapter) PollUsage(ctx context.Context, ref usage.AgentSessionRef) ([]usage.UsageTotals, error) {
	if ref.Kind != snapshot.AgentSessionRefKindID {
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedKind, ref.Kind)
	}

	db, err := a.openReadOnly(ctx)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	root, err := resolveRoot(ctx, db, ref.Value)
	if err != nil {
		return nil, err
	}

	// One recursive CTE returns the whole subtree: the seed is the root, and
	// each step adds the children of everything already in `tree`. UNION (not
	// UNION ALL) dedups, so a cyclic parent_id chain terminates instead of
	// recursing forever. session_parent_idx makes the parent_id join indexed.
	rows, err := db.QueryContext(ctx, `
		WITH RECURSIVE tree(id) AS (
			VALUES(?)
			UNION
			SELECT s.id FROM session s JOIN tree t ON s.parent_id = t.id
		)
		SELECT id, parent_id, cost, tokens_input, tokens_output,
		       tokens_reasoning, tokens_cache_read, tokens_cache_write
		FROM session WHERE id IN (SELECT id FROM tree)`, root)
	if err != nil {
		return nil, fmt.Errorf("querying session tree %q: %w", root, err)
	}
	defer rows.Close()

	var out []usage.UsageTotals
	for rows.Next() {
		var (
			id                    string
			parent                sql.NullString
			cost                  sql.NullFloat64
			input, output         sql.NullInt64
			reasoning             sql.NullInt64
			cacheRead, cacheWrite sql.NullInt64
		)
		if err := rows.Scan(&id, &parent, &cost, &input, &output, &reasoning, &cacheRead, &cacheWrite); err != nil {
			return nil, fmt.Errorf("scanning session tree row for %q: %w", root, err)
		}
		out = append(out, usage.UsageTotals{
			SessionID:        id,
			ParentID:         parent.String,
			CostUSD:          cost.Float64,
			InputTokens:      input.Int64,
			OutputTokens:     output.Int64,
			ReasoningTokens:  reasoning.Int64,
			CacheReadTokens:  cacheRead.Int64,
			CacheWriteTokens: cacheWrite.Int64,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading session tree %q: %w", root, err)
	}
	return out, nil
}

// resolveRoot walks parent_id upward from id to the top of its session tree.
// Depth is tiny (2 today), so it queries row-by-row rather than pulling the
// whole graph. A missing row for the referenced id is ErrSessionNotFound; a
// missing ancestor stops the walk at the last existing id (a pruned parent
// shouldn't blank the child's usage). The iteration bound guards a cyclic or
// corrupt chain.
func resolveRoot(ctx context.Context, db *sql.DB, id string) (string, error) {
	root := id
	for depth := 0; depth < maxParentWalkDepth; depth++ {
		var parent sql.NullString
		err := db.QueryRowContext(ctx, `SELECT parent_id FROM session WHERE id = ?`, root).Scan(&parent)
		if errors.Is(err, sql.ErrNoRows) {
			if root == id {
				return "", fmt.Errorf("%w: %q", ErrSessionNotFound, id)
			}
			return root, nil
		}
		if err != nil {
			return "", fmt.Errorf("resolving parent of session %q: %w", root, err)
		}
		if !parent.Valid || parent.String == "" {
			return root, nil
		}
		root = parent.String
	}
	return root, nil
}

// defaultDBPath resolves opencode's standard data-dir location, honoring
// XDG_DATA_HOME and falling back to the U0-spike-confirmed default
// (~/.local/share/opencode/opencode.db).
func defaultDBPath() string {
	if xdg := os.Getenv("XDG_DATA_HOME"); xdg != "" {
		return filepath.Join(xdg, "opencode", "opencode.db")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		// A missing/unreachable $HOME is not recoverable for a per-user store.
		return filepath.Join(".", ".local", "share", "opencode", "opencode.db")
	}
	return filepath.Join(home, ".local", "share", "opencode", "opencode.db")
}

// openReadOnly opens a fresh read-only connection to opencode.db, open-per-poll
// (U2.1): the caller queries and closes it; nothing long-lived is retained. A
// connection at the 10s poll cadence is cheap and sidesteps any WAL/stale-
// snapshot concerns across a long-lived handle. ctx aborts the eager
// connect/verification ping if the poll is cancelled mid-open.
//
// The modernc.org/sqlite driver has no URI "mode=ro" key (unlike mattn's), so
// write-rejection is enforced the driver's own way: _query_only=on (a
// validated shorthand for PRAGMA query_only=1), plus _busy_timeout so a
// transient opencode writer lock pauses briefly instead of failing the poll.
//
// WAL concurrency: opencode.db is journal_mode=WAL, confirmed live
// (journal_mode="wal", with opencode.db-wal/-shm present) — and modernc's
// query_only connection just reads committed frames, so SQLite's WAL snapshot
// isolation means a concurrent opencode writer never blocks this reader. The
// only WAL caveat, read-only directory access to -wal/-shm, cannot apply here:
// the plugin runs as the same user and directory that owns opencode.db.
//
// The dbpath is statted first so a typo'd path returns an error instead of
// silently materializing an empty database file (a source-of-truth no-op that
// would otherwise look like "session not found" forever).
func (a *OpencodeAdapter) openReadOnly(ctx context.Context) (*sql.DB, error) {
	if fi, err := os.Stat(a.dbPath); err != nil {
		return nil, fmt.Errorf("opencode.db %s not readable: %w", a.dbPath, err)
	} else if fi.IsDir() {
		return nil, fmt.Errorf("opencode.db %s is a directory", a.dbPath)
	}

	dsn := "file:" + a.dbPath + "?_busy_timeout=5000&_query_only=on"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening %s read-only: %w", a.dbPath, err)
	}
	// sql.Open is lazy — force the real open so connection-level failures
	// surface here rather than on the caller's first query.
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("can't open %s read-only: %w", a.dbPath, err)
	}
	return db, nil
}

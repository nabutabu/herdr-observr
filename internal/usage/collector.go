package usage

import (
	"context"
	"log/slog"
	"time"

	"github.com/nabutabu/herdr-observr/internal/snapshot"
	"github.com/nabutabu/herdr-observr/internal/tracker"
)

// DefaultPollInterval is how often the collector polls each active pane's
// session (U1.2). Tuned in the same range as the U0 spike's ~10s probes: fine
// enough granularity for cost/token deltas without hammering the source read.
const DefaultPollInterval = 10 * time.Second

// DefaultReconcileInterval is how often Run re-derives the active set from a
// fresh panes() snapshot even when no transition or sync landed in the
// meantime. It bounds the first-seen-at-working gap (an in-scope pane whose
// first-ever status is `working` emits no transition — the tracker's
// first-seen upsert — so neither NotifyTransition nor RequestSync can ever
// activate it) to one interval, and it self-heals any stale ordering between a
// sync and a concurrent transition within that same bound. Memory-only: the
// scan copies the panes map and applies adds/removes; it never polls
// non-working panes, so it does not reintroduce the poll-everything waste this
// revision removes.
const DefaultReconcileInterval = time.Minute

// UsageCollector turns per-session cumulative totals from assistant sources
// into session-scoped usage deltas (U1.2). It is event-driven rather than a
// full-pane scan: usage totals only move while an agent is `working`, so the
// collector tracks an *active set* of panes in AgentStatusWorking (the only
// status where polling is worth anything) and polls exactly those on an
// interval, dispatching each pane's agent_session to the adapter registered
// for that pane's agent type. Everything that causes a pane to enter or leave
// the active set arrives as an event or a snapshot reconciliation, never a
// scan of every open pane. The collector owns all session_id ->
// last-observed totals state; adapters stay stateless across PollUsage calls.
//
// Cursor and active-set state (the session_id -> UsageTotals map, the active
// pane set, and the adapter routing map) is owned solely by the collector's
// Run goroutine: register adapters before Run, then talk to it only through
// NotifyTransition (live agent transitions) and RequestSync (snapshot
// re-baselines) — both non-blocking channel handoffs that Run itself drains —
// and consume output on Deltas. No locking, same single-owner discipline as
// Subscriber and the tracker.
type UsageCollector struct {
	// adapters maps agent type (e.g. "opencode") to the adapter serving it.
	adapters map[string]UsageAdapter

	// panes is the injected read accessor over the tracker's live panes,
	// bound at construction so the collector never reaches into the tracker
	// lock itself.
	panes func() map[string]tracker.PaneState

	// cursors holds the last-observed cumulative totals per session id,
	// independent of which pane (if any) currently points at that session.
	cursors map[string]UsageTotals

	// deltas surfaces accrued usage between polls. Non-blocking send with
	// drop-and-warn on backpressure, matching Subscriber's events channel
	// idiom.
	deltas chan UsageDelta

	// interval is the poll cadence for the active set. Defaults to
	// DefaultPollInterval; tests substitute a short one.
	interval time.Duration

	// reconcileInterval is how often Run calls applySync against a fresh
	// c.panes() map on its own. Defaults to DefaultReconcileInterval; tests
	// substitute a short one. See the const above for why this exists.
	reconcileInterval time.Duration

	// active holds the pane IDs currently in AgentStatusWorking — the only
	// status where usage totals move. Populated by applyTransition (live
	// transitions) and applySync (requested re-baselines plus Run's own
	// self-heal reconcile scan); cleaned by pollPaneInMap when a pane closes
	// or vanishes, and by applySync when it stops working.
	active map[string]struct{}

	// transitions receives AgentTransition values forwarded from
	// Tracker.Transitions() via NotifyTransition. Buffered and drop-and-warn
	// on backpressure, matching the events/deltas channel idiom elsewhere in
	// this repo.
	transitions chan tracker.AgentTransition

	// syncs receives snapshot-derived pane maps forwarded from RequestSync
	// (ApplySnapshot re-baselines: bootstrap and every reconnect). Capacity 1,
	// coalesced — a pending sync is replaced by a newer one rather than
	// queued, since each carries the full current picture and supersedes
	// whatever was waiting. This is the only path into active/cursors from
	// outside the collector's own Run goroutine, which is what keeps this
	// data-race-free: RequestSync never touches active/cursors itself, only
	// Run does, on its own goroutine, via applySync.
	syncs chan map[string]tracker.PaneState
}

// NewUsageCollector builds a collector over the given panes accessor and
// initial adapters. Register additional adapters before starting Run.
func NewUsageCollector(panes func() map[string]tracker.PaneState, adapters ...UsageAdapter) *UsageCollector {
	c := &UsageCollector{
		adapters:          map[string]UsageAdapter{},
		panes:             panes,
		cursors:           map[string]UsageTotals{},
		deltas:            make(chan UsageDelta, 64),
		interval:          DefaultPollInterval,
		reconcileInterval: DefaultReconcileInterval,
		active:            map[string]struct{}{},
		transitions:       make(chan tracker.AgentTransition, 64),
		syncs:             make(chan map[string]tracker.PaneState, 1),
	}
	for _, a := range adapters {
		c.Register(a)
	}
	return c
}

// Register routes one adapter's agent type to that adapter. Call before Run —
// the routing map is read by the collector goroutine and must not be mutated
// once polling starts.
func (c *UsageCollector) Register(a UsageAdapter) {
	if a == nil {
		slog.Warn("registering nil usage adapter; skipping")
		return
	}
	c.adapters[a.AgentType()] = a
}

// Deltas delivers accrued per-session usage as it is observed (U1.2). This is
// the normalized usage seam (U3.1): the app consumes it on its event loop like
// the tracker's Transition/StateDuration channels and U4 exports each delta
// directly.
func (c *UsageCollector) Deltas() <-chan UsageDelta { return c.deltas }

// emitDelta is a non-blocking send with drop-and-warn, matching the repo's
// channel idioms (Subscriber, Tracker).
func (c *UsageCollector) emitDelta(d UsageDelta) {
	select {
	case c.deltas <- d:
	default:
		slog.Warn("usage deltas channel full; dropping", "session_id", d.SessionID)
	}
}

// NotifyTransition feeds one agent state transition (from
// Tracker.Transitions()) into the collector so it can start or stop polling
// the pane's usage. Call from the app's event-loop goroutine — this is a
// non-blocking send; the transition is applied on the collector's own Run
// goroutine so active/cursors stay single-owner.
func (c *UsageCollector) NotifyTransition(tr tracker.AgentTransition) {
	select {
	case c.transitions <- tr:
	default:
		slog.Warn("usage collector transitions channel full; dropping", "pane_id", tr.PaneID)
	}
}

// RequestSync enqueues a snapshot-derived pane map for reconciliation against
// the active set. Transitions() never fires for a snapshot bootstrap or
// re-baseline (by design — it's not a genuine transition), so this is the
// seam that (a) catches panes already `working` at cold start — the "collect
// once at the beginning so we're up to date on first load" requirement — and
// (b) catches panes that started or stopped working while disconnected,
// across a reconnect's re-baseline.
//
// Call it right after every Tracker.ApplySnapshot: once at bootstrap, once
// after every reconnect. Non-blocking; safe to call from the app's
// event-loop goroutine even while Run is live, because this method never
// touches active/cursors itself — it only hands the map to Run, which
// applies it (applySync) on its own goroutine. That indirection is what
// makes this safe: a version of this that mutated active/cursors directly
// from the caller would race Run's own goroutine on a reconnect, since Run
// is already up and iterating/writing those maps by then (only the
// once-before-Run-starts bootstrap call would have been safe as a direct
// call — reconnect's is the one that actually needs this).
//
// The channel is capacity 1 and coalesced: if a sync is already pending, it
// is replaced by this newer one rather than queued, since each snapshot is
// the full current picture and supersedes whatever was waiting.
func (c *UsageCollector) RequestSync(panes map[string]tracker.PaneState) {
	select {
	case c.syncs <- panes:
	default:
		select {
		case <-c.syncs: // drop the stale pending sync
		default:
		}
		select {
		case c.syncs <- panes:
		default: // Run drained it between our two selects; fine, it'll pick this up next call
		}
	}
}

// applySync is RequestSync's effect (plus Run's self-heal reconcile), run on
// the collector's own Run goroutine. Same active-set reconciliation logic as
// before, just no longer reachable from outside that goroutine.
func (c *UsageCollector) applySync(ctx context.Context, panes map[string]tracker.PaneState) {
	seen := make(map[string]struct{}, len(panes))
	for id, p := range panes {
		if p.Status != snapshot.AgentStatusWorking {
			continue
		}
		seen[id] = struct{}{}
		if _, already := c.active[id]; already {
			continue
		}
		// Fresh-status guard against a stale map: the passed panes snapshot
		// was built on the app (or reconcile) goroutine, so a transition that
		// applyTransition already processed in between can disagree with it.
		// Re-check the live pane before activating, so a pane that already
		// stopped working isn't re-added (and one that started working isn't
		// dropped before the self-heal scan re-derives it).
		if live, ok := c.panes()[id]; !ok || live.Status != snapshot.AgentStatusWorking {
			continue
		} else {
			c.active[id] = struct{}{}
			c.pollPane(ctx, live) // immediate first poll, same as a live transition-in
		}
	}
	for id := range c.active {
		if _, still := seen[id]; !still {
			delete(c.active, id) // pane stopped working (or closed) while we weren't watching
		}
	}
}

// Run drives the collector until ctx is cancelled. Call in its own goroutine,
// mirroring Tracker.Run. Four inputs, all landing on this one goroutine so
// active/cursors have exactly one writer: live transitions (add/remove from
// the active set, with an immediate poll on entry), requested snapshot resyncs
// (bootstrap + post-reconnect reconciliation), the interval ticker (re-polls
// whatever is currently active; a no-op tick when active is empty), and the
// self-heal reconcile ticker (re-derives the active set from a fresh panes()
// map — see DefaultReconcileInterval).
func (c *UsageCollector) Run(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	reconcile := time.NewTicker(c.reconcileInterval)
	defer reconcile.Stop()
	for {
		select {
		case tr := <-c.transitions:
			c.applyTransition(ctx, tr)
		case panes := <-c.syncs:
			c.applySync(ctx, panes)
		case <-ticker.C:
			c.pollActive(ctx)
		case <-reconcile.C:
			c.applySync(ctx, c.panes())
		case <-ctx.Done():
			return
		}
	}
}

// applyTransition adds or removes a pane from the active set based on the
// transition's New status. Entering working polls immediately rather than
// waiting for the next tick. Leaving working does one trailing poll first —
// to catch whatever accrued between the last tick and this transition — then
// removes the pane; no further polling happens for it until it transitions
// back into working.
func (c *UsageCollector) applyTransition(ctx context.Context, tr tracker.AgentTransition) {
	if tr.New == snapshot.AgentStatusWorking {
		c.active[tr.PaneID] = struct{}{}
		c.pollPaneByID(ctx, tr.PaneID)
		return
	}
	if _, was := c.active[tr.PaneID]; was {
		c.pollPaneByID(ctx, tr.PaneID) // trailing poll before we stop watching
		delete(c.active, tr.PaneID)
	}
}

// pollActive re-polls every currently-active pane. Empty active set: no-op.
// Copies the panes map once for the whole tick and looks each pane up in that
// shared copy rather than per-pane fresh lookups (each of which would re-copy
// the whole tracker map). Cross-tick freshness is what matters — the
// frontmost session value flips between ticks, not within one — so one copy
// per tick preserves it.
func (c *UsageCollector) pollActive(ctx context.Context) {
	panes := c.panes()
	for id := range c.active {
		c.pollPaneInMap(ctx, id, panes)
	}
}

// pollPaneByID looks the pane up fresh (agent_session can flip which session
// is frontmost for a given pane between polls — the U0 finding) and polls it
// if it's still live and has a session. Missing/closed/session-less panes are
// accepted gaps, not errors.
func (c *UsageCollector) pollPaneByID(ctx context.Context, paneID string) {
	c.pollPaneInMap(ctx, paneID, c.panes())
}

// pollPaneInMap looks paneID up in a caller-provided panes map and polls it.
// Shared by pollPaneByID (fresh map per call) and pollActive (one map per
// tick). A pane that is gone — or in its close grace window (ClosedAt set) —
// is dropped from the active set. The ClosedAt branch matters: closing a
// `working` pane emits NO transition (close is not a state transition; pane,
// tab, and workspace close only flush durations/attention latency), so
// without this the pane would sit in active forever, taking a no-op poll every
// tick until a sync or self-heal reconcile happened to remove it.
func (c *UsageCollector) pollPaneInMap(ctx context.Context, paneID string, panes map[string]tracker.PaneState) {
	pane, ok := panes[paneID]
	if !ok || !pane.ClosedAt.IsZero() {
		delete(c.active, paneID) // pane is gone, or closing: stop tracking it as active
		return
	}
	c.pollPane(ctx, pane)
}

// pollPane fetches one pane's session totals and diffs them against the
// cursor. Pane-level skips (no agent_session, no adapter, gracing pane) are
// accepted gaps — the plan's design says panes without a session attribution
// get no usage tracking, not silently degraded to a heuristic.
func (c *UsageCollector) pollPane(ctx context.Context, pane tracker.PaneState) {
	if !pane.ClosedAt.IsZero() {
		return // in close grace window (2.7): not a live pane, don't poll
	}
	if pane.AgentSession == nil {
		return // no agent_session: accepted gap, no usage tracking
	}

	// Dispatch on the pane's agent type, falling back to the session's own
	// agent type when the pane hasn't had an agent.detected yet.
	agentType := pane.Agent
	if agentType == "" {
		agentType = pane.AgentSession.Agent
	}
	adapter := c.adapters[agentType]
	if adapter == nil {
		slog.Debug("no usage adapter for agent type; skipping", "pane_id", pane.PaneID, "agent_type", agentType)
		return
	}

	ref := AgentSessionRef{
		Source: pane.AgentSession.Source,
		Kind:   pane.AgentSession.Kind,
		Value:  pane.AgentSession.Value,
	}
	totals, err := adapter.PollUsage(ctx, ref)
	if err != nil {
		slog.Warn("polling usage failed; skipping", "pane_id", pane.PaneID, "error", err)
		return
	}
	if totals.SessionID == "" {
		slog.Warn("adapter returned empty session id; skipping", "pane_id", pane.PaneID, "ref_value", ref.Value)
		return
	}

	c.diffAndRecord(totals)
}

// diffAndRecord compares fresh totals against the last-observed totals for the
// session and emits the accrued delta. The cursor is keyed by the adapter's
// reported SessionID, never by the ref's Value lookup key.
func (c *UsageCollector) diffAndRecord(totals UsageTotals) {
	prev, seen := c.cursors[totals.SessionID]
	if !seen {
		// First observation: seed the cursor silently. Emitting the full
		// cumulative total now would fabricate a spike that predates this
		// process; the delta the next poll observes is the accrual since
		// this seed.
		c.cursors[totals.SessionID] = totals
		return
	}

	d := diffTotals(prev, totals)
	if anyNegative(d) {
		// The source reset (DB recreated, session pruned): a decrease is not a
		// negative delta. Re-seed the cursor at the fresh totals and emit
		// nothing — the next poll observes real accrual from the new baseline.
		slog.Warn("usage totals decreased; re-seeding cursor", "session_id", totals.SessionID)
		c.cursors[totals.SessionID] = totals
		return
	}
	if usageDeltaZero(d) {
		// Nothing accrued since last poll. Cursor is already these totals.
		return
	}
	c.cursors[totals.SessionID] = totals
	c.emitDelta(d)
}

// diffTotals computes the per-field delta between two cumulative totals.
func diffTotals(prev, cur UsageTotals) UsageDelta {
	return UsageDelta{
		SessionID:        cur.SessionID,
		CostUSD:          cur.CostUSD - prev.CostUSD,
		InputTokens:      cur.InputTokens - prev.InputTokens,
		OutputTokens:     cur.OutputTokens - prev.OutputTokens,
		ReasoningTokens:  cur.ReasoningTokens - prev.ReasoningTokens,
		CacheReadTokens:  cur.CacheReadTokens - prev.CacheReadTokens,
		CacheWriteTokens: cur.CacheWriteTokens - prev.CacheWriteTokens,
	}
}

// anyNegative reports whether any diffed field went below zero — a source
// reset, not a real negative accrual.
func anyNegative(d UsageDelta) bool {
	return d.CostUSD < 0 ||
		d.InputTokens < 0 ||
		d.OutputTokens < 0 ||
		d.ReasoningTokens < 0 ||
		d.CacheReadTokens < 0 ||
		d.CacheWriteTokens < 0
}

// usageDeltaZero reports whether a delta carries no accrued usage at all,
// ignoring the SessionID field (which is identity, not an amount).
func usageDeltaZero(d UsageDelta) bool {
	return d.CostUSD == 0 &&
		d.InputTokens == 0 &&
		d.OutputTokens == 0 &&
		d.ReasoningTokens == 0 &&
		d.CacheReadTokens == 0 &&
		d.CacheWriteTokens == 0
}
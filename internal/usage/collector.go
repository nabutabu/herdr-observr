package usage

import (
	"context"
	"log/slog"
	"time"

	"github.com/nabutabu/herdr-observr/internal/tracker"
)

// DefaultPollInterval is how often the collector polls each pane's session
// (U1.2). Tuned in the same range as the U0 spike's ~10s probes: fine enough
// granularity for cost/token deltas without hammering the source read.
const DefaultPollInterval = 10 * time.Second

// UsageCollector turns per-session cumulative totals from assistant sources
// into session-scoped usage deltas (U1.2). It ticks on an interval, iterates
// the tracker's currently-open panes, reads each pane's agent_session, and
// dispatches to the adapter registered for that pane's agent type. The
// collector owns all session_id -> last-observed totals state; adapters stay
// stateless across PollUsage calls.
//
// Cursor state (the session_id -> UsageTotals map, plus the adapter routing
// map) is owned solely by the collector goroutine: register adapters before
// Run, then never touch the collector from another goroutine — no locking,
// same single-owner discipline as Subscriber and scope.
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

	// interval is the poll cadence. Defaults to DefaultPollInterval;
	// tests substitute a short one.
	interval time.Duration
}

// NewUsageCollector builds a collector over the given panes accessor and
// initial adapters. Register additional adapters before starting Run.
func NewUsageCollector(panes func() map[string]tracker.PaneState, adapters ...UsageAdapter) *UsageCollector {
	c := &UsageCollector{
		adapters: map[string]UsageAdapter{},
		panes:    panes,
		cursors:  map[string]UsageTotals{},
		deltas:   make(chan UsageDelta, 64),
		interval: DefaultPollInterval,
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

// Run drives the poll loop until ctx is cancelled. Call in its own goroutine,
// mirroring Tracker.Run.
func (c *UsageCollector) Run(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.pollOnce(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// pollOnce polls every currently-open pane's agent session and dispatches to
// the pane's adapter. Unexported so tests drive a single pass directly.
func (c *UsageCollector) pollOnce(ctx context.Context) {
	for _, pane := range c.panes() {
		c.pollPane(ctx, pane)
	}
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
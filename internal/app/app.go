// Package app owns the process lifecycle: it pings the Herdr socket,
// bootstraps from a session.snapshot, subscribes to scoped lifecycle events,
// and keeps the subscription alive under the tracker's drift detection.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nabutabu/herdr-observr/internal/client"
	"github.com/nabutabu/herdr-observr/internal/config"
	"github.com/nabutabu/herdr-observr/internal/events"
	"github.com/nabutabu/herdr-observr/internal/snapshot"
	"github.com/nabutabu/herdr-observr/internal/tracker"
)

const maxInitialAttempts = 5

// evictInterval is how often closed entities are swept from the tracker's
// retention grace window (2.7). Tuned under DefaultGraceWindow (15s) so closed
// entities are retained for [grace, grace+evictInterval) — long enough to
// absorb late events, short enough to bound memory.
const evictInterval = 5 * time.Second

// App owns the subscription lifecycle. A single Tracker is shared between the
// event loop (handleEvent) and the reconciliation watchdog (Run's diffing).
type App struct {
	sub *events.Subscriber
	tr  *tracker.Tracker

	// cfg is the resolved 4.2 plugin config from HERDR_PLUGIN_CONFIG_DIR,
	// threaded into telemetry (exporter options, machine-id override,
	// cardinality flag). Nil means no config file: pure env-driven defaults.
	cfg *config.Config

	// scope is the live coverage of the current subscription: the pane IDs it
	// scopes pane.agent_status_changed to, as returned by SubscribeFromSnapshot
	// at the last (re)subscribe. All lifecycle events are kind-scoped and need
	// no such set; only agent_status_changed is per-pane. Read/written solely
	// on the Run event-loop goroutine — no locking. Nil until Run subscribes.
	scope *scope

	// telemetry owns the process's OTel instruments (Phase 3): the meter, the
	// registered instruments, and the MeterProvider shutdown. Nil means
	// telemetry is disabled and every record method is a no-op.
	telemetry *Telemetry
}

// New returns an App. An optional plugin config (4.2) is attached when
// present; nil keeps the pre-4.2 env-only behavior.
func New(cfg ...*config.Config) *App {
	a := &App{}
	if len(cfg) > 0 {
		a.cfg = cfg[0]
	}
	return a
}

// Run drives the process until ctx is cancelled or the subscription stream
// ends. Any startup failure is returned as an error.
func (a *App) Run(ctx context.Context) error {
	// (3.1) Stand up the OTel SDK first so telemetry covers the whole
	// process lifetime. Failures disable telemetry, never the subscription.
	a.telemetry = NewTelemetry(ctx, a.cfg)
	defer a.telemetry.Shutdown(context.Background())

	if err := a.ping(ctx); err != nil {
		return fmt.Errorf("ping herdr: %w", err)
	}

	paneIDs, sub, resp, ok := subscribeWithBackoff(ctx, maxInitialAttempts)
	if !ok {
		return fmt.Errorf("initial session snapshot failed; aborting")
	}
	a.sub = sub
	a.scope = newScope(paneIDs)
	defer func() { a.sub.Close() }() // reconnect() swaps a.sub; close the last one

	a.tr = tracker.NewTracker()
	a.tr.ApplySnapshot(resp.Snapshot)

	// (3.6/3.7) Wire the live concurrency counts (2.6) to the herdr.agent.*
	// and herdr.workspace.agent.concurrent gauges. Registered here rather than
	// in NewTelemetry because the tracker — the counts source — only exists
	// after bootstrap. a.tr.Counts binds the tracker pointer before the SDK's
	// first collection (10s interval), so there is no race; the callback reads
	// under the tracker's own lock.
	a.telemetry.registerCountGauges(a.cfg, a.tr.Counts)

	// Reconciled diffs reach this callback (still running on the loop
	// goroutine). It never touches `sub` — subscription teardown stays owned by
	// Run's event loop. A report carries either genuine drift (the only signal
	// that tears down and re-creates the subscription) or a benign done→idle
	// seen-flip (2.4), whose application is App.Run's job per the single-writer
	// rule — applied here, before any same-tick resubscribe is signalled, so a
	// re-baseline can't eat a just-closed attention-latency interval.
	resubscribe := make(chan struct{}, 1)
	onReport := func(report tracker.DiffReport) {
		for _, sf := range report.SeenFlips {
			a.tr.ApplySeenFlip(sf.PaneID)
		}
		if !report.Drifted() {
			return
		}
		signalResubscribe(resubscribe)
	}
	go a.tr.Run(ctx, time.Minute, onReport)

	evictTicker := time.NewTicker(evictInterval)
	defer evictTicker.Stop()

	slog.Info("subscribed; open/close a pane or drive an agent to see events (Ctrl-C to stop)")

	for {
		select {
		case ev, ok := <-a.sub.Events():
			if !ok {
				slog.Info("subscription stream ended")
				return nil
			}

			a.handleEvent(ev)
			if a.paneCreatedNeedsResubscribe(ev) {
				// The pane was created after the last snapshot-scoped
				// subscription, so pane.agent_status_changed isn't covering it
				// (herdr only delivers that event for pre-existing panes). Fold
				// it in now rather than waiting up to a reconcile interval for a
				// drift-triggered resubscribe.
				signalResubscribe(resubscribe)
			}
			slog.Info("Parsed Event", "event", ev)

		case err := <-a.sub.Err():
			if err != nil {
				slog.Error("subscription error", "error", err)
			}
			a.reconnect(ctx)

		case al := <-a.tr.AttentionLatency():
			// 3.5: each closed done-but-unseen interval records one
			// herdr.agent.attention_latency histogram sample. Kept as a debug
			// log for local troubleshooting now that the metric owns the
			// signal.
			a.telemetry.recordAttentionLatency(al)
			slog.Debug("attention latency", "pane_id", al.PaneID, "agent", al.Agent, "duration", al.Duration)

		case tr := <-a.tr.Transitions():
			// 3.3: every genuine agent state transition increments the
			// herdr.agent.state.transitions counter; 3.8 additionally emits a
			// herdr.agent.state_change event record and 3.9 a short
			// herdr.agent.state_change span, both carrying the
			// high-cardinality ids the counter excludes.
			a.telemetry.recordTransition(tr)
			a.telemetry.recordTransitionEvent(tr)
			a.telemetry.recordTransitionSpan(tr)
			slog.Debug("agent state change", "pane_id", tr.PaneID, "agent", tr.Agent, "previous", tr.Previous, "new", tr.New)

		case sd := <-a.tr.StateDurations():
			// 3.4: every closed state interval records one
			// herdr.agent.state.duration histogram sample.
			a.telemetry.recordStateDuration(sd)

		case <-resubscribe:
			// Silent-stream guard (0.4): the socket is healthy but the event
			// stream drifted from reality. Re-bootstrap and resubscribe.
			a.reconnect(ctx)

		case <-evictTicker.C:
			// Bounded in-memory retention (2.7): drop closed panes/tabs/agents
			// whose grace window has elapsed. Ran here on the event-loop
			// goroutine so the tracker keeps its single-writer discipline.
			a.tr.EvictExpired()

		case <-ctx.Done():
			slog.Info("shutting down")
			return nil
		}
	}
}

// handleEvent routes a normalized event to the tracker handler for its kind.
// It is called synchronously from Run's event loop — never in its own
// goroutine — so the tracker's state stays single-writer. An unknown kind is
// protocol drift (0.2): log and skip rather than panic the process.
func (a *App) handleEvent(ev events.NormalizedEvent) {
	switch ev.Kind {
	case events.KindWorkspaceCreated:
		a.tr.ApplyWorkspaceCreated(ev)
	case events.KindWorkspaceClosed:
		a.tr.ApplyWorkspaceClosed(ev)
	case events.KindTabCreated:
		a.tr.ApplyTabCreated(ev)
	case events.KindTabClosed:
		a.tr.ApplyTabClosed(ev)
	case events.KindTabRenamed:
		a.tr.ApplyTabRenamed(ev)
	case events.KindPaneCreated:
		a.tr.ApplyPaneCreated(ev)
	case events.KindPaneClosed:
		a.tr.ApplyPaneClosed(ev)
	case events.KindAgentDetected:
		a.tr.ApplyAgentDetected(ev)
	case events.KindAgentStatusChanged:
		a.tr.ApplyAgentStatusChanged(ev)
	default:
		slog.Warn("unhandled event kind; skipping", "kind", ev.Kind)
	}
}

// paneCreatedNeedsResubscribe reports whether a pane.created event concerns a
// pane the current subscription does not cover for status changes. Purely a
// decision predicate: the caller (Run's event loop) owns signaling, keeping
// the single-writer rule intact.
func (a *App) paneCreatedNeedsResubscribe(ev events.NormalizedEvent) bool {
	if ev.Kind != events.KindPaneCreated {
		return false
	}
	return a.scope == nil || !a.scope.subscribed(ev.PaneID) // nil scope = nothing covered
}

// signalResubscribe is a non-blocking send matching the channel's "coalesce
// pending requests" semantics: a full buffer just means a reconnect is already
// queued.
func signalResubscribe(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (a *App) ping(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	raw, err := client.Call(cctx, "ping", map[string]any{})
	cancel()
	if err != nil {
		return err
	}
	slog.Info("ping ok", "response", string(raw))
	return nil
}

// reconnect closes the current subscription and establishes a fresh one with a
// fresh session.snapshot (state may have drifted while we were disconnect),
// re-baselining the tracker so the next reconcile tick doesn't immediately
// re-report the gap.
func (a *App) reconnect(ctx context.Context) {
	a.sub.Close()
	paneIDs, next, resp, ok := subscribeWithBackoff(ctx, 0)
	if ok {
		a.sub = next
		a.scope = newScope(paneIDs)
		a.tr.ApplySnapshot(resp.Snapshot)
	}
}

// subscribeWithBackoff fetches a fresh session.snapshot and establishes a
// subscription scoped to the panes it finds, retrying with capped exponential
// backoff. maxAttempts == 0 means retry forever; otherwise it returns
// success=false after maxAttempts failed attempts.
func subscribeWithBackoff(ctx context.Context, maxAttempts int) ([]string, *events.Subscriber, snapshot.Response, bool) {
	for attempt := 1; maxAttempts == 0 || attempt <= maxAttempts; attempt++ {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		paneIDs, s, resp, ok := events.SubscribeFromSnapshot(cctx)
		cancel()
		if ok {
			return paneIDs, s, resp, true
		}
		backoff := time.Duration(1<<(attempt-1)) * time.Second
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
		slog.Info("snapshot subscribe failed; retrying", "attempt", attempt, "backoff", backoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil, nil, snapshot.Response{}, false
		}
	}
	return nil, nil, snapshot.Response{}, false
}

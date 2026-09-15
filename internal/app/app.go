// Package app owns the process lifecycle: it pings the Herdr socket,
// bootstraps from a session.snapshot, subscribes to scoped lifecycle events,
// and keeps the subscription alive under the tracker's drift detection.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/nabutabu/herdr-scribe/internal/client"
	"github.com/nabutabu/herdr-scribe/internal/events"
	"github.com/nabutabu/herdr-scribe/internal/otel"
	"github.com/nabutabu/herdr-scribe/internal/snapshot"
	"github.com/nabutabu/herdr-scribe/internal/tracker"
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

	// scope is the live coverage of the current subscription: the pane IDs it
	// scopes pane.agent_status_changed to, as returned by SubscribeFromSnapshot
	// at the last (re)subscribe. All lifecycle events are kind-scoped and need
	// no such set; only agent_status_changed is per-pane. Read/written solely
	// on the Run event-loop goroutine — no locking. Nil until Run subscribes.
	scope *scope

	// meter is the OTel meter for this process's telemetry (Phase 3). Non-nil
	// only when the OTel SDK initialized successfully; nil means telemetry is
	// disabled and every instrument method returns a no-op.
	meter metric.Meter

	// transitions is the herdr.agent.state.transitions counter (3.3), defined
	// only when telemetry initialized. Nil means recordTransition no-ops.
	transitions metric.Int64Counter

	// shutdownTelemetry flushes and closes the MeterProvider. Nil when the
	// SDK never initialized.
	shutdownTelemetry func(context.Context) error
}

func New() *App {
	return &App{}
}

// Run drives the process until ctx is cancelled or the subscription stream
// ends. Any startup failure is returned as an error.
func (a *App) Run(ctx context.Context) error {
	// (3.1) Stand up the OTel SDK first so telemetry covers the whole
	// process lifetime. Failures disable telemetry, never the subscription.
	a.initTelemetry(ctx)
	defer a.shutdownTelemetryIfInitialized(context.Background())

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
			// Phase 3.5 replaces this log with an OTel histogram emission.
			slog.Info("attention latency", "pane_id", al.PaneID, "agent", al.Agent, "duration", al.Duration)

		case tr := <-a.tr.Transitions():
			// 3.3: every genuine agent state transition increments the
			// herdr.agent.state.transitions counter.
			a.recordTransition(tr)

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

// initTelemetry initializes the OTel metrics SDK from the environment (3.1):
// OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_SERVICE_NAME (default herdr-telemetry),
// and OTEL_RESOURCE_ATTRIBUTES. The exporter dials lazily, so an unreachable
// collector shows up as dropped exports, not a startup failure (Phase 5.4);
// the only errors here are configuration-level, and both are treated the same
// way: log and run without telemetry.
func (a *App) initTelemetry(ctx context.Context) {
	res, err := otel.BuildResource(ctx)
	if err != nil {
		slog.Warn("OTel resource init failed; telemetry disabled", "error", err)
		return
	}
	mp, shutdown, err := otel.NewMeterProvider(ctx, res)
	if err != nil {
		slog.Warn("OTel SDK init failed; telemetry disabled", "error", err)
		return
	}
	a.meter = mp.Meter(otel.MeterName)
	a.shutdownTelemetry = shutdown
	slog.Info("OTel metrics enabled", "service", otel.ServiceName(res))
	a.registerUpMetric()
	a.registerTransitionCounter()
}

// shutdownTelemetryIfInitialized flushes and closes the MeterProvider. Safe to
// call when telemetry was never initialized.
func (a *App) shutdownTelemetryIfInitialized(ctx context.Context) {
	if a.shutdownTelemetry != nil {
		_ = a.shutdownTelemetry(ctx)
	}
}

// registerUpMetric registers the 3.1 proof metric: herdr.up reports 1 for as
// long as this process is alive and exporting. It doubles as the liveness
// signal later phases alert on.
func (a *App) registerUpMetric() {
	if a.meter == nil {
		return
	}
	_, err := a.meter.Int64ObservableGauge(
		otel.UpMetricName,
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(1)
			return nil
		}),
		metric.WithDescription("1 while the herdr-scribe exporter process is running"),
	)
	if err != nil {
		slog.Warn("registering "+otel.UpMetricName+" failed", "error", err)
	}
}

// registerTransitionCounter registers the 3.3 herdr.agent.state.transitions
// counter. Registration failure (e.g. a stale meter) disables the counter,
// never the subscription.
func (a *App) registerTransitionCounter() {
	if a.meter == nil {
		return
	}
	counter, err := otel.NewTransitionCounter(a.meter)
	if err != nil {
		slog.Warn("registering "+otel.TransitionMetricName+" failed", "error", err)
		return
	}
	a.transitions = counter
}

// recordTransition increments herdr.agent.state.transitions for one genuine
// agent state transition, tagged by agent.type and previous/new state. A nil
// counter (telemetry disabled or registration failed) is a no-op.
func (a *App) recordTransition(tr tracker.AgentTransition) {
	if a.transitions == nil {
		return
	}
	a.transitions.Add(context.Background(), 1,
		metric.WithAttributes(
			attribute.String(otel.AgentTypeKey, tr.Agent),
			attribute.String(otel.AgentPreviousState, string(tr.Previous)),
			attribute.String(otel.AgentStateKey, string(tr.New)),
		),
	)
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

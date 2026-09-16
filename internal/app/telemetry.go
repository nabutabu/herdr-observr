// Package app owns the process lifecycle: it pings the Herdr socket,
// bootstraps from a session.snapshot, subscribes to scoped lifecycle events,
// and keeps the subscription alive under the tracker's drift detection.
package app

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/trace"

	"github.com/nabutabu/herdr-observr/internal/config"
	"github.com/nabutabu/herdr-observr/internal/otel"
	"github.com/nabutabu/herdr-observr/internal/snapshot"
	"github.com/nabutabu/herdr-observr/internal/tracker"
)

// Telemetry owns the process's OTel instruments (Phase 3): the meter, the
// registered instruments, and the MeterProvider shutdown. A nil Telemetry (or
// a nil instrument field) means telemetry is disabled and the corresponding
// record method is a no-op.
type Telemetry struct {
	// meter is the OTel meter for this process's telemetry. Non-nil only when
	// the OTel SDK initialized successfully.
	meter metric.Meter

	// resource is the resolved OTel resource, shared by the meter and logger
	// providers so every exported datum (metric, event record) carries the
	// same service/machine identity. Nil only on the test-constructed paths
	// that never build a resource.
	resource *resource.Resource

	// transitions is the herdr.agent.state.transitions counter (3.3), defined
	// only when telemetry initialized. Nil means recordTransition no-ops.
	transitions metric.Int64Counter

	// stateDurationHistogram is the herdr.agent.state.duration histogram (3.4),
	// defined only when telemetry initialized. Nil means recordStateDuration
	// no-ops.
	stateDurationHistogram metric.Float64Histogram

	// attentionLatencyHistogram is the herdr.agent.attention_latency histogram
	// (3.5), defined only when telemetry initialized. Nil means
	// recordAttentionLatency no-ops.
	attentionLatencyHistogram metric.Float64Histogram

	// logger emits the 3.8 structured state-change events (over the OTLP Logs
	// signal), defined only when the logs SDK initialized. Nil means
	// recordTransitionEvent no-ops.
	logger log.Logger

	// loggerShutdown flushes and closes the LoggerProvider. Nil when the logs
	// SDK never initialized.
	loggerShutdown func(context.Context) error

	// tracer emits the 3.9 short state-change spans (over the OTLP Traces
	// signal), defined only when the traces SDK initialized. Nil means
	// recordTransitionSpan no-ops.
	tracer trace.Tracer

	// tracerShutdown flushes and closes the TracerProvider. Nil when the traces
	// SDK never initialized.
	tracerShutdown func(context.Context) error

	// shutdown flushes and closes the MeterProvider. Nil when the SDK never
	// initialized.
	shutdown func(context.Context) error
}

// NewTelemetry initializes the OTel SDKs from the environment (3.1) and an
// optional 4.2 plugin config file (otp cfg; nil keeps pure env behavior): the
// metrics provider (endpoint from config-or-OTEL_EXPORTER_OTLP_ENDPOINT,
// OTEL_SERVICE_NAME defaulting to herdr-telemetry, OTEL_RESOURCE_ATTRIBUTES)
// and, for 3.8's structured state-change events and 3.9's state-change spans,
// the logs and traces providers over the same resource and endpoint. Each
// exporter dials lazily, so an unreachable collector shows up as dropped
// exports, not a startup failure (Phase 5.4); the only errors here are
// configuration-level, and each signal is treated the same way: log and run
// without that signal. With instruments registered it returns the constructed
// Telemetry; nil means telemetry is disabled and every record method no-ops.
func NewTelemetry(ctx context.Context, cfg ...*config.Config) *Telemetry {
	c := firstConfig(cfg)
	res, err := otel.BuildResource(ctx, c)
	if err != nil {
		slog.Warn("OTel resource init failed; telemetry disabled", "error", err)
		return nil
	}
	mp, shutdown, err := otel.NewMeterProvider(ctx, res, c)
	if err != nil {
		slog.Warn("OTel SDK init failed; telemetry disabled", "error", err)
		return nil
	}
	t := &Telemetry{meter: mp.Meter(otel.MeterName), resource: res, shutdown: shutdown}
	slog.Info("OTel metrics enabled", "service", otel.ServiceName(res))
	t.registerUp()
	t.registerLogger(c)
	t.registerTracers(c)
	t.registerTransitions()
	t.registerStateDurations()
	t.registerAttentionLatency()
	return t
}

// firstConfig picks the single optional plugin config, or nil.
func firstConfig(cfg []*config.Config) *config.Config {
	if len(cfg) == 0 {
		return nil
	}
	return cfg[0]
}

// Shutdown flushes and closes the providers. Safe on a nil Telemetry and when
// telemetry was never initialized.
func (t *Telemetry) Shutdown(ctx context.Context) {
	if t == nil {
		return
	}
	if t.shutdown != nil {
		_ = t.shutdown(ctx)
	}
	if t.loggerShutdown != nil {
		_ = t.loggerShutdown(ctx)
	}
	if t.tracerShutdown != nil {
		_ = t.tracerShutdown(ctx)
	}
}

// registerUp registers the 3.1 proof metric: herdr.up reports 1 for as long as
// this process is alive and exporting. It doubles as the liveness signal later
// phases alert on.
func (t *Telemetry) registerUp() {
	if t.meter == nil {
		return
	}
	_, err := t.meter.Int64ObservableGauge(
		otel.UpMetricName,
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(1)
			return nil
		}),
		metric.WithDescription("1 while the herdr-observr exporter process is running"),
	)
	if err != nil {
		slog.Warn("registering "+otel.UpMetricName+" failed", "error", err)
	}
}

// registerLogger initializes the 3.8 logs signal: a LoggerProvider over the
// same resource as the metrics provider, exporting state-change event records
// on the OTLP Logs path of the same OTLP endpoint (config-driven exporter
// options per 4.2). Initialization failure (e.g. a stale configuration)
// disables state-change events only, never the subscription.
func (t *Telemetry) registerLogger(c *config.Config) {
	if t.meter == nil {
		return
	}
	lp, lshutdown, err := otel.NewLoggerProvider(context.Background(), t.resource, c)
	if err != nil {
		slog.Warn("OTel logs SDK init failed; state-change events disabled", "error", err)
		return
	}
	t.logger = lp.Logger(otel.LoggerName)
	t.loggerShutdown = lshutdown
	slog.Info("OTel logs enabled", "event", otel.StateChangeEventName)
}

// registerTracers initializes the 3.9 traces signal: a TracerProvider over the
// same resource as the metrics provider, exporting state-change spans on the
// OTLP Traces path of the same OTLP endpoint (config-driven exporter options
// per 4.2). Initialization failure (e.g. a stale configuration) disables
// state-change spans only, never the subscription.
func (t *Telemetry) registerTracers(c *config.Config) {
	if t.meter == nil {
		return
	}
	tp, tshutdown, err := otel.NewTracerProvider(context.Background(), t.resource, c)
	if err != nil {
		slog.Warn("OTel traces SDK init failed; state-change spans disabled", "error", err)
		return
	}
	t.tracer = tp.Tracer(otel.TracerName)
	t.tracerShutdown = tshutdown
	slog.Info("OTel traces enabled", "span", otel.StateChangeEventName)
}

// recordTransitionSpan emits one short herdr.agent.state_change span (3.9) for
// a genuine agent state transition, carrying the same high-cardinality ids as
// the 3.8 event record (herdr.agent.id and herdr.workspace.id) plus agent.type
// and previous/current state. Root span, started at the transition's observed
// time and ended immediately — never open-ended. A nil Telemetry or tracer
// (telemetry disabled or init failed) is a no-op.
//
// herdr.state.source is deliberately absent, same as 3.8: the plan reserved it
// for pane.report_agent's source field, which is verified-absent from pushed
// status events (finding #3).
func (t *Telemetry) recordTransitionSpan(tr tracker.AgentTransition) {
	if t == nil || t.tracer == nil {
		return
	}
	_, span := t.tracer.Start(context.Background(), otel.StateChangeEventName,
		trace.WithTimestamp(tr.ObservedAt),
	)
	span.SetAttributes(
		attribute.String(otel.AgentIDKey, tr.PaneID),
		attribute.String(otel.AgentTypeKey, tr.Agent),
		attribute.String(otel.WorkspaceIDKey, tr.WorkspaceID),
		attribute.String(otel.AgentPreviousState, string(tr.Previous)),
		attribute.String(otel.AgentStateKey, string(tr.New)),
	)
	span.End()
}

// recordTransitionEvent emits one herdr.agent.state_change event record (3.8)
// for a genuine agent state transition, tagged with the high-cardinality
// resource ids the 3.3 counter excludes (herdr.agent.id and
// herdr.workspace.id) plus agent.type and previous/current state. A nil
// Telemetry or logger (telemetry disabled or init failed) is a no-op.
//
// herdr.state.source is deliberately absent — the plan reserved it for
// pane.report_agent's source field, which is verified-absent from pushed
// status events (finding #3), so emitting it would be a permanent no-op.
func (t *Telemetry) recordTransitionEvent(tr tracker.AgentTransition) {
	if t == nil || t.logger == nil {
		return
	}
	rec := log.Record{}
	rec.SetEventName(otel.StateChangeEventName)
	rec.SetTimestamp(tr.ObservedAt)
	rec.SetSeverity(log.SeverityInfo)
	rec.SetBody(attribute.StringValue(otel.StateChangeEventBody))
	rec.AddAttributes(
		attribute.String(otel.AgentIDKey, tr.PaneID),
		attribute.String(otel.AgentTypeKey, tr.Agent),
		attribute.String(otel.WorkspaceIDKey, tr.WorkspaceID),
		attribute.String(otel.AgentPreviousState, string(tr.Previous)),
		attribute.String(otel.AgentStateKey, string(tr.New)),
	)
	t.logger.Emit(context.Background(), rec)
}

// registerTransitions registers the 3.3 herdr.agent.state.transitions counter.
// Registration failure (e.g. a stale meter) disables the counter, never the
// subscription.
func (t *Telemetry) registerTransitions() {
	if t.meter == nil {
		return
	}
	counter, err := otel.NewTransitionCounter(t.meter)
	if err != nil {
		slog.Warn("registering "+otel.TransitionMetricName+" failed", "error", err)
		return
	}
	t.transitions = counter
}

// recordTransition increments herdr.agent.state.transitions for one genuine
// agent state transition, tagged by agent.type and previous/new state. A nil
// Telemetry or counter (telemetry disabled or registration failed) is a no-op.
func (t *Telemetry) recordTransition(tr tracker.AgentTransition) {
	if t == nil || t.transitions == nil {
		return
	}
	t.transitions.Add(context.Background(), 1,
		metric.WithAttributes(
			attribute.String(otel.AgentTypeKey, tr.Agent),
			attribute.String(otel.AgentPreviousState, string(tr.Previous)),
			attribute.String(otel.AgentStateKey, string(tr.New)),
		),
	)
}

// registerStateDurations registers the 3.4 herdr.agent.state.duration
// histogram. Registration failure (e.g. a stale meter) disables the histogram,
// never the subscription.
func (t *Telemetry) registerStateDurations() {
	if t.meter == nil {
		return
	}
	hist, err := otel.NewDurationHistogram(t.meter)
	if err != nil {
		slog.Warn("registering "+otel.DurationMetricName+" failed", "error", err)
		return
	}
	t.stateDurationHistogram = hist
}

// recordStateDuration records one herdr.agent.state.duration histogram sample
// for a closed state interval, tagged by agent.type and the state whose
// interval just closed. A nil Telemetry or histogram (telemetry disabled or
// registration failed) is a no-op. Durations are recorded in seconds per the
// instrument unit (3.4).
func (t *Telemetry) recordStateDuration(sd tracker.StateDuration) {
	if t == nil || t.stateDurationHistogram == nil {
		return
	}
	t.stateDurationHistogram.Record(context.Background(), sd.Duration.Seconds(),
		metric.WithAttributes(
			attribute.String(otel.AgentTypeKey, sd.Agent),
			attribute.String(otel.AgentStateKey, string(sd.State)),
		),
	)
}

// registerAttentionLatency registers the 3.5 herdr.agent.attention_latency
// histogram. Registration failure (e.g. a stale meter) disables the histogram,
// never the subscription.
func (t *Telemetry) registerAttentionLatency() {
	if t.meter == nil {
		return
	}
	hist, err := otel.NewAttentionLatencyHistogram(t.meter)
	if err != nil {
		slog.Warn("registering "+otel.AttentionLatencyMetricName+" failed", "error", err)
		return
	}
	t.attentionLatencyHistogram = hist
}

// recordAttentionLatency records one herdr.agent.attention_latency histogram
// sample for a closed done-but-unseen interval, tagged by agent.type. A nil
// Telemetry or histogram (telemetry disabled or registration failed) is a
// no-op. Durations are recorded in seconds per the instrument unit (3.5).
func (t *Telemetry) recordAttentionLatency(al tracker.AttentionLatency) {
	if t == nil || t.attentionLatencyHistogram == nil {
		return
	}
	t.attentionLatencyHistogram.Record(context.Background(), al.Duration.Seconds(),
		metric.WithAttributes(
			attribute.String(otel.AgentTypeKey, al.Agent),
		),
	)
}

// registerCountGauges registers the 3.6 herdr.agent.{active,blocked,idle,done,
// unknown} gauges and the 3.7 herdr.workspace.agent.concurrent gauge, and wires
// them to the tracker's live concurrency counts (2.6). Called from App.Run
// once the tracker exists — unlike the 3.3–3.5 instruments these need a counts
// source, so registration is not part of NewTelemetry. Registration failure
// (e.g. a stale meter) disables the gauges, never the subscription.
//
// The 4.2 HERDR_OBSRVR_EMIT_PER_WORKSPACE_GAUGES config flag (cfg) gates the
// per-workspace datapoints: when false, the 3.6 gauges observe only their
// global datapoint and the 3.7 per-workspace gauge is not registered at all —
// a cardinality opt-out for backends that cannot take 5 x N_workspaces series
// per machine. Default (nil cfg or flag unset) is the always-emit behavior
// with explicit zeros.
func (t *Telemetry) registerCountGauges(cfg *config.Config, counts func() tracker.AgentCounts) {
	if t == nil || t.meter == nil {
		return
	}
	emitWs := cfg == nil || cfg.EmitPerWorkspaceGaugesDefault()

	gauges, err := otel.NewAgentCountGauges(t.meter)
	if err != nil {
		slog.Warn("registering agent count gauges failed", "error", err)
		return
	}

	var concurrent metric.Int64ObservableGauge
	instruments := []metric.Observable{
		gauges.Active, gauges.Blocked, gauges.Idle, gauges.Done, gauges.Unknown,
	}
	if emitWs {
		concurrent, err = otel.NewWorkspaceConcurrentGauge(t.meter)
		if err != nil {
			slog.Warn("registering workspace concurrent gauge failed", "error", err)
			return
		}
		instruments = append(instruments, concurrent)
	}

	// stateGauges maps each tracked agent state to its 3.6 gauge; the gauge's
	// metric name already encodes the state, so datapoints need no state
	// attribute. Active is the working count; done/unknown are gauged too so
	// all five model states are visible.
	stateGauges := []struct {
		gauge metric.Int64ObservableGauge
		state snapshot.AgentStatus
	}{
		{gauges.Active, snapshot.AgentStatusWorking},
		{gauges.Blocked, snapshot.AgentStatusBlocked},
		{gauges.Idle, snapshot.AgentStatusIdle},
		{gauges.Done, snapshot.AgentStatusDone},
		{gauges.Unknown, snapshot.AgentStatusUnknown},
	}

	// One callback serves all registered gauges so the tracker's Counts()
	// (deep-copy snapshot under its own lock) is taken once per collection.
	// Each 3.6 gauge observes a global datapoint plus, when emitWs, one per
	// workspace carrying herdr.workspace.id — always emitted (bounded at 5 x
	// N_workspaces per machine; see NewAgentCountGauges for the config-flag
	// note). Absent map entries read as zero, so empty states still emit an
	// explicit 0 and their Prometheus series never go stale. The 3.7 gauge
	// observes only the per-workspace working+blocked sum (no global point),
	// same always-emitted workspace dimension and same single Counts().
	_, err = t.meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		c := counts()
		for _, sg := range stateGauges {
			o.ObserveInt64(sg.gauge, int64(c.Global[sg.state]))
			if !emitWs {
				continue
			}
			// Observe every state for each workspace that has any agents, not
			// just the states present in its bucket, so per-workspace series
			// stay present across state changes.
			for wsID, wsCounts := range c.Workspace {
				o.ObserveInt64(sg.gauge, int64(wsCounts[sg.state]),
					metric.WithAttributes(attribute.String(otel.WorkspaceIDKey, wsID)),
				)
			}
		}
		if emitWs {
			for wsID, wsCounts := range c.Workspace {
				o.ObserveInt64(concurrent,
					int64(wsCounts[snapshot.AgentStatusWorking])+int64(wsCounts[snapshot.AgentStatusBlocked]),
					metric.WithAttributes(attribute.String(otel.WorkspaceIDKey, wsID)),
				)
			}
		}
		return nil
	}, instruments...)
	if err != nil {
		slog.Warn("registering agent count gauge callback failed", "error", err)
	}
}

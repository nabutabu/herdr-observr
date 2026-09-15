// Package app owns the process lifecycle: it pings the Herdr socket,
// bootstraps from a session.snapshot, subscribes to scoped lifecycle events,
// and keeps the subscription alive under the tracker's drift detection.
package app

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/nabutabu/herdr-scribe/internal/otel"
	"github.com/nabutabu/herdr-scribe/internal/tracker"
)

// Telemetry owns the process's OTel instruments (Phase 3): the meter, the
// registered instruments, and the MeterProvider shutdown. A nil Telemetry (or
// a nil instrument field) means telemetry is disabled and the corresponding
// record method is a no-op.
type Telemetry struct {
	// meter is the OTel meter for this process's telemetry. Non-nil only when
	// the OTel SDK initialized successfully.
	meter metric.Meter

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

	// shutdown flushes and closes the MeterProvider. Nil when the SDK never
	// initialized.
	shutdown func(context.Context) error
}

// NewTelemetry initializes the OTel metrics SDK from the environment (3.1):
// OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_SERVICE_NAME (default herdr-telemetry),
// and OTEL_RESOURCE_ATTRIBUTES. The exporter dials lazily, so an unreachable
// collector shows up as dropped exports, not a startup failure (Phase 5.4);
// the only errors here are configuration-level, and both are treated the same
// way: log and run without telemetry. With instruments registered it returns
// the constructed Telemetry; nil means telemetry is disabled and every record
// method no-ops.
func NewTelemetry(ctx context.Context) *Telemetry {
	res, err := otel.BuildResource(ctx)
	if err != nil {
		slog.Warn("OTel resource init failed; telemetry disabled", "error", err)
		return nil
	}
	mp, shutdown, err := otel.NewMeterProvider(ctx, res)
	if err != nil {
		slog.Warn("OTel SDK init failed; telemetry disabled", "error", err)
		return nil
	}
	t := &Telemetry{meter: mp.Meter(otel.MeterName), shutdown: shutdown}
	slog.Info("OTel metrics enabled", "service", otel.ServiceName(res))
	t.registerUp()
	t.registerTransitions()
	t.registerStateDurations()
	t.registerAttentionLatency()
	return t
}

// Shutdown flushes and closes the MeterProvider. Safe on a nil Telemetry and
// when telemetry was never initialized.
func (t *Telemetry) Shutdown(ctx context.Context) {
	if t == nil || t.shutdown == nil {
		return
	}
	_ = t.shutdown(ctx)
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
		metric.WithDescription("1 while the herdr-scribe exporter process is running"),
	)
	if err != nil {
		slog.Warn("registering "+otel.UpMetricName+" failed", "error", err)
	}
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

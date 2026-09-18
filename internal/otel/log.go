// internal/otel/log.go
package otel

import (
	"context"
	"fmt"

	"github.com/nabutabu/herdr-observr/internal/config"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	apilog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
)

// (3.8) herdr.agent.state_change — a structured OTel *event* emitted once per
// genuine agent state transition (2.3), over the OTLP Logs signal. Each event
// is tagged with the high-cardinality resource ids the 3.3 counter and 3.4
// histogram deliberately exclude (herdr.agent.id and herdr.workspace.id) plus
// agent.type and previous/current state, so a backend can correlate a single
// agent's whole transition history. Unlike 3.3-3.7's bounded-cardinality
// instruments, event records are transient log records, not long-lived series:
// volume is bounded by transition rate, and every id is potentially distinct,
// so there is no cardinality concern.
//
// Naming: the log record is an *event record* per the OTel spec — its event
// name (StateChangeEventName) is what makes it one. It deliberately shares the
// herdr.agent.state_change name with the 3.9 span so transition telemetry can
// be correlated across the logs and traces signals in a single backend.
//
// Privacy constraint: attributes are ids/states/timestamps only. No pane
// content, terminal output, or agent transcript ever rides on an event record.
const (
	// LoggerName is the instrumentation scope name for all herdr-observr
	// loggers, mirroring MeterName on the metrics side.
	LoggerName = "herdr-observr"

	// StateChangeEventName is the event name on every 3.8 transition event
	// record. Shared with 3.9's herdr.agent.state_change span.
	StateChangeEventName = "herdr.agent.state_change"

	// StateChangeEventBody is the fixed record body on every 3.8 transition
	// event. Constant by design: the structured data rides in the attributes,
	// so the body never participates in cardinality and backends that surface
	// bodies verbatim see a stable, greppable value.
	StateChangeEventBody = "agent state transition"

	// Attribute keys on StateChangeEventName records. AgentIDKey is the
	// pane-scoped agent handle: the wire carries no standalone agent id —
	// pushed pane.agent_status_changed is only {agent, agent_status, pane_id,
	// workspace_id} (confirmed, finding #3) and the tracker keys agents by pane
	// id — so herdr.agent.id IS pane id by construction.
	AgentIDKey = "herdr.agent.id"

	// herdr.state.source (the plan's sixth attribute, from pane.report_agent's
	// source field) is deliberately NOT emitted: that field is verified-absent
	// from pushed status events (finding #3), so it would be a permanent no-op.
	// If the wire ever carries a source, add capture in
	// internal/events/event.go's eventPayload and emit the attribute here when
	// non-empty.
)

// NewLoggerProvider initializes the OTel logs SDK with the OTLP/gRPC exporter
// and associates it with res.
//
// The exporter reads its whole configuration from the environment (the same
// OTEL_EXPORTER_OTLP_* variables as the metrics exporter; logs ride the
// /v1/logs path of the same endpoint). A 4.2 plugin config file overrides the
// keys it explicitly sets, exactly like the metrics path (see NewMeterProvider
// and logOTLPOptions); logs TLS/insecure follow the same config-file-driven
// rule.
//
// The gRPC connection is established lazily (grpc.NewClient): construction
// never blocks or fails because the collector is unreachable — export batches
// fail and are dropped on the batch processor's cadence instead, so the process
// keeps tracking state with no collector (Phase 5.4). Records are exported via
// the SDK's batch processor with its default ~1s schedule, so events are
// near-real-time without being synchronously coupled to Emit.
//
// The returned func flushes and shuts down the provider. Callers that receive
// an error (configuration-level failure only) should log and continue without
// event telemetry rather than aborting the process — exactly the metrics
// path's behavior.
func NewLoggerProvider(ctx context.Context, res *resource.Resource, cfg ...*config.Config) (apilog.LoggerProvider, func(context.Context) error, error) {
	exporter, err := otlploggrpc.New(ctx, logOTLPOptions(firstConfig(cfg))...)
	if err != nil {
		return nil, nil, fmt.Errorf("creating OTLP gRPC logs exporter: %w", err)
	}

	processor := log.NewBatchProcessor(exporter)
	lp := newLoggerProviderWithProcessor(processor, res)

	// Install as the global so any instrumentation that doesn't receive the
	// provider explicitly still exports through it.
	global.SetLoggerProvider(lp)

	return lp, lp.Shutdown, nil
}

// newLoggerProviderWithProcessor builds a LoggerProvider over an explicit
// Processor. Separated from NewLoggerProvider so tests can capture records
// synchronously (NewSimpleProcessor) instead of through a live batch processor.
func newLoggerProviderWithProcessor(processor log.Processor, res *resource.Resource) *log.LoggerProvider {
	return log.NewLoggerProvider(log.WithResource(res), log.WithProcessor(processor))
}

// NewLoggerProviderWithProcessor builds a LoggerProvider over an explicit
// Processor, exported for cross-package tests that capture emitted records
// without a live collector (the app tests drive a simple processor the same
// way the package-internal tests do). Not part of the runtime configuration;
// production code should use NewLoggerProvider.
func NewLoggerProviderWithProcessor(processor log.Processor, res *resource.Resource) *log.LoggerProvider {
	return newLoggerProviderWithProcessor(processor, res)
}

// logOTLPOptions is the logs analogue of metricOTLPOptions: exporter options
// only for keys the 4.2 config file explicitly set, and the historical
// plaintext WithInsecure() pin when there is no config.
func logOTLPOptions(c *config.Config) []otlploggrpc.Option {
	if c == nil {
		return []otlploggrpc.Option{otlploggrpc.WithInsecure()}
	}
	var opts []otlploggrpc.Option
	if c.OTLP.InsecureDefault() {
		opts = append(opts, otlploggrpc.WithInsecure())
	}
	if c.OTLP.Endpoint != nil {
		opts = append(opts, otlploggrpc.WithEndpoint(*c.OTLP.Endpoint))
	}
	if c.OTLP.Timeout != nil {
		opts = append(opts, otlploggrpc.WithTimeout(*c.OTLP.Timeout))
	}
	if c.OTLP.Headers != nil {
		opts = append(opts, otlploggrpc.WithHeaders(config.ParseHeaders(*c.OTLP.Headers)))
	}
	return opts
}

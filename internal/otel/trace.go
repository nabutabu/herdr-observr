// internal/otel/trace.go
package otel

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	apitrace "go.opentelemetry.io/otel/trace"
)

// (3.9) herdr.agent.state_change — a short-lived span emitted once per genuine
// agent state transition (2.3), over the OTLP Traces signal. It deliberately
// shares the herdr.agent.state_change name with the 3.8 event record (see
// StateChangeEventName in log.go) so transition telemetry correlates across the
// logs and traces signals in a single backend, keyed by herdr.agent.id.
//
// Each span is a *root* span: the event stream carries no parent context, and
// the plan explicitly rejects an open-ended session span as a parent, so these
// mini-traces stand alone and are ended milliseconds after they start — never
// open indefinitely (the Phase 3 exit criterion). The start timestamp is the
// transition's ObservedAt so the span lands at the moment the change happened,
// and it carries the same high-cardinality attributes as 3.8 (agent.id,
// workspace.id, agent.type, previous/new state). herdr.state.source stays
// absent, same as 3.8 (finding #3: no source field on the wire).
//
// Privacy constraint: attributes are ids/states/timestamps only. No pane
// content, terminal output, or agent transcript ever rides on a span.
const (
	// TracerName is the instrumentation scope name for all herdr-observr
	// tracers, mirroring MeterName on the metrics side and LoggerName on the
	// logs side.
	TracerName = "herdr-observr"
)

// NewTracerProvider initializes the OTel traces SDK with the OTLP/gRPC
// exporter and associates it with res.
//
// The exporter reads its whole configuration from the environment (the same
// OTEL_EXPORTER_OTLP_* variables as the metrics/logs exporters; traces ride
// the /v1/traces path of the same endpoint), so no endpoint plumbing is needed
// here. WithInsecure pins the SDK to a plaintext local collector, mirroring
// NewMeterProvider and NewLoggerProvider.
//
// The gRPC connection is established lazily (grpc.NewClient): construction
// never blocks or fails because the collector is unreachable — export batches
// fail and are dropped on the batch processor's cadence instead, so the process
// keeps tracking state with no collector (Phase 5.4). Spans are exported via
// the SDK's batch span processor, so recording a transition never blocks on
// the exporter.
//
// The returned func flushes and shuts down the provider. Callers that receive
// an error (configuration-level failure only) should log and continue without
// span telemetry rather than aborting the process — exactly the metrics path's
// behavior.
func NewTracerProvider(ctx context.Context, res *resource.Resource) (apitrace.TracerProvider, func(context.Context) error, error) {
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, nil, fmt.Errorf("creating OTLP gRPC traces exporter: %w", err)
	}

	tp := newTracerProviderWithProcessor(sdktrace.NewBatchSpanProcessor(exporter), res)

	// Install as the global so any instrumentation that doesn't receive the
	// provider explicitly still exports through it.
	otel.SetTracerProvider(tp)

	return tp, tp.Shutdown, nil
}

// newTracerProviderWithProcessor builds a TracerProvider over an explicit
// SpanProcessor. Separated from NewTracerProvider so tests can capture spans
// synchronously (NewSimpleSpanProcessor) instead of through a live batch
// processor.
func newTracerProviderWithProcessor(processor sdktrace.SpanProcessor, res *resource.Resource) *sdktrace.TracerProvider {
	return sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithSpanProcessor(processor))
}

// NewTracerProviderWithProcessor builds a TracerProvider over an explicit
// SpanProcessor, exported for cross-package tests that capture emitted spans
// without a live collector (the app tests drive a simple processor the same
// way the package-internal tests do). Not part of the runtime configuration;
// production code should use NewTracerProvider.
func NewTracerProviderWithProcessor(processor sdktrace.SpanProcessor, res *resource.Resource) *sdktrace.TracerProvider {
	return newTracerProviderWithProcessor(processor, res)
}

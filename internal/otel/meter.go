package otel

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	apimetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

const (
	// exportInterval is how often the periodic reader collects and exports
	// metric data. Short enough that the agent-state gauges of 3.6/3.7 stay
	// current; the OTLP exporter itself retries individual export attempts so
	// a collector blip is absorbed rather than losing a sample outright.
	exportInterval = 10 * time.Second

	// MeterName is the instrumentation scope name for all herdr-scribe meters.
	MeterName = "herdr-scribe"

	// UpMetricName is the 3.1 proof/liveness metric: 1 while this process is
	// alive and exporting. Registered by App on startup.
	UpMetricName = "herdr.up"
)

// NewMeterProvider initializes the OTel metrics SDK with the OTLP/gRPC
// exporter and associates it with res.
//
// The exporter reads its whole configuration from the environment
// (OTEL_EXPORTER_OTLP_ENDPOINT defaulting to https://localhost:4317,
// OTEL_EXPORTER_OTLP_INSECURE, OTEL_EXPORTER_OTLP_HEADERS, ...), so no
// endpoint plumbing is needed here. WithInsecure pins the SDK to a plaintext
// local collector, which is the deployment this plugin targets; promoting TLS
// to a config flag is deferred to the Phase 4.2 plugin config.
//
// The gRPC connection is established lazily (grpc.NewClient): construction
// never blocks or fails because the collector is unreachable — individual
// exports fail and are dropped instead, so the process keeps tracking state
// with no exporter (Phase 5.4).
//
// The returned func flushes and shuts down the provider. Callers that receive
// an error (configuration-level failure only) should log and continue without
// telemetry rather than aborting the process.
func NewMeterProvider(ctx context.Context, res *resource.Resource) (apimetric.MeterProvider, func(context.Context) error, error) {
	exporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithInsecure())
	if err != nil {
		return nil, nil, fmt.Errorf("creating OTLP gRPC metrics exporter: %w", err)
	}

	reader := metric.NewPeriodicReader(exporter, metric.WithInterval(exportInterval))
	mp := newMeterProviderWithReader(reader, res)

	// Install as the global so any instrumentation that doesn't receive the
	// provider explicitly still exports through it.
	otel.SetMeterProvider(mp)

	return mp, mp.Shutdown, nil
}

// newMeterProviderWithReader builds a MeterProvider over an explicit Reader.
// Separated from NewMeterProvider so tests can drive collection with a manual
// reader instead of a live collector.
func newMeterProviderWithReader(reader metric.Reader, res *resource.Resource) *metric.MeterProvider {
	return metric.NewMeterProvider(metric.WithReader(reader), metric.WithResource(res))
}

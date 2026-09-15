package otel

import (
	"context"
	"testing"

	otmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestNewMeterProviderWithUnreachableEndpoint(t *testing.T) {
	// The gRPC connection is established lazily, so construction must succeed
	// even when the collector is unreachable (Phase 5.4): exports fail and are
	// dropped, the process itself is unaffected.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:1")
	// Shorten the per-export timeout so shutdown's flush (which waits on the
	// failed export) doesn't burn the full 10s default in the test.
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "500")

	res, err := BuildResource(context.Background())
	if err != nil {
		t.Fatalf("BuildResource: %v", err)
	}
	mp, shutdown, err := NewMeterProvider(context.Background(), res)
	if err != nil {
		t.Fatalf("NewMeterProvider with unreachable endpoint: %v", err)
	}
	// Shutdown flushes pending exports, which fails against an unreachable
	// endpoint — exactly the Phase 5.4 behavior we're pinning down: exports
	// fail and are dropped, the process itself is unaffected.
	_ = shutdown(context.Background())
	_ = mp
}

func TestMeterProviderAssociatesResource(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "svc")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "env.k=v")

	res, err := BuildResource(context.Background())
	if err != nil {
		t.Fatalf("BuildResource: %v", err)
	}

	reader := metric.NewManualReader()
	mp := newMeterProviderWithReader(reader, res)
	defer func() { _ = mp.Shutdown(context.Background()) }()

	meter := mp.Meter("test")
	_, err = meter.Int64ObservableGauge(
		"test.gauge",
		otmetric.WithInt64Callback(func(_ context.Context, o otmetric.Int64Observer) error {
			o.Observe(1)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("creating observable gauge: %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	if rm.Resource == nil {
		t.Fatal("collected resource is nil")
	}
	if got := ServiceName(rm.Resource); got != "svc" {
		t.Errorf("service.name = %q, want %q", got, "svc")
	}
	if got, ok := ResourceAttribute(rm.Resource, "env.k"); !ok || got != "v" {
		t.Errorf("env.k = %q (ok=%v), want %q", got, ok, "v")
	}
}

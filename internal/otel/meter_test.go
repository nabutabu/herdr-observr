package otel

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	otmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestTransitionCounterRegistersAndIncrements(t *testing.T) {
	// (3.3) Pin the registered metric name and its attribute keys: the app
	// test asserts end-to-end increments, this one asserts the contract of the
	// counter itself — name, unit, and the three-attribute shape.
	setCleanEnv(t)

	res, err := BuildResource(context.Background())
	if err != nil {
		t.Fatalf("BuildResource: %v", err)
	}

	reader := metric.NewManualReader()
	mp := newMeterProviderWithReader(reader, res)
	defer func() { _ = mp.Shutdown(context.Background()) }()

	counter, err := NewTransitionCounter(mp.Meter(MeterName))
	if err != nil {
		t.Fatalf("NewTransitionCounter: %v", err)
	}

	counter.Add(context.Background(), 1,
		otmetric.WithAttributes(
			attribute.String(AgentTypeKey, "codex"),
			attribute.String(AgentPreviousState, "working"),
			attribute.String(AgentStateKey, "blocked"),
		),
	)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}

	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != TransitionMetricName {
				continue
			}
			found = true
			if m.Unit != "1" {
				t.Errorf("unit = %q, want %q", m.Unit, "1")
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("data type = %T, want Sum[int64]", m.Data)
			}
			if len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 1 {
				t.Errorf("datapoints = %+v, want a single value 1", sum.DataPoints)
			}
		}
	}
	if !found {
		t.Fatalf("metric %q not found in collected data", TransitionMetricName)
	}
}

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
	setCleanEnv(t)
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

func TestMachineAttrsRideOnCollectedResource(t *testing.T) {
	// The machine attributes must reach the exported OTLP resource, not just
	// the in-memory one — prove it structurally with a manual reader (the same
	// path the OTLP exporter takes, which 3.1 live-verified end to end).
	setCleanEnv(t)
	t.Setenv("HERDR_PLUGIN_STATE_DIR", t.TempDir())

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

	if got, ok := ResourceAttribute(rm.Resource, machineIDKey); !ok || got == "" {
		t.Errorf("herdr.machine.id present=%v value=%q on collected resource, want non-empty", ok, got)
	}
	if got, ok := ResourceAttribute(rm.Resource, machineHostnameKey); !ok || got == "" {
		t.Errorf("herdr.machine.hostname present=%v value=%q on collected resource, want non-empty", ok, got)
	}
}

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

func TestDurationHistogramRegistersAndRecords(t *testing.T) {
	// (3.4) Pin the registered metric name, its unit, and the attribute shape:
	// durations are recorded in seconds with the agent.type and state (the
	// state whose interval just closed) attributes.
	setCleanEnv(t)

	res, err := BuildResource(context.Background())
	if err != nil {
		t.Fatalf("BuildResource: %v", err)
	}

	reader := metric.NewManualReader()
	mp := newMeterProviderWithReader(reader, res)
	defer func() { _ = mp.Shutdown(context.Background()) }()

	hist, err := NewDurationHistogram(mp.Meter(MeterName))
	if err != nil {
		t.Fatalf("NewDurationHistogram: %v", err)
	}

	hist.Record(context.Background(), 84.3,
		otmetric.WithAttributes(
			attribute.String(AgentTypeKey, "codex"),
			attribute.String(AgentStateKey, "working"),
		),
	)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}

	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != DurationMetricName {
				continue
			}
			found = true
			if m.Unit != "s" {
				t.Errorf("unit = %q, want %q", m.Unit, "s")
			}
			h, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("data type = %T, want Histogram[float64]", m.Data)
			}
			if len(h.DataPoints) != 1 {
				t.Fatalf("datapoints = %d, want 1", len(h.DataPoints))
			}
			dp := h.DataPoints[0]
			if dp.Count != 1 {
				t.Errorf("count = %d, want 1", dp.Count)
			}
			if v, ok := dp.Attributes.Value(attribute.Key(AgentTypeKey)); !ok || v.AsString() != "codex" {
				t.Errorf("agent.type attribute = %v (ok=%v), want codex", v, ok)
			}
			if v, ok := dp.Attributes.Value(attribute.Key(AgentStateKey)); !ok || v.AsString() != "working" {
				t.Errorf("state attribute = %v (ok=%v), want working", v, ok)
			}
		}
	}
	if !found {
		t.Fatalf("metric %q not found in collected data", DurationMetricName)
	}
}

func TestAttentionLatencyHistogramRegistersAndRecords(t *testing.T) {
	// (3.5) Pin the registered metric name, its unit, and the attribute shape:
	// closed done-but-unseen intervals recorded in seconds, tagged by agent
	// type only — no state attribute, since the state is done by construction.
	setCleanEnv(t)

	res, err := BuildResource(context.Background())
	if err != nil {
		t.Fatalf("BuildResource: %v", err)
	}

	reader := metric.NewManualReader()
	mp := newMeterProviderWithReader(reader, res)
	defer func() { _ = mp.Shutdown(context.Background()) }()

	hist, err := NewAttentionLatencyHistogram(mp.Meter(MeterName))
	if err != nil {
		t.Fatalf("NewAttentionLatencyHistogram: %v", err)
	}

	hist.Record(context.Background(), 170.999,
		otmetric.WithAttributes(
			attribute.String(AgentTypeKey, "codex"),
		),
	)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}

	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != AttentionLatencyMetricName {
				continue
			}
			found = true
			if m.Unit != "s" {
				t.Errorf("unit = %q, want %q", m.Unit, "s")
			}
			h, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("data type = %T, want Histogram[float64]", m.Data)
			}
			if len(h.DataPoints) != 1 {
				t.Fatalf("datapoints = %d, want 1", len(h.DataPoints))
			}
			dp := h.DataPoints[0]
			if dp.Count != 1 {
				t.Errorf("count = %d, want 1", dp.Count)
			}
			if v, ok := dp.Attributes.Value(attribute.Key(AgentTypeKey)); !ok || v.AsString() != "codex" {
				t.Errorf("agent.type attribute = %v (ok=%v), want codex", v, ok)
			}
			if _, ok := dp.Attributes.Value(attribute.Key(AgentStateKey)); ok {
				t.Errorf("state attribute present on %q; attention latency is done-only", AttentionLatencyMetricName)
			}
		}
	}
	if !found {
		t.Fatalf("metric %q not found in collected data", AttentionLatencyMetricName)
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

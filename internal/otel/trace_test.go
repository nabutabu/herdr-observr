package otel

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	apitrace "go.opentelemetry.io/otel/trace"
)

// captureSpanExporter is a minimal sdktrace.SpanExporter that snapshots
// exported spans for assertions. Synchronous via a SimpleSpanProcessor, so
// spans are captured at End time — the traces analogue of the metrics tests'
// manual reader and the logs tests' captureExporter.
type captureSpanExporter struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
}

func (c *captureSpanExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.spans = append(c.spans, spans...)
	return nil
}

func (c *captureSpanExporter) Shutdown(context.Context) error { return nil }

func (c *captureSpanExporter) snapshot() []sdktrace.ReadOnlySpan {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]sdktrace.ReadOnlySpan, len(c.spans))
	copy(out, c.spans)
	return out
}

// traceProcessorProvider builds a TracerProvider whose spans synchronously land
// in the given captureSpanExporter, via the package-private provider
// constructor — same seam the metrics and logs tests use.
func traceProcessorProvider(t *testing.T, res *resource.Resource) (*captureSpanExporter, *sdktrace.TracerProvider) {
	t.Helper()
	capt := &captureSpanExporter{}
	tp := newTracerProviderWithProcessor(sdktrace.NewSimpleSpanProcessor(capt), res)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return capt, tp
}

// spanAttributeMap flattens a captured span's attributes by key.
func spanAttributeMap(s sdktrace.ReadOnlySpan) map[string]string {
	m := map[string]string{}
	for _, kv := range s.Attributes() {
		m[string(kv.Key)] = kv.Value.AsString()
	}
	return m
}

func TestStateChangeSpanConstantContract(t *testing.T) {
	// (3.9) Pin the scope name: the tracer must report the same instrumentation
	// scope name as the meter and logger so backends can group all herdr-observr
	// signals by scope. The span name itself is already pinned by
	// TestStateChangeEventConstantContract (StateChangeEventName is shared
	// between 3.8 and 3.9).
	if TracerName != "herdr-observr" {
		t.Errorf("TracerName = %q, want %q", TracerName, "herdr-observr")
	}
}

func TestTracerProviderEmitsShortSpans(t *testing.T) {
	t.Setenv("HERDR_PLUGIN_STATE_DIR", t.TempDir())
	res, err := BuildResource(context.Background())
	if err != nil {
		t.Fatalf("BuildResource: %v", err)
	}

	capt, tp := traceProcessorProvider(t, res)
	trc := tp.Tracer(TracerName)

	// A 3.9 transition span is started at its observed time and ended
	// immediately: a root span (no parent), short by construction. In
	// production the observed time is the tracker's processing instant (ms
	// before End), so model it with a just-now timestamp — the duration bound
	// below is what pins the "short, never open-ended" property.
	observed := time.Now()
	_, span := trc.Start(context.Background(), StateChangeEventName, apitrace.WithTimestamp(observed))
	span.SetAttributes(
		attribute.String(AgentIDKey, "w1:p1"),
		attribute.String(AgentTypeKey, "codex"),
		attribute.String(WorkspaceIDKey, "w1"),
		attribute.String(AgentPreviousState, "working"),
		attribute.String(AgentStateKey, "blocked"),
	)
	span.End()

	got := capt.snapshot()
	if len(got) != 1 {
		t.Fatalf("captured spans = %d, want 1", len(got))
	}
	s := got[0]
	if s.Name() != StateChangeEventName {
		t.Errorf("span name = %q, want %q", s.Name(), StateChangeEventName)
	}
	if s.Parent().IsValid() {
		t.Error("transition span has a parent; 3.9 spans must be root spans (no open-ended session span as parent)")
	}
	if !s.StartTime().Equal(observed) {
		t.Errorf("span start time = %v, want observed time %v", s.StartTime(), observed)
	}
	if s.EndTime().IsZero() {
		t.Error("span end time is zero; transition spans must end, never stay open")
	}
	if d := s.EndTime().Sub(s.StartTime()); d < 0 || d > time.Second {
		t.Errorf("span duration = %v, want short (>= 0 and < 1s)", d)
	}
	attrs := spanAttributeMap(s)
	for key, want := range map[string]string{
		AgentIDKey:         "w1:p1",
		AgentTypeKey:       "codex",
		WorkspaceIDKey:     "w1",
		AgentPreviousState: "working",
		AgentStateKey:      "blocked",
	} {
		if got := attrs[key]; got != want {
			t.Errorf("attribute %q = %q, want %q", key, got, want)
		}
	}
	if attrs["herdr.state.source"] != "" {
		t.Errorf("herdr.state.source unexpectedly present on span: %q (field is verified-absent from the wire, finding #3)", attrs["herdr.state.source"])
	}
}

func TestTracerProviderAssociatesResource(t *testing.T) {
	setCleanEnv(t)
	t.Setenv("HERDR_PLUGIN_STATE_DIR", t.TempDir())

	res, err := BuildResource(context.Background())
	if err != nil {
		t.Fatalf("BuildResource: %v", err)
	}

	capt, tp := traceProcessorProvider(t, res)
	_, span := tp.Tracer(TracerName).Start(context.Background(), StateChangeEventName)
	span.End()

	got := capt.snapshot()
	if len(got) != 1 {
		t.Fatalf("captured spans = %d, want 1", len(got))
	}
	rr := got[0].Resource()
	if rr == nil {
		t.Fatal("captured span resource is nil")
	}
	if got, ok := ResourceAttribute(rr, serviceNameKey); !ok || got != DefaultServiceName {
		t.Errorf("service.name = %q (ok=%v), want default %q", got, ok, DefaultServiceName)
	}
	if got, ok := ResourceAttribute(rr, machineIDKey); !ok || got == "" {
		t.Errorf("herdr.machine.id present=%v value=%q on span resource, want non-empty", ok, got)
	}
	if got, ok := ResourceAttribute(rr, machineHostnameKey); !ok || got == "" {
		t.Errorf("herdr.machine.hostname present=%v value=%q on span resource, want non-empty", ok, got)
	}
}

func TestNewTracerProviderWithUnreachableEndpoint(t *testing.T) {
	// Same invariant as the metrics/logs exporters (Phase 5.4): the gRPC
	// connection is established lazily, so construction must succeed even when
	// the collector is unreachable — export batches fail and are dropped, the
	// process itself is unaffected.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:1")
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "500")

	res, err := BuildResource(context.Background())
	if err != nil {
		t.Fatalf("BuildResource: %v", err)
	}
	tp, shutdown, err := NewTracerProvider(context.Background(), res)
	if err != nil {
		t.Fatalf("NewTracerProvider with unreachable endpoint: %v", err)
	}
	// Shutdown flushes pending export batches, which fail against an
	// unreachable endpoint — exactly the Phase 5.4 behavior we're pinning
	// down: exports fail and are dropped, the process itself is unaffected.
	_ = shutdown(context.Background())
	_ = tp
}

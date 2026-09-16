package otel

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	apilog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
)

// captureExporter is a minimal sdklog.Exporter that snapshots exported records
// for assertions. Synchronous via a SimpleProcessor, so records are captured at
// Emit time — the logs analogue of the metrics tests' manual reader.
type captureExporter struct {
	mu      sync.Mutex
	records []sdklog.Record
}

func (c *captureExporter) Export(_ context.Context, records []sdklog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, records...)
	return nil
}

func (c *captureExporter) Shutdown(context.Context) error { return nil }

func (c *captureExporter) ForceFlush(context.Context) error { return nil }

func (c *captureExporter) snapshot() []sdklog.Record {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]sdklog.Record, len(c.records))
	copy(out, c.records)
	return out
}

// logProcessorProvider builds a LoggerProvider whose records synchronously land
// in the given captureExporter, via the package-private provider constructor —
// same seam the metrics tests use.
func logProcessorProvider(t *testing.T, res *resource.Resource) (*captureExporter, *sdklog.LoggerProvider) {
	t.Helper()
	capt := &captureExporter{}
	lp := newLoggerProviderWithProcessor(sdklog.NewSimpleProcessor(capt), res)
	t.Cleanup(func() { _ = lp.Shutdown(context.Background()) })
	return capt, lp
}

// logAttributeMap flattens a captured record's attributes by key, failing the
// test on any record without WalkAttributes.
func logAttributeMap(t *testing.T, rec sdklog.Record) map[string]string {
	t.Helper()
	m := map[string]string{}
	rec.WalkAttributes(func(kv attribute.KeyValue) bool {
		m[string(kv.Key)] = kv.Value.AsString()
		return true
	})
	return m
}

func TestStateChangeEventConstantContract(t *testing.T) {
	// (3.8) Pin the event name and new attribute key: the app test asserts the
	// end-to-end record shape, this one asserts the contract of the constants
	// the record is built from.
	if StateChangeEventName != "herdr.agent.state_change" {
		t.Errorf("StateChangeEventName = %q, want %q", StateChangeEventName, "herdr.agent.state_change")
	}
	if AgentIDKey != "herdr.agent.id" {
		t.Errorf("AgentIDKey = %q, want %q", AgentIDKey, "herdr.agent.id")
	}
}

func TestLoggerProviderEmitsEventRecords(t *testing.T) {
	t.Setenv("HERDR_PLUGIN_STATE_DIR", t.TempDir())
	res, err := BuildResource(context.Background())
	if err != nil {
		t.Fatalf("BuildResource: %v", err)
	}

	capt, lp := logProcessorProvider(t, res)
	logger := lp.Logger(LoggerName)

	rec := apilog.Record{}
	rec.SetEventName(StateChangeEventName)
	rec.SetTimestamp(time.Unix(1_700_000_000, 0))
	rec.SetBody(attribute.StringValue(StateChangeEventBody))
	rec.AddAttributes(
		attribute.String(AgentIDKey, "w1:p1"),
		attribute.String(AgentTypeKey, "codex"),
		attribute.String(WorkspaceIDKey, "w1"),
		attribute.String(AgentPreviousState, "working"),
		attribute.String(AgentStateKey, "blocked"),
	)
	logger.Emit(context.Background(), rec)

	got := capt.snapshot()
	if len(got) != 1 {
		t.Fatalf("captured records = %d, want 1", len(got))
	}
	r := got[0]
	if r.EventName() != StateChangeEventName {
		t.Errorf("event name = %q, want %q", r.EventName(), StateChangeEventName)
	}
	if r.Timestamp().IsZero() {
		t.Error("record timestamp is zero; transition events must carry the observed time")
	}
	attrs := logAttributeMap(t, r)
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
		t.Errorf("herdr.state.source unexpectedly present on event record: %q (field is verified-absent from the wire, finding #3)", attrs["herdr.state.source"])
	}
}

func TestLoggerProviderAssociatesResource(t *testing.T) {
	setCleanEnv(t)
	t.Setenv("HERDR_PLUGIN_STATE_DIR", t.TempDir())

	res, err := BuildResource(context.Background())
	if err != nil {
		t.Fatalf("BuildResource: %v", err)
	}

	capt, lp := logProcessorProvider(t, res)
	rec := apilog.Record{}
	rec.SetEventName(StateChangeEventName)
	rec.AddAttributes(attribute.String(AgentIDKey, "w1:p1"))
	lp.Logger(LoggerName).Emit(context.Background(), rec)

	got := capt.snapshot()
	if len(got) != 1 {
		t.Fatalf("captured records = %d, want 1", len(got))
	}
	rr := got[0].Resource()
	if rr == nil {
		t.Fatal("captured record resource is nil")
	}
	if got, ok := ResourceAttribute(rr, serviceNameKey); !ok || got != DefaultServiceName {
		t.Errorf("service.name = %q (ok=%v), want default %q", got, ok, DefaultServiceName)
	}
	if got, ok := ResourceAttribute(rr, machineIDKey); !ok || got == "" {
		t.Errorf("herdr.machine.id present=%v value=%q on record resource, want non-empty", ok, got)
	}
	if got, ok := ResourceAttribute(rr, machineHostnameKey); !ok || got == "" {
		t.Errorf("herdr.machine.hostname present=%v value=%q on record resource, want non-empty", ok, got)
	}
}

func TestNewLoggerProviderWithUnreachableEndpoint(t *testing.T) {
	// Same invariant as the metrics exporter (Phase 5.4): the gRPC connection
	// is established lazily, so construction must succeed even when the
	// collector is unreachable — export batches fail and are dropped, the
	// process itself is unaffected.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:1")
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "500")

	res, err := BuildResource(context.Background())
	if err != nil {
		t.Fatalf("BuildResource: %v", err)
	}
	lp, shutdown, err := NewLoggerProvider(context.Background(), res)
	if err != nil {
		t.Fatalf("NewLoggerProvider with unreachable endpoint: %v", err)
	}
	// Shutdown flushes pending export batches, which fail against an
	// unreachable endpoint — exactly the Phase 5.4 behavior we're pinning
	// down: exports fail and are dropped, the process itself is unaffected.
	_ = shutdown(context.Background())
	_ = lp
}

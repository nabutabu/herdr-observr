package app

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nabutabu/herdr-observr/internal/events"
	"github.com/nabutabu/herdr-observr/internal/otel"
	"github.com/nabutabu/herdr-observr/internal/snapshot"
	"github.com/nabutabu/herdr-observr/internal/tracker"
	"github.com/nabutabu/herdr-observr/internal/usage"
	"go.opentelemetry.io/otel/attribute"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestPaneCreatedNeedsResubscribe(t *testing.T) {
	a := &App{scope: newScope([]string{"w1:p1"})}

	cases := []struct {
		name string
		ev   events.NormalizedEvent
		want bool
	}{
		{name: "new pane", ev: events.NormalizedEvent{Kind: events.KindPaneCreated, PaneID: "w1:p2"}, want: true},
		{name: "already subscribed pane", ev: events.NormalizedEvent{Kind: events.KindPaneCreated, PaneID: "w1:p1"}, want: false},
		{name: "empty pane id", ev: events.NormalizedEvent{Kind: events.KindPaneCreated}, want: true},
		{name: "workspace event ignores pane id", ev: events.NormalizedEvent{Kind: events.KindWorkspaceCreated, PaneID: "w1:p9"}, want: false},
		{name: "tab event ignores pane id", ev: events.NormalizedEvent{Kind: events.KindTabCreated, PaneID: "w1:p9", TabID: "w1:t1"}, want: false},
		{name: "status change not resubscribe-worthy", ev: events.NormalizedEvent{Kind: events.KindAgentStatusChanged, PaneID: "w1:p2"}, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := a.paneCreatedNeedsResubscribe(tc.ev); got != tc.want {
				t.Errorf("paneCreatedNeedsResubscribe(%+v) = %v, want %v", tc.ev, got, tc.want)
			}
		})
	}

	nilScope := &App{}
	if got := nilScope.paneCreatedNeedsResubscribe(events.NormalizedEvent{Kind: events.KindPaneCreated, PaneID: "w1:p1"}); !got {
		t.Error("nil scope must report every pane.created as needing a resubscribe")
	}
}

func TestScope(t *testing.T) {
	s := newScope([]string{"w1:p1", "w2:p1"})
	if len(s.subscribedPanes) != 2 {
		t.Fatalf("subscribedPanes = %v, want 2 panes", s.subscribedPanes)
	}
	for _, want := range []string{"w1:p1", "w2:p1"} {
		if !s.subscribed(want) {
			t.Errorf("scope not subscribed to %q: %v", want, s.subscribedPanes)
		}
	}
	if s.subscribed("w1:p9") {
		t.Errorf("scope unexpectedly subscribed to w1:p9: %v", s.subscribedPanes)
	}
	if newScope(nil).subscribed("w1:p1") {
		t.Error("empty scope must not cover any pane")
	}
}

func TestSignalResubscribeCoalesces(t *testing.T) {
	ch := make(chan struct{}, 1)
	signalResubscribe(ch)
	signalResubscribe(ch)
	select {
	case <-ch:
	default:
		t.Fatal("expected one pending signal")
	}
	select {
	case <-ch:
		t.Fatal("signals must coalesce into the single-buffer channel")
	default:
	}
}

func TestNewTelemetryRegistersUpGauge(t *testing.T) {
	// Keep the shutdown flush fast: with no collector running the deferred
	// export fails after the per-export timeout.
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "500")

	telemetry := NewTelemetry(context.Background())
	if telemetry == nil {
		t.Fatal("NewTelemetry did not create telemetry")
	}
	if telemetry.meter == nil {
		t.Fatal("NewTelemetry did not create a meter")
	}
	if telemetry.shutdown == nil {
		t.Fatal("NewTelemetry did not install the shutdown func")
	}
	if telemetry.tracer == nil || telemetry.tracerShutdown == nil {
		t.Fatal("NewTelemetry did not wire the 3.9 trace provider")
	}
	telemetry.Shutdown(context.Background())
}

// drainTransitions replicates Run's select-case consumer, forwarding each
// transition to the counter, the 3.8 event logger, and the 3.9 span tracer
// exactly as production does.
func drainTransitions(t *testing.T, a *App) {
	t.Helper()
	for {
		select {
		case tr := <-a.tr.Transitions():
			a.telemetry.recordTransition(tr)
			a.telemetry.recordTransitionEvent(tr)
			a.telemetry.recordTransitionSpan(tr)
		default:
			return
		}
	}
}

// drainStateDurations replicates Run's select-case consumer, forwarding each
// closed state interval to the histogram exactly as production does.
func drainStateDurations(t *testing.T, a *App) {
	t.Helper()
	for {
		select {
		case sd := <-a.tr.StateDurations():
			a.telemetry.recordStateDuration(sd)
		default:
			return
		}
	}
}

// drainAttentionLatency replicates Run's select-case consumer, forwarding each
// closed attention-latency interval to the histogram exactly as production
// does.
func drainAttentionLatency(t *testing.T, a *App) {
	t.Helper()
	for {
		select {
		case al := <-a.tr.AttentionLatency():
			a.telemetry.recordAttentionLatency(al)
		default:
			return
		}
	}
}

// recordUsageDeltaResolved replicates Run's select-case body for the U4.1
// Deltas branch: resolve the delta's last-known attribution from the tracker
// (U3.2) and forward both to the counters exactly as production does. For
// tests that can't drive the collector (a real poll) this is the atomic unit
// under test.
func recordUsageDeltaResolved(t *testing.T, a *App, d usage.UsageDelta) {
	t.Helper()
	a.telemetry.recordUsageDelta(d, a.tr.Sessions()[d.SessionID])
}

// captureLogExporter is a minimal sdklog.Exporter that snapshots emitted
// event records for assertion, wired synchronously through a SimpleProcessor so
// records are captured at Emit time.
type captureLogExporter struct {
	mu      sync.Mutex
	records []sdklog.Record
}

func (c *captureLogExporter) Export(_ context.Context, records []sdklog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, records...)
	return nil
}

func (c *captureLogExporter) Shutdown(context.Context) error { return nil }

func (c *captureLogExporter) ForceFlush(context.Context) error { return nil }

func (c *captureLogExporter) snapshot() []sdklog.Record {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]sdklog.Record, len(c.records))
	copy(out, c.records)
	return out
}

// telemetryWithLogger builds a Telemetry whose 3.8 event logger is wired to a
// synchronous capture exporter, the logs analogue of the manual-reader seam the
// metric tests use.
func telemetryWithLogger(t *testing.T) (*Telemetry, *captureLogExporter) {
	t.Helper()
	capt := &captureLogExporter{}
	lp := otel.NewLoggerProviderWithProcessor(sdklog.NewSimpleProcessor(capt), resource.NewSchemaless(attribute.String("service.name", "test")))
	t.Cleanup(func() { _ = lp.Shutdown(context.Background()) })
	return &Telemetry{logger: lp.Logger(otel.LoggerName)}, capt
}

// logRecordAttrs flattens a captured log record's attributes by key.
func logRecordAttrs(t *testing.T, rec sdklog.Record) map[string]string {
	t.Helper()
	m := map[string]string{}
	rec.WalkAttributes(func(kv attribute.KeyValue) bool {
		m[string(kv.Key)] = kv.Value.AsString()
		return true
	})
	return m
}

// captureSpanExporter is a minimal sdktrace.SpanExporter that snapshots emitted
// transition spans for assertion, wired synchronously through a
// SimpleSpanProcessor so spans are captured at End time.
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

// telemetryWithTracer builds a Telemetry whose 3.9 span tracer is wired to a
// synchronous capture exporter, the traces analogue of the manual-reader seam
// the metric tests use.
func telemetryWithTracer(t *testing.T) (*Telemetry, *captureSpanExporter) {
	t.Helper()
	capt := &captureSpanExporter{}
	tp := otel.NewTracerProviderWithProcessor(sdktrace.NewSimpleSpanProcessor(capt), resource.NewSchemaless(attribute.String("service.name", "test")))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return &Telemetry{tracer: tp.Tracer(otel.TracerName)}, capt
}

// spanAttributeMap flattens a captured span's attributes by key.
func spanAttributeMap(s sdktrace.ReadOnlySpan) map[string]string {
	m := map[string]string{}
	for _, kv := range s.Attributes() {
		m[string(kv.Key)] = kv.Value.AsString()
	}
	return m
}

func findInt64SumPoints(t *testing.T, rm metricdata.ResourceMetrics, name string) []metricdata.DataPoint[int64] {
	t.Helper()
	var out []metricdata.DataPoint[int64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %s data type = %T, want Sum[int64]", name, m.Data)
			}
			out = append(out, sum.DataPoints...)
		}
	}
	return out
}

func findFloat64SumPoints(t *testing.T, rm metricdata.ResourceMetrics, name string) []metricdata.DataPoint[float64] {
	t.Helper()
	var out []metricdata.DataPoint[float64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[float64])
			if !ok {
				t.Fatalf("metric %s data type = %T, want Sum[float64]", name, m.Data)
			}
			out = append(out, sum.DataPoints...)
		}
	}
	return out
}

func findFloat64HistogramPoints(t *testing.T, rm metricdata.ResourceMetrics, name string) []metricdata.HistogramDataPoint[float64] {
	t.Helper()
	var out []metricdata.HistogramDataPoint[float64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("metric %s data type = %T, want Histogram[float64]", name, m.Data)
			}
			out = append(out, h.DataPoints...)
		}
	}
	return out
}

func findInt64GaugePoints(t *testing.T, rm metricdata.ResourceMetrics, name string) []metricdata.DataPoint[int64] {
	t.Helper()
	var out []metricdata.DataPoint[int64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[int64])
			if !ok {
				t.Fatalf("metric %s data type = %T, want Gauge[int64]", name, m.Data)
			}
			out = append(out, g.DataPoints...)
		}
	}
	return out
}

// gaugeValues splits a gauge's datapoints into the attribute-free global point
// and a per-workspace map keyed by herdr.workspace.id.
func gaugeValues(points []metricdata.DataPoint[int64]) (global int64, byWorkspace map[string]int64) {
	byWorkspace = map[string]int64{}
	for _, p := range points {
		if ws, ok := p.Attributes.Value(attribute.Key(otel.WorkspaceIDKey)); ok {
			byWorkspace[ws.AsString()] = p.Value
		} else {
			global = p.Value
		}
	}
	return
}

func attrString(t *testing.T, s attribute.Set, key string) string {
	t.Helper()
	v, ok := s.Value(attribute.Key(key))
	if !ok {
		t.Fatalf("missing attribute %q in %s", key, s.Encoded(attribute.DefaultEncoder()))
	}
	return v.AsString()
}

// statusEv is a minimal pane.agent_status_changed event the tracker applies
// through its normal handleEvent path.
func statusEv(s snapshot.AgentStatus) events.NormalizedEvent {
	return events.NormalizedEvent{Kind: events.KindAgentStatusChanged, PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: s}
}

func TestRecordTransitionCounterEventDriven(t *testing.T) {
	reader := metric.NewManualReader()
	mp := otel.NewMeterProviderWithReader(reader, resource.NewSchemaless(attribute.String("service.name", "test")))
	defer func() { _ = mp.Shutdown(context.Background()) }()

	a := &App{telemetry: &Telemetry{meter: mp.Meter(otel.MeterName)}, tr: tracker.NewTracker()}
	a.telemetry.registerTransitions()
	if a.telemetry.transitions == nil {
		t.Fatal("registerTransitionCounter did not create the counter")
	}

	a.handleEvent(statusEv(snapshot.AgentStatusWorking)) // first-seen: no transition
	a.handleEvent(statusEv(snapshot.AgentStatusBlocked)) // working→blocked
	drainTransitions(t, a)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	points := findInt64SumPoints(t, rm, otel.TransitionMetricName)
	if len(points) != 1 {
		t.Fatalf("transition datapoints = %d, want 1 (first-seen emits none)", len(points))
	}
	dp := points[0]
	if dp.Value != 1 {
		t.Errorf("transition count = %d, want 1", dp.Value)
	}
	if got := attrString(t, dp.Attributes, otel.AgentTypeKey); got != "codex" {
		t.Errorf("agent.type = %q, want codex", got)
	}
	if got := attrString(t, dp.Attributes, otel.AgentPreviousState); got != "working" {
		t.Errorf("previous_state = %q, want working", got)
	}
	if got := attrString(t, dp.Attributes, otel.AgentStateKey); got != "blocked" {
		t.Errorf("state = %q, want blocked", got)
	}
}

func TestRecordTransitionCounterSeenFlip(t *testing.T) {
	reader := metric.NewManualReader()
	mp := otel.NewMeterProviderWithReader(reader, resource.NewSchemaless(attribute.String("service.name", "test")))
	defer func() { _ = mp.Shutdown(context.Background()) }()

	a := &App{telemetry: &Telemetry{meter: mp.Meter(otel.MeterName)}, tr: tracker.NewTracker()}
	a.telemetry.registerTransitions()

	// The silent reconcile-detected close path (2.4): done is flip-flopped to
	// idle by ApplySeenFlip, exactly as onReport does in production.
	a.handleEvent(statusEv(snapshot.AgentStatusDone))
	a.tr.ApplySeenFlip("w1:p1")
	drainTransitions(t, a)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	points := findInt64SumPoints(t, rm, otel.TransitionMetricName)
	if len(points) != 1 {
		t.Fatalf("transition datapoints = %d, want 1", len(points))
	}
	dp := points[0]
	if dp.Value != 1 {
		t.Errorf("transition count = %d, want 1", dp.Value)
	}
	if got := attrString(t, dp.Attributes, otel.AgentPreviousState); got != "done" {
		t.Errorf("previous_state = %q, want done", got)
	}
	if got := attrString(t, dp.Attributes, otel.AgentStateKey); got != "idle" {
		t.Errorf("state = %q, want idle", got)
	}
}

func TestRecordTransitionNoopWithoutTelemetry(t *testing.T) {
	// Telemetry disabled (nil counter): consuming transitions must be a no-op,
	// never a panic or block.
	a := &App{tr: tracker.NewTracker()}
	tr := tracker.NewTracker()
	tr.ApplyAgentStatusChanged(statusEv(snapshot.AgentStatusWorking))
	tr.ApplyAgentStatusChanged(statusEv(snapshot.AgentStatusBlocked))
	select {
	case trns := <-tr.Transitions():
		a.telemetry.recordTransition(trns)
	default:
		t.Fatal("expected a transition from the tracker")
	}
}

func TestRecordTransitionEventEventDriven(t *testing.T) {
	// (3.8) One herdr.agent.state_change event record per genuine transition,
	// tagged with the high-cardinality ids the 3.3 counter excludes plus
	// type/workspace/previous/current state. First-seen upserts emit nothing.
	telemetry, capt := telemetryWithLogger(t)
	a := &App{telemetry: telemetry, tr: tracker.NewTracker()}

	a.handleEvent(statusEv(snapshot.AgentStatusWorking)) // first-seen: no transition
	a.handleEvent(statusEv(snapshot.AgentStatusBlocked)) // working→blocked
	drainTransitions(t, a)

	got := capt.snapshot()
	if len(got) != 1 {
		t.Fatalf("event records = %d, want 1 (first-seen emits none)", len(got))
	}
	rec := got[0]
	if rec.EventName() != otel.StateChangeEventName {
		t.Errorf("event name = %q, want %q", rec.EventName(), otel.StateChangeEventName)
	}
	if rec.Timestamp().IsZero() {
		t.Error("event timestamp is zero; want the transition's observed time")
	}
	attrs := logRecordAttrs(t, rec)
	for key, want := range map[string]string{
		otel.AgentIDKey:         "w1:p1",
		otel.AgentTypeKey:       "codex",
		otel.WorkspaceIDKey:     "w1",
		otel.AgentPreviousState: "working",
		otel.AgentStateKey:      "blocked",
	} {
		if got := attrs[key]; got != want {
			t.Errorf("attribute %q = %q, want %q", key, got, want)
		}
	}
	if attrs["herdr.state.source"] != "" {
		t.Errorf("herdr.state.source present = %q; verified-absent from pushed status events (finding #3)", attrs["herdr.state.source"])
	}
}

func TestRecordTransitionEventSeenFlip(t *testing.T) {
	// The silent reconcile-detected close path (2.4): done is flip-flopped to
	// idle by ApplySeenFlip, exactly as onReport does in production — that is
	// still a genuine transition and must emit an event record too.
	telemetry, capt := telemetryWithLogger(t)
	a := &App{telemetry: telemetry, tr: tracker.NewTracker()}

	a.handleEvent(statusEv(snapshot.AgentStatusDone))
	a.tr.ApplySeenFlip("w1:p1")
	drainTransitions(t, a)

	got := capt.snapshot()
	if len(got) != 1 {
		t.Fatalf("event records = %d, want 1", len(got))
	}
	attrs := logRecordAttrs(t, got[0])
	if attrs[otel.AgentPreviousState] != "done" || attrs[otel.AgentStateKey] != "idle" {
		t.Errorf("seen-flip event attrs = %v, want previous=done state=idle", attrs)
	}
}

func TestRecordTransitionEventNoopWithoutTelemetry(t *testing.T) {
	// Telemetry disabled (nil logger): emitting events must be a no-op, never
	// a panic or block.
	a := &App{tr: tracker.NewTracker()}
	tr := tracker.NewTracker()
	tr.ApplyAgentStatusChanged(statusEv(snapshot.AgentStatusWorking))
	tr.ApplyAgentStatusChanged(statusEv(snapshot.AgentStatusBlocked))
	select {
	case trns := <-tr.Transitions():
		a.telemetry.recordTransitionEvent(trns)
	default:
		t.Fatal("expected a transition from the tracker")
	}
}

func TestRecordTransitionSpanEventDriven(t *testing.T) {
	// 3.9: a genuine event-driven transition emits one short
	// herdr.agent.state_change root span carrying the high-cardinality ids the
	// 3.3 counter excludes, ended immediately at the observed transition — the
	// "never open-ended" exit criterion.
	telemetry, capt := telemetryWithTracer(t)
	a := &App{telemetry: telemetry, tr: tracker.NewTracker()}

	a.handleEvent(statusEv(snapshot.AgentStatusWorking)) // first-seen: no transition
	a.handleEvent(statusEv(snapshot.AgentStatusBlocked)) // working→blocked
	drainTransitions(t, a)

	got := capt.snapshot()
	if len(got) != 1 {
		t.Fatalf("spans = %d, want 1 (first-seen emits none)", len(got))
	}
	s := got[0]
	if s.Name() != otel.StateChangeEventName {
		t.Errorf("span name = %q, want %q", s.Name(), otel.StateChangeEventName)
	}
	if s.Parent().IsValid() {
		t.Error("transition span has a parent; 3.9 spans must be root spans")
	}
	if s.EndTime().IsZero() {
		t.Error("span end time is zero; transition spans must end, never stay open")
	}
	if d := s.EndTime().Sub(s.StartTime()); d < 0 || d > time.Second {
		t.Errorf("span duration = %v, want short (>= 0 and < 1s)", d)
	}
	attrs := spanAttributeMap(s)
	for key, want := range map[string]string{
		otel.AgentIDKey:         "w1:p1",
		otel.AgentTypeKey:       "codex",
		otel.WorkspaceIDKey:     "w1",
		otel.AgentPreviousState: "working",
		otel.AgentStateKey:      "blocked",
	} {
		if got := attrs[key]; got != want {
			t.Errorf("attribute %q = %q, want %q", key, got, want)
		}
	}
	if attrs["herdr.state.source"] != "" {
		t.Errorf("herdr.state.source present = %q; verified-absent from pushed status events (finding #3)", attrs["herdr.state.source"])
	}
}

func TestRecordTransitionSpanSeenFlip(t *testing.T) {
	// The silent reconcile-detected close path (2.4): done is flip-flopped to
	// idle by ApplySeenFlip, exactly as onReport does in production — that is
	// still a genuine transition and must emit a span too.
	telemetry, capt := telemetryWithTracer(t)
	a := &App{telemetry: telemetry, tr: tracker.NewTracker()}

	a.handleEvent(statusEv(snapshot.AgentStatusDone))
	a.tr.ApplySeenFlip("w1:p1")
	drainTransitions(t, a)

	got := capt.snapshot()
	if len(got) != 1 {
		t.Fatalf("spans = %d, want 1", len(got))
	}
	attrs := spanAttributeMap(got[0])
	if attrs[otel.AgentPreviousState] != "done" || attrs[otel.AgentStateKey] != "idle" {
		t.Errorf("seen-flip span attrs = %v, want previous=done state=idle", attrs)
	}
}

func TestRecordTransitionSpanNoopWithoutTelemetry(t *testing.T) {
	// Telemetry disabled (nil tracer): emitting spans must be a no-op, never a
	// panic or block.
	a := &App{tr: tracker.NewTracker()}
	tr := tracker.NewTracker()
	tr.ApplyAgentStatusChanged(statusEv(snapshot.AgentStatusWorking))
	tr.ApplyAgentStatusChanged(statusEv(snapshot.AgentStatusBlocked))
	select {
	case trns := <-tr.Transitions():
		a.telemetry.recordTransitionSpan(trns)
	default:
		t.Fatal("expected a transition from the tracker")
	}
}

func TestRecordStateDurationHistogramEventDriven(t *testing.T) {
	reader := metric.NewManualReader()
	mp := otel.NewMeterProviderWithReader(reader, resource.NewSchemaless(attribute.String("service.name", "test")))
	defer func() { _ = mp.Shutdown(context.Background()) }()

	a := &App{telemetry: &Telemetry{meter: mp.Meter(otel.MeterName)}, tr: tracker.NewTracker()}
	a.telemetry.registerStateDurations()
	if a.telemetry.stateDurationHistogram == nil {
		t.Fatal("registerStateDurationHistogram did not create the histogram")
	}

	a.handleEvent(statusEv(snapshot.AgentStatusWorking)) // first-seen: nothing to close
	a.handleEvent(statusEv(snapshot.AgentStatusBlocked)) // closes working
	drainStateDurations(t, a)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	points := findFloat64HistogramPoints(t, rm, otel.DurationMetricName)
	if len(points) != 1 {
		t.Fatalf("state-duration datapoints = %d, want 1 (first-seen records none)", len(points))
	}
	dp := points[0]
	if dp.Count != 1 {
		t.Errorf("count = %d, want 1", dp.Count)
	}
	if got := attrString(t, dp.Attributes, otel.AgentTypeKey); got != "codex" {
		t.Errorf("agent.type = %q, want codex", got)
	}
	if got := attrString(t, dp.Attributes, otel.AgentStateKey); got != "working" {
		t.Errorf("state = %q, want working", got)
	}
}

func TestRecordStateDurationHistogramSeenFlip(t *testing.T) {
	reader := metric.NewManualReader()
	mp := otel.NewMeterProviderWithReader(reader, resource.NewSchemaless(attribute.String("service.name", "test")))
	defer func() { _ = mp.Shutdown(context.Background()) }()

	a := &App{telemetry: &Telemetry{meter: mp.Meter(otel.MeterName)}, tr: tracker.NewTracker()}
	a.telemetry.registerStateDurations()

	// The silent reconcile-detected close path (2.4): done is flip-flopped to
	// idle by ApplySeenFlip, closing the done interval.
	a.handleEvent(statusEv(snapshot.AgentStatusDone))
	a.tr.ApplySeenFlip("w1:p1")
	drainStateDurations(t, a)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	points := findFloat64HistogramPoints(t, rm, otel.DurationMetricName)
	if len(points) != 1 {
		t.Fatalf("state-duration datapoints = %d, want 1", len(points))
	}
	if got := attrString(t, points[0].Attributes, otel.AgentStateKey); got != "done" {
		t.Errorf("state = %q, want done", got)
	}
}

func TestRecordStateDurationHistogramCloseFlush(t *testing.T) {
	reader := metric.NewManualReader()
	mp := otel.NewMeterProviderWithReader(reader, resource.NewSchemaless(attribute.String("service.name", "test")))
	defer func() { _ = mp.Shutdown(context.Background()) }()

	a := &App{telemetry: &Telemetry{meter: mp.Meter(otel.MeterName)}, tr: tracker.NewTracker()}
	a.telemetry.registerStateDurations()

	// Closing a pane mid-state flushes the open interval (2.7 flush-on-close).
	a.handleEvent(statusEv(snapshot.AgentStatusWorking))
	a.handleEvent(events.NormalizedEvent{Kind: events.KindPaneClosed, PaneID: "w1:p1"})
	drainStateDurations(t, a)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	points := findFloat64HistogramPoints(t, rm, otel.DurationMetricName)
	if len(points) != 1 {
		t.Fatalf("state-duration datapoints = %d, want 1", len(points))
	}
	if got := attrString(t, points[0].Attributes, otel.AgentStateKey); got != "working" {
		t.Errorf("state = %q, want working", got)
	}
}

func TestRecordStateDurationNoopWithoutTelemetry(t *testing.T) {
	// Telemetry disabled (nil histogram): consuming closed intervals must be a
	// no-op, never a panic or block.
	a := &App{tr: tracker.NewTracker()}
	sd := tracker.StateDuration{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", State: snapshot.AgentStatusWorking}
	a.telemetry.recordStateDuration(sd)
}

func TestRecordAttentionLatencyHistogramEventDriven(t *testing.T) {
	reader := metric.NewManualReader()
	mp := otel.NewMeterProviderWithReader(reader, resource.NewSchemaless(attribute.String("service.name", "test")))
	defer func() { _ = mp.Shutdown(context.Background()) }()

	a := &App{telemetry: &Telemetry{meter: mp.Meter(otel.MeterName)}, tr: tracker.NewTracker()}
	a.telemetry.registerAttentionLatency()
	if a.telemetry.attentionLatencyHistogram == nil {
		t.Fatal("registerAttentionLatencyHistogram did not create the histogram")
	}

	// Entering done on a first-seen upsert starts the clock but emits nothing;
	// leaving done via a real status event closes and emits the interval.
	a.handleEvent(statusEv(snapshot.AgentStatusDone))
	a.handleEvent(statusEv(snapshot.AgentStatusIdle))
	drainAttentionLatency(t, a)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	points := findFloat64HistogramPoints(t, rm, otel.AttentionLatencyMetricName)
	if len(points) != 1 {
		t.Fatalf("attention-latency datapoints = %d, want 1 (entering done emits none)", len(points))
	}
	dp := points[0]
	if dp.Count != 1 {
		t.Errorf("count = %d, want 1", dp.Count)
	}
	if got := attrString(t, dp.Attributes, otel.AgentTypeKey); got != "codex" {
		t.Errorf("agent.type = %q, want codex", got)
	}
	if _, ok := dp.Attributes.Value(attribute.Key(otel.AgentStateKey)); ok {
		t.Errorf("state attribute present on attention latency; it is done-only by construction")
	}
}

func TestRecordAttentionLatencyHistogramSeenFlip(t *testing.T) {
	reader := metric.NewManualReader()
	mp := otel.NewMeterProviderWithReader(reader, resource.NewSchemaless(attribute.String("service.name", "test")))
	defer func() { _ = mp.Shutdown(context.Background()) }()

	a := &App{telemetry: &Telemetry{meter: mp.Meter(otel.MeterName)}, tr: tracker.NewTracker()}
	a.telemetry.registerAttentionLatency()

	// The silent reconcile-detected close path (2.4): done is flip-flopped to
	// idle by ApplySeenFlip, closing the attention-latency interval.
	a.handleEvent(statusEv(snapshot.AgentStatusDone))
	a.tr.ApplySeenFlip("w1:p1")
	drainAttentionLatency(t, a)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	points := findFloat64HistogramPoints(t, rm, otel.AttentionLatencyMetricName)
	if len(points) != 1 {
		t.Fatalf("attention-latency datapoints = %d, want 1", len(points))
	}
	if got := attrString(t, points[0].Attributes, otel.AgentTypeKey); got != "codex" {
		t.Errorf("agent.type = %q, want codex", got)
	}
}

func TestRecordAttentionLatencyHistogramCloseFlush(t *testing.T) {
	reader := metric.NewManualReader()
	mp := otel.NewMeterProviderWithReader(reader, resource.NewSchemaless(attribute.String("service.name", "test")))
	defer func() { _ = mp.Shutdown(context.Background()) }()

	a := &App{telemetry: &Telemetry{meter: mp.Meter(otel.MeterName)}, tr: tracker.NewTracker()}
	a.telemetry.registerAttentionLatency()

	// Closing a pane while its agent is still done flushes the open
	// attention-latency interval (2.7 flush-on-close).
	a.handleEvent(statusEv(snapshot.AgentStatusDone))
	a.handleEvent(events.NormalizedEvent{Kind: events.KindPaneClosed, PaneID: "w1:p1"})
	drainAttentionLatency(t, a)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	points := findFloat64HistogramPoints(t, rm, otel.AttentionLatencyMetricName)
	if len(points) != 1 {
		t.Fatalf("attention-latency datapoints = %d, want 1", len(points))
	}
	if got := attrString(t, points[0].Attributes, otel.AgentTypeKey); got != "codex" {
		t.Errorf("agent.type = %q, want codex", got)
	}
}

func TestRecordAttentionLatencyNoopWithoutTelemetry(t *testing.T) {
	// Telemetry disabled (nil histogram): consuming closed intervals must be a
	// no-op, never a panic or block.
	a := &App{tr: tracker.NewTracker()}
	al := tracker.AttentionLatency{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex"}
	a.telemetry.recordAttentionLatency(al)
}

func TestCountGaugesEventDriven(t *testing.T) {
	reader := metric.NewManualReader()
	mp := otel.NewMeterProviderWithReader(reader, resource.NewSchemaless(attribute.String("service.name", "test")))
	defer func() { _ = mp.Shutdown(context.Background()) }()

	a := &App{telemetry: &Telemetry{meter: mp.Meter(otel.MeterName)}, tr: tracker.NewTracker()}
	a.telemetry.registerCountGauges(a.tr.Counts)

	// Drive the tracker through the same handleEvent path as production via
	// status events spread across two workspaces.
	ws1, ws2 := "w1", "w2"
	status := func(state snapshot.AgentStatus, paneID, wsID string) events.NormalizedEvent {
		return events.NormalizedEvent{Kind: events.KindAgentStatusChanged, PaneID: paneID, WorkspaceID: wsID, Agent: "codex", NewState: state}
	}
	a.handleEvent(status(snapshot.AgentStatusWorking, "w1:p1", ws1)) // first seen: working=1
	a.handleEvent(status(snapshot.AgentStatusBlocked, "w1:p2", ws1)) // blocked=1
	a.handleEvent(status(snapshot.AgentStatusWorking, "w2:p1", ws2)) // working=2

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}

	active := findInt64GaugePoints(t, rm, otel.ActiveAgentsMetricName)
	if len(active) != 3 {
		t.Fatalf("active datapoints = %d, want 3 (global + one per workspace)", len(active))
	}
	global, byWs := gaugeValues(active)
	if global != 2 {
		t.Errorf("active global = %d, want 2", global)
	}
	if byWs[ws1] != 1 || byWs[ws2] != 1 {
		t.Errorf("active per-workspace = %+v, want w1=1 w2=1", byWs)
	}

	blocked := findInt64GaugePoints(t, rm, otel.BlockedAgentsMetricName)
	global, byWs = gaugeValues(blocked)
	if global != 1 {
		t.Errorf("blocked global = %d, want 1", global)
	}
	// A workspace with no blocked agents still emits an explicit 0 (series
	// stability), not an absent datapoint.
	if byWs[ws1] != 1 || byWs[ws2] != 0 {
		t.Errorf("blocked per-workspace = %+v, want w1=1 w2=0", byWs)
	}

	// Zero-count states must still emit an explicit 0 globally.
	idle := findInt64GaugePoints(t, rm, otel.IdleAgentsMetricName)
	if global, _ = gaugeValues(idle); global != 0 {
		t.Errorf("idle global = %d, want 0", global)
	}
	done := findInt64GaugePoints(t, rm, otel.DoneAgentsMetricName)
	if global, byWs = gaugeValues(done); global != 0 {
		t.Errorf("done global = %d, want 0", global)
	}
	if len(byWs) != 2 {
		t.Errorf("done per-workspace entries = %d, want 2 (w1, w2)", len(byWs))
	}

	// (3.7) herdr.workspace.agent.concurrent sums working + blocked per
	// workspace, one datapoint per workspace and no global point.
	concurrent := findInt64GaugePoints(t, rm, otel.WorkspaceConcurrentMetricName)
	if global, byWs = gaugeValues(concurrent); global != 0 {
		t.Errorf("concurrent global = %d, want 0 (per-workspace metric only)", global)
	}
	if byWs[ws1] != 2 || byWs[ws2] != 1 {
		t.Errorf("concurrent per-workspace = %+v, want w1=2 (working+blocked) w2=1", byWs)
	}

	// Closing a pane decrements its workspace and global counts immediately
	// (2.7), reflected on the next collection — no per-event gauge writes.
	a.handleEvent(events.NormalizedEvent{Kind: events.KindPaneClosed, PaneID: "w1:p1"})
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect after close: %v", err)
	}
	active = findInt64GaugePoints(t, rm, otel.ActiveAgentsMetricName)
	global, byWs = gaugeValues(active)
	if global != 1 {
		t.Errorf("active global after close = %d, want 1", global)
	}
	if byWs[ws1] != 0 || byWs[ws2] != 1 {
		t.Errorf("active per-workspace after close = %+v, want w1=0 w2=1", byWs)
	}
	concurrent = findInt64GaugePoints(t, rm, otel.WorkspaceConcurrentMetricName)
	global, byWs = gaugeValues(concurrent)
	if byWs[ws1] != 1 || byWs[ws2] != 1 {
		t.Errorf("concurrent per-workspace after close = %+v, want w1=1 w2=1", byWs)
	}

	// A workspace whose agents are all non-concurrent still emits an explicit
	// 0 (3.7 mirrors the 3.6 always-emit decision) until work resumes.
	a.handleEvent(status(snapshot.AgentStatusIdle, "w2:p1", ws2)) // w2:p1 working -> idle
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect after idle: %v", err)
	}
	concurrent = findInt64GaugePoints(t, rm, otel.WorkspaceConcurrentMetricName)
	if global, byWs = gaugeValues(concurrent); global != 0 {
		t.Errorf("concurrent global after idle = %d, want 0", global)
	}
	if byWs[ws1] != 1 || byWs[ws2] != 0 {
		t.Errorf("concurrent per-workspace after idle = %+v, want w1=1 w2=0 (explicit zero)", byWs)
	}
}

func TestCountGaugesNoopWithoutTelemetry(t *testing.T) {
	// Telemetry disabled (nil meter): registering the gauges must be a no-op,
	// never a panic or block.
	a := &App{tr: tracker.NewTracker()}
	a.telemetry.registerCountGauges(a.tr.Counts)
	a.tr.ApplyAgentStatusChanged(events.NormalizedEvent{Kind: events.KindAgentStatusChanged, PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusWorking})
}

func TestRecordUsageCountersEventDriven(t *testing.T) {
	// (U4.1) One accrued usage delta increments the cost counter and all six
	// token counters, tagged with the session id and the session's last-known
	// pane/workspace/agent location (U3.2 resolution, exactly as production's
	// Run with the Deltas case).
	reader := metric.NewManualReader()
	mp := otel.NewMeterProviderWithReader(reader, resource.NewSchemaless(attribute.String("service.name", "test")))
	defer func() { _ = mp.Shutdown(context.Background()) }()

	a := &App{
		telemetry: &Telemetry{meter: mp.Meter(otel.MeterName)},
		tr:        tracker.NewTracker(),
	}
	a.telemetry.registerUsage()
	if a.telemetry.usage == nil {
		t.Fatal("registerUsage did not create the usage counters")
	}

	// Seed the tracker's session→attribution index (U3.2) so the delta
	// resolves to pane/workspace/agent tags.
	sess := snapshot.AgentSessionInfo{Source: "herdr:opencode", Agent: "opencode", Kind: snapshot.AgentSessionRefKindID, Value: "ses_1"}
	agent := "opencode"
	resp := snapshot.Snapshot{
		Panes: []snapshot.Pane{
			{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1", Agent: &agent, AgentSession: &sess, AgentStatus: snapshot.AgentStatusWorking},
		},
	}
	a.tr.ApplySnapshot(resp)

	d := usage.UsageDelta{SessionID: "ses_1", CostUSD: 0.10, InputTokens: 40, OutputTokens: 20, ReasoningTokens: 2, CacheReadTokens: 3, CacheWriteTokens: 1}
	recordUsageDeltaResolved(t, a, d)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}

	wantTokens := map[string]int64{
		otel.TokensInputMetricName:      40,
		otel.TokensOutputMetricName:     20,
		otel.TokensReasoningMetricName:  2,
		otel.TokensCacheReadMetricName:  3,
		otel.TokensCacheWriteMetricName: 1,
		otel.TokensTotalMetricName:      60, // derived input + output
	}
	for name, want := range wantTokens {
		points := findInt64SumPoints(t, rm, name)
		if len(points) != 1 {
			t.Fatalf("%s datapoints = %d, want 1", name, len(points))
		}
		if points[0].Value != want {
			t.Errorf("%s = %d, want %d", name, points[0].Value, want)
		}
		if got := attrString(t, points[0].Attributes, otel.SessionIDKey); got != "ses_1" {
			t.Errorf("%s session.id = %q, want ses_1", name, got)
		}
		if got := attrString(t, points[0].Attributes, otel.PaneIDKey); got != "w1:p1" {
			t.Errorf("%s pane.id = %q, want w1:p1", name, got)
		}
		if got := attrString(t, points[0].Attributes, otel.WorkspaceIDKey); got != "w1" {
			t.Errorf("%s workspace.id = %q, want w1", name, got)
		}
		if got := attrString(t, points[0].Attributes, otel.AgentTypeKey); got != "opencode" {
			t.Errorf("%s agent.type = %q, want opencode", name, got)
		}
	}

	cost := findFloat64SumPoints(t, rm, otel.CostMetricName)
	if len(cost) != 1 {
		t.Fatalf("cost datapoints = %d, want 1", len(cost))
	}
	if cost[0].Value != 0.10 {
		t.Errorf("cost = %v, want 0.10", cost[0].Value)
	}
	if got := attrString(t, cost[0].Attributes, otel.SessionIDKey); got != "ses_1" {
		t.Errorf("cost session.id = %q, want ses_1", got)
	}
}

func TestRecordUsageCountersMissAttributionUntagged(t *testing.T) {
	// The session's id is never in the tracker's index (miss → U3.2's
	// "emit untagged"): the counters still increment with the session id,
	// but carry no pane/workspace/agent location attributes.
	reader := metric.NewManualReader()
	mp := otel.NewMeterProviderWithReader(reader, resource.NewSchemaless(attribute.String("service.name", "test")))
	defer func() { _ = mp.Shutdown(context.Background()) }()

	a := &App{
		telemetry: &Telemetry{meter: mp.Meter(otel.MeterName)},
		tr:        tracker.NewTracker(),
	}
	a.telemetry.registerUsage()

	d := usage.UsageDelta{SessionID: "ses_unknown", CostUSD: 0.05, InputTokens: 10, OutputTokens: 5}
	recordUsageDeltaResolved(t, a, d)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}

	points := findInt64SumPoints(t, rm, otel.TokensInputMetricName)
	if len(points) != 1 || points[0].Value != 10 {
		t.Fatalf("input datapoints = %+v, want a single value 10", points)
	}
	if got := attrString(t, points[0].Attributes, otel.SessionIDKey); got != "ses_unknown" {
		t.Errorf("session.id = %q, want ses_unknown", got)
	}
	for _, key := range []string{otel.PaneIDKey, otel.WorkspaceIDKey, otel.AgentTypeKey} {
		if _, ok := points[0].Attributes.Value(attribute.Key(key)); ok {
			t.Errorf("attribute %q present on untagged delta; want omitted", key)
		}
	}
}

func TestRecordUsageNoopWithoutTelemetry(t *testing.T) {
	// Telemetry disabled (nil usage bundle): consuming deltas must be a no-op,
	// never a panic or block.
	a := &App{tr: tracker.NewTracker()}
	d := usage.UsageDelta{SessionID: "ses_1", InputTokens: 1}
	a.telemetry.recordUsageDelta(d, a.tr.Sessions()[d.SessionID])
}

func TestHandleEventRoutesTabKinds(t *testing.T) {
	a := &App{tr: tracker.NewTracker()}

	a.handleEvent(events.NormalizedEvent{Kind: events.KindTabCreated, TabID: "w1:t1", WorkspaceID: "w1", Label: "herdr-observr"})
	a.handleEvent(events.NormalizedEvent{Kind: events.KindTabRenamed, TabID: "w1:t1", Label: "agents"})
	a.handleEvent(events.NormalizedEvent{Kind: events.KindTabClosed, TabID: "w1:t1", WorkspaceID: "w1"})

	// After the close the tab is retained in its grace window (2.7) until the
	// app's evict ticker drops it; eviction itself is covered by the tracker
	// tests with a controllable clock, since ClosedAt is tracker-internal.
	if got := len(a.tr.Tabs()); got != 1 {
		t.Errorf("tab after full lifecycle = %d entries, want 1 (retained during grace)", got)
	}
}

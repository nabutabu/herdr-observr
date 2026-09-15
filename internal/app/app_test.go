package app

import (
	"context"
	"testing"

	"github.com/nabutabu/herdr-scribe/internal/events"
	"github.com/nabutabu/herdr-scribe/internal/otel"
	"github.com/nabutabu/herdr-scribe/internal/snapshot"
	"github.com/nabutabu/herdr-scribe/internal/tracker"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
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
	telemetry.Shutdown(context.Background())
}

// drainTransitions replicates Run's select-case consumer, forwarding each
// transition to the counter exactly as production does.
func drainTransitions(t *testing.T, a *App) {
	t.Helper()
	for {
		select {
		case tr := <-a.tr.Transitions():
			a.telemetry.recordTransition(tr)
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

func TestAgentCountGaugesEventDriven(t *testing.T) {
	reader := metric.NewManualReader()
	mp := otel.NewMeterProviderWithReader(reader, resource.NewSchemaless(attribute.String("service.name", "test")))
	defer func() { _ = mp.Shutdown(context.Background()) }()

	a := &App{telemetry: &Telemetry{meter: mp.Meter(otel.MeterName)}, tr: tracker.NewTracker()}
	a.telemetry.registerAgentCounts(a.tr.Counts)

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
}

func TestAgentCountGaugesNoopWithoutTelemetry(t *testing.T) {
	// Telemetry disabled (nil meter): registering the gauges must be a no-op,
	// never a panic or block.
	a := &App{tr: tracker.NewTracker()}
	a.telemetry.registerAgentCounts(a.tr.Counts)
	a.tr.ApplyAgentStatusChanged(events.NormalizedEvent{Kind: events.KindAgentStatusChanged, PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusWorking})
}

func TestHandleEventRoutesTabKinds(t *testing.T) {
	a := &App{tr: tracker.NewTracker()}

	a.handleEvent(events.NormalizedEvent{Kind: events.KindTabCreated, TabID: "w1:t1", WorkspaceID: "w1", Label: "herdr-scribe"})
	a.handleEvent(events.NormalizedEvent{Kind: events.KindTabRenamed, TabID: "w1:t1", Label: "agents"})
	a.handleEvent(events.NormalizedEvent{Kind: events.KindTabClosed, TabID: "w1:t1", WorkspaceID: "w1"})

	// After the close the tab is retained in its grace window (2.7) until the
	// app's evict ticker drops it; eviction itself is covered by the tracker
	// tests with a controllable clock, since ClosedAt is tracker-internal.
	if got := len(a.tr.Tabs()); got != 1 {
		t.Errorf("tab after full lifecycle = %d entries, want 1 (retained during grace)", got)
	}
}

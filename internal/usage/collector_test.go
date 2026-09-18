package usage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nabutabu/herdr-observr/internal/snapshot"
	"github.com/nabutabu/herdr-observr/internal/tracker"
)

// fakeSource is a controllable UsageAdapter: totals keyed by ref.Value, an
// injectable error, and a call counter for dispatch assertions.
type fakeSource struct {
	agentType string

	mu     sync.Mutex
	totals map[string]UsageTotals
	err    error
	calls  int
}

var _ UsageAdapter = (*fakeSource)(nil)

func (f *fakeSource) AgentType() string { return f.agentType }

func (f *fakeSource) PollUsage(_ context.Context, ref AgentSessionRef) (UsageTotals, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return UsageTotals{}, f.err
	}
	t, ok := f.totals[ref.Value]
	if !ok {
		return UsageTotals{}, fmt.Errorf("no totals for %q", ref.Value)
	}
	return t, nil
}

func (f *fakeSource) setTotals(v string, t UsageTotals) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.totals == nil {
		f.totals = map[string]UsageTotals{}
	}
	f.totals[v] = t
}

func (f *fakeSource) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeSource) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// agentPane builds a live pane carrying the given agent_session attribution.
func agentPane(paneID, agentType, source, value string) tracker.PaneState {
	return tracker.PaneState{
		PaneID: paneID,
		Agent:  agentType,
		AgentSession: &snapshot.AgentSessionInfo{
			Source: source,
			Agent:  agentType,
			Kind:   snapshot.AgentSessionRefKindID,
			Value:  value,
		},
	}
}

func totals(sid string, cost float64, in, out, reason, cacheRead, cacheWrite int64) UsageTotals {
	return UsageTotals{
		SessionID:        sid,
		CostUSD:          cost,
		InputTokens:      in,
		OutputTokens:     out,
		ReasoningTokens:  reason,
		CacheReadTokens:  cacheRead,
		CacheWriteTokens: cacheWrite,
	}
}

// drainDeltas collects everything currently buffered on the collector's deltas
// channel. pollOnce is synchronous with non-blocking emits, so after a poll
// returns, whatever was emitted is already in the buffer.
func drainDeltas(c *UsageCollector) []UsageDelta {
	var out []UsageDelta
	for {
		select {
		case d := <-c.deltas:
			out = append(out, d)
		default:
			return out
		}
	}
}

func assertDeltaEqual(t *testing.T, got, want UsageDelta) {
	t.Helper()
	if got.SessionID != want.SessionID {
		t.Errorf("SessionID = %q, want %q", got.SessionID, want.SessionID)
	}
	if !approx(got.CostUSD, want.CostUSD) {
		t.Errorf("CostUSD = %v, want %v", got.CostUSD, want.CostUSD)
	}
	if got.InputTokens != want.InputTokens ||
		got.OutputTokens != want.OutputTokens ||
		got.ReasoningTokens != want.ReasoningTokens ||
		got.CacheReadTokens != want.CacheReadTokens ||
		got.CacheWriteTokens != want.CacheWriteTokens {
		t.Errorf("delta = %+v, want %+v", got, want)
	}
}

func approx(a, b float64) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff < 1e-9
}

func TestCollectorSeedsThenEmitsAccrual(t *testing.T) {
	src := &fakeSource{agentType: "opencode"}
	src.setTotals("ses-1", totals("ses-1", 0.42, 100, 50, 10, 5, 2))

	c := NewUsageCollector(func() map[string]tracker.PaneState {
		return map[string]tracker.PaneState{"w1:p1": agentPane("w1:p1", "opencode", "herdr:opencode", "ses-1")}
	}, src)

	c.pollOnce(context.Background())
	if got := drainDeltas(c); len(got) != 0 {
		t.Fatalf("first poll emitted deltas, want seed-only: %+v", got)
	}

	src.setTotals("ses-1", totals("ses-1", 0.52, 140, 70, 12, 8, 3))
	c.pollOnce(context.Background())

	got := drainDeltas(c)
	if len(got) != 1 {
		t.Fatalf("second poll emitted %d deltas, want 1", len(got))
	}
	want := UsageDelta{SessionID: "ses-1", CostUSD: 0.10, InputTokens: 40, OutputTokens: 20, ReasoningTokens: 2, CacheReadTokens: 3, CacheWriteTokens: 1}
	assertDeltaEqual(t, got[0], want)

	// Third poll with no change: nothing to emit.
	c.pollOnce(context.Background())
	if got := drainDeltas(c); len(got) != 0 {
		t.Errorf("unchanged poll emitted deltas: %+v", got)
	}
}

func TestCollectorRoutesByAgentType(t *testing.T) {
	oc := &fakeSource{agentType: "opencode"}
	oc.setTotals("ses-o", totals("ses-o", 1, 10, 0, 0, 0, 0))
	cx := &fakeSource{agentType: "codex"}
	cx.setTotals("ses-c", totals("ses-c", 2, 20, 0, 0, 0, 0))

	c := NewUsageCollector(func() map[string]tracker.PaneState {
		return map[string]tracker.PaneState{
			"w1:p1": agentPane("w1:p1", "opencode", "herdr:opencode", "ses-o"),
			"w1:p2": agentPane("w1:p2", "codex", "herdr:codex", "ses-c"),
		}
	}, oc, cx)

	c.pollOnce(context.Background()) // seed both
	c.pollOnce(context.Background()) // emit no-op (unchanged)

	if got := len(drainDeltas(c)); got != 0 {
		t.Fatalf("unchanged routing poll emitted %d deltas", got)
	}
	if oc.callCount() != 2 || cx.callCount() != 2 {
		t.Errorf("call counts = opencode %d, codex %d, want 2 each", oc.callCount(), cx.callCount())
	}
}

func TestCollectorSkipsPanesWithoutSession(t *testing.T) {
	src := &fakeSource{agentType: "opencode"}
	c := NewUsageCollector(func() map[string]tracker.PaneState {
		p := agentPane("w1:p1", "opencode", "herdr:opencode", "ses-1")
		p.AgentSession = nil
		return map[string]tracker.PaneState{"w1:p1": p}
	}, src)

	c.pollOnce(context.Background())
	if got := drainDeltas(c); len(got) != 0 {
		t.Errorf("nil-session pane emitted deltas: %+v", got)
	}
	if src.callCount() != 0 {
		t.Errorf("adapter polled for nil-session pane: %d calls", src.callCount())
	}
}

func TestCollectorSkipsUnregisteredAgentType(t *testing.T) {
	src := &fakeSource{agentType: "opencode"}
	c := NewUsageCollector(func() map[string]tracker.PaneState {
		return map[string]tracker.PaneState{"w1:p1": agentPane("w1:p1", "codex", "herdr:codex", "ses-1")}
	}, src)

	c.pollOnce(context.Background())
	if got := drainDeltas(c); len(got) != 0 {
		t.Errorf("unregistered agent type emitted deltas: %+v", got)
	}
	if src.callCount() != 0 {
		t.Errorf("adapter polled for unregistered agent type: %d calls", src.callCount())
	}
}

func TestCollectorDispatchFallsBackToSessionAgent(t *testing.T) {
	src := &fakeSource{agentType: "opencode"}
	src.setTotals("ses-1", totals("ses-1", 0.1, 1, 0, 0, 0, 0))

	c := NewUsageCollector(func() map[string]tracker.PaneState {
		// Pane-level agent not yet detected; only the session's own agent type
		// is available to route on.
		p := agentPane("w1:p1", "", "herdr:opencode", "ses-1")
		p.AgentSession.Agent = "opencode"
		return map[string]tracker.PaneState{"w1:p1": p}
	}, src)

	c.pollOnce(context.Background())
	if src.callCount() != 1 {
		t.Errorf("no fallback dispatch: %d calls, want 1", src.callCount())
	}
}

func TestCollectorSkipsGracingPane(t *testing.T) {
	src := &fakeSource{agentType: "opencode"}
	c := NewUsageCollector(func() map[string]tracker.PaneState {
		p := agentPane("w1:p1", "opencode", "herdr:opencode", "ses-1")
		p.ClosedAt = time.Now()
		return map[string]tracker.PaneState{"w1:p1": p}
	}, src)

	c.pollOnce(context.Background())
	if got := drainDeltas(c); len(got) != 0 {
		t.Errorf("gracing pane emitted deltas: %+v", got)
	}
	if src.callCount() != 0 {
		t.Errorf("adapter polled for gracing pane: %d calls", src.callCount())
	}
}

func TestCollectorAdapterErrorKeepsCursor(t *testing.T) {
	src := &fakeSource{agentType: "opencode"}
	src.setTotals("ses-1", totals("ses-1", 1, 100, 0, 0, 0, 0))

	c := NewUsageCollector(func() map[string]tracker.PaneState {
		return map[string]tracker.PaneState{"w1:p1": agentPane("w1:p1", "opencode", "herdr:opencode", "ses-1")}
	}, src)

	c.pollOnce(context.Background()) // seed at 100

	src.setErr(errors.New("source unavailable"))
	c.pollOnce(context.Background()) // poll fails, cursor untouched
	if got := drainDeltas(c); len(got) != 0 {
		t.Errorf("errored poll emitted deltas: %+v", got)
	}

	src.setErr(nil)
	src.setTotals("ses-1", totals("ses-1", 1, 140, 0, 0, 0, 0))
	c.pollOnce(context.Background()) // now accrual of 40 since the seed baseline

	got := drainDeltas(c)
	if len(got) != 1 {
		t.Fatalf("recovery poll emitted %d deltas, want 1", len(got))
	}
	if got[0].InputTokens != 40 {
		t.Errorf("delta input = %d, want 40 (accrual since seed, not since errored poll)", got[0].InputTokens)
	}
}

func TestCollectorDecreaseReseedsCursor(t *testing.T) {
	src := &fakeSource{agentType: "opencode"}
	src.setTotals("ses-1", totals("ses-1", 1, 100, 0, 0, 0, 0))

	c := NewUsageCollector(func() map[string]tracker.PaneState {
		return map[string]tracker.PaneState{"w1:p1": agentPane("w1:p1", "opencode", "herdr:opencode", "ses-1")}
	}, src)

	c.pollOnce(context.Background()) // seed at 100

	// Source reset: totals drop below the cursor.
	src.setTotals("ses-1", totals("ses-1", 0, 20, 0, 0, 0, 0))
	c.pollOnce(context.Background())
	if got := drainDeltas(c); len(got) != 0 {
		t.Errorf("decreased poll emitted (negative) deltas: %+v", got)
	}

	// Accrual from the new baseline (20) is emitted on the next poll.
	src.setTotals("ses-1", totals("ses-1", 0, 30, 0, 0, 0, 0))
	c.pollOnce(context.Background())
	got := drainDeltas(c)
	if len(got) != 1 {
		t.Fatalf("post-reset poll emitted %d deltas, want 1", len(got))
	}
	if got[0].InputTokens != 10 || got[0].CostUSD != 0 {
		t.Errorf("post-reset delta = %+v, want input 10 cost 0", got[0])
	}
}

func TestCollectorKeysByAdapterSessionID(t *testing.T) {
	// The adapter reports a SessionID that is not its lookup value: the cursor
	// and emitted delta must be keyed by the reported SessionID.
	src := &fakeSource{agentType: "opencode"}
	src.setTotals("v-1", totals("reported-sid", 1, 100, 0, 0, 0, 0))

	c := NewUsageCollector(func() map[string]tracker.PaneState {
		return map[string]tracker.PaneState{"w1:p1": agentPane("w1:p1", "opencode", "herdr:opencode", "v-1")}
	}, src)

	c.pollOnce(context.Background()) // seed keyed "reported-sid"
	c.pollOnce(context.Background()) // unchanged

	if got := drainDeltas(c); len(got) != 0 {
		t.Fatalf("unchanged poll emitted deltas: %+v", got)
	}

	src.setTotals("v-1", totals("reported-sid", 1, 120, 0, 0, 0, 0))
	c.pollOnce(context.Background())
	got := drainDeltas(c)
	if len(got) != 1 {
		t.Fatalf("increase poll emitted %d deltas, want 1", len(got))
	}
	if got[0].SessionID != "reported-sid" || got[0].InputTokens != 20 {
		t.Errorf("delta = %+v, want session %q input 20", got[0], "reported-sid")
	}

	// A different pane resolving to the same reported SessionID shares the
	// cursor: no delta for an unchanged second observation.
	src.setTotals("v-2", totals("reported-sid", 1, 120, 0, 0, 0, 0))
	c.pollOnce(context.Background())
	if got := drainDeltas(c); len(got) != 0 {
		t.Errorf("same-session second ref emitted a delta: %+v", got)
	}
}

func TestCollectorZeroCostButTokensFlow(t *testing.T) {
	// The live-observed real-world case (PLAN.md U0.3): cost pinned at zero
	// while token counters advance. Deltas must carry the tokens regardless.
	src := &fakeSource{agentType: "opencode"}
	src.setTotals("ses-1", totals("ses-1", 0, 100, 50, 10, 5, 2))

	c := NewUsageCollector(func() map[string]tracker.PaneState {
		return map[string]tracker.PaneState{"w1:p1": agentPane("w1:p1", "opencode", "herdr:opencode", "ses-1")}
	}, src)

	c.pollOnce(context.Background()) // seed
	src.setTotals("ses-1", totals("ses-1", 0, 140, 70, 12, 8, 3))
	c.pollOnce(context.Background())

	got := drainDeltas(c)
	if len(got) != 1 {
		t.Fatalf("emitted %d deltas, want 1", len(got))
	}
	if got[0].CostUSD != 0 || got[0].InputTokens != 40 || got[0].OutputTokens != 20 {
		t.Errorf("delta = %+v, want cost 0 input 40 output 20", got[0])
	}
}

func TestCollectorRegisterReplacesDuplicateAgentType(t *testing.T) {
	first := &fakeSource{agentType: "opencode"}
	second := &fakeSource{agentType: "opencode"}
	first.setTotals("ses-1", totals("ses-1", 1, 100, 0, 0, 0, 0))
	second.setTotals("ses-1", totals("ses-1", 2, 200, 0, 0, 0, 0))

	c := NewUsageCollector(func() map[string]tracker.PaneState {
		return map[string]tracker.PaneState{"w1:p1": agentPane("w1:p1", "opencode", "herdr:opencode", "ses-1")}
	}, first)
	c.Register(second)

	c.pollOnce(context.Background())
	if first.callCount() != 0 {
		t.Errorf("superseded adapter polled: %d calls", first.callCount())
	}
	if second.callCount() != 1 {
		t.Errorf("replacement adapter not polled: %d calls", second.callCount())
	}
}

func TestCollectorRunPollsOnIntervalAndStopsOnCancel(t *testing.T) {
	src := &fakeSource{agentType: "opencode"}
	src.setTotals("ses-1", totals("ses-1", 1, 100, 0, 0, 0, 0))

	c := NewUsageCollector(func() map[string]tracker.PaneState {
		return map[string]tracker.PaneState{"w1:p1": agentPane("w1:p1", "opencode", "herdr:opencode", "ses-1")}
	}, src)
	c.interval = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx)
	}()

	// Wait for the first tick to seed the cursor before bumping totals —
	// bumping first would make the very first poll see the increase.
	seedDeadline := time.After(2 * time.Second)
	for src.callCount() < 1 {
		select {
		case <-seedDeadline:
			t.Fatal("Run never performed its first poll")
		case <-time.After(5 * time.Millisecond):
		}
	}

	// The next tick observes the increase and emits the accrued delta.
	deadline := time.After(2 * time.Second)
	src.setTotals("ses-1", totals("ses-1", 1, 150, 0, 0, 0, 0))
	for {
		if got := drainDeltas(c); len(got) == 1 && got[0].InputTokens == 50 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("Run never emitted the accrued delta on its ticker")
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancellation")
	}
}
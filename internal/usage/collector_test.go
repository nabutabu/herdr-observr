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
// Status is left at the zero (non-working) value by default; tests that assert
// active-set behavior set it explicitly.
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

// workingPane is agentPane plus a Status of working — the only status the
// collector treats as pollable.
func workingPane(paneID, agentType, source, value string) tracker.PaneState {
	p := agentPane(paneID, agentType, source, value)
	p.Status = snapshot.AgentStatusWorking
	return p
}

// liveSource is a goroutine-safe panes() source for tests that mutate pane
// state while Run is live. Its accessor returns a snapshot copy under the
// lock — mirroring Tracker.Panes() — so a test writing to the underlying map
// never races Run's reads. Tests with a static map keep plain closures.
type liveSource struct {
	mu    sync.Mutex
	panes map[string]tracker.PaneState
}

func newLiveSource() *liveSource {
	return &liveSource{panes: map[string]tracker.PaneState{}}
}

func (s *liveSource) set(id string, p tracker.PaneState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.panes[id] = p
}

func (s *liveSource) snapshot() map[string]tracker.PaneState {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]tracker.PaneState, len(s.panes))
	for k, v := range s.panes {
		out[k] = v
	}
	return out
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
// channel. pollPane is synchronous with non-blocking emits, so after a poll
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

// pollPaneID drives one pane through the synchronous pollPane seam by looking
// it up in the collector's own panes() accessor. This is the deterministic way
// to exercise diffing semantics without the async Run path.
func pollPaneID(c *UsageCollector, ctx context.Context, id string) {
	panes := c.panes()
	if p, ok := panes[id]; ok {
		c.pollPane(ctx, p)
	}
}

// awaitCalls blocks until the fake's call count reaches want, or the deadline
// expires. Mirrors the select-with-timeout pattern subscriber_test.go uses for
// async channel assertions — applySync/applyTransition run on Run's goroutine,
// so a test cannot observe their effect synchronously.
func awaitCalls(t *testing.T, f *fakeSource, want int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for f.callCount() < want {
		select {
		case <-deadline:
			t.Fatalf("adapter call count = %d, want %d", f.callCount(), want)
		case <-time.After(5 * time.Millisecond):
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

	pollPaneID(c, context.Background(), "w1:p1")
	if got := drainDeltas(c); len(got) != 0 {
		t.Fatalf("first poll emitted deltas, want seed-only: %+v", got)
	}

	src.setTotals("ses-1", totals("ses-1", 0.52, 140, 70, 12, 8, 3))
	pollPaneID(c, context.Background(), "w1:p1")

	got := drainDeltas(c)
	if len(got) != 1 {
		t.Fatalf("second poll emitted %d deltas, want 1", len(got))
	}
	want := UsageDelta{SessionID: "ses-1", CostUSD: 0.10, InputTokens: 40, OutputTokens: 20, ReasoningTokens: 2, CacheReadTokens: 3, CacheWriteTokens: 1}
	assertDeltaEqual(t, got[0], want)

	// Third poll with no change: nothing to emit.
	pollPaneID(c, context.Background(), "w1:p1")
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

	pollPaneID(c, context.Background(), "w1:p1") // seed both
	pollPaneID(c, context.Background(), "w1:p2")
	pollPaneID(c, context.Background(), "w1:p1") // emit no-op (unchanged)
	pollPaneID(c, context.Background(), "w1:p2")

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

	pollPaneID(c, context.Background(), "w1:p1")
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

	pollPaneID(c, context.Background(), "w1:p1")
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

	pollPaneID(c, context.Background(), "w1:p1")
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

	pollPaneID(c, context.Background(), "w1:p1")
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

	pollPaneID(c, context.Background(), "w1:p1") // seed at 100

	src.setErr(errors.New("source unavailable"))
	pollPaneID(c, context.Background(), "w1:p1") // poll fails, cursor untouched
	if got := drainDeltas(c); len(got) != 0 {
		t.Errorf("errored poll emitted deltas: %+v", got)
	}

	src.setErr(nil)
	src.setTotals("ses-1", totals("ses-1", 1, 140, 0, 0, 0, 0))
	pollPaneID(c, context.Background(), "w1:p1") // now accrual of 40 since the seed baseline

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

	pollPaneID(c, context.Background(), "w1:p1") // seed at 100

	// Source reset: totals drop below the cursor.
	src.setTotals("ses-1", totals("ses-1", 0, 20, 0, 0, 0, 0))
	pollPaneID(c, context.Background(), "w1:p1")
	if got := drainDeltas(c); len(got) != 0 {
		t.Errorf("decreased poll emitted (negative) deltas: %+v", got)
	}

	// Accrual from the new baseline (20) is emitted on the next poll.
	src.setTotals("ses-1", totals("ses-1", 0, 30, 0, 0, 0, 0))
	pollPaneID(c, context.Background(), "w1:p1")
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

	pollPaneID(c, context.Background(), "w1:p1") // seed keyed "reported-sid"
	pollPaneID(c, context.Background(), "w1:p1") // unchanged

	if got := drainDeltas(c); len(got) != 0 {
		t.Fatalf("unchanged poll emitted deltas: %+v", got)
	}

	src.setTotals("v-1", totals("reported-sid", 1, 120, 0, 0, 0, 0))
	pollPaneID(c, context.Background(), "w1:p1")
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
	pollPaneID(c, context.Background(), "w1:p1")
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

	pollPaneID(c, context.Background(), "w1:p1") // seed
	src.setTotals("ses-1", totals("ses-1", 0, 140, 70, 12, 8, 3))
	pollPaneID(c, context.Background(), "w1:p1")

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

	pollPaneID(c, context.Background(), "w1:p1")
	if first.callCount() != 0 {
		t.Errorf("superseded adapter polled: %d calls", first.callCount())
	}
	if second.callCount() != 1 {
		t.Errorf("replacement adapter not polled: %d calls", second.callCount())
	}
}

func TestCollectorTransitionInPollsImmediately(t *testing.T) {
	// A working transition must poll right away, not wait for the next tick.
	// A long interval (a tick could never pass during the test) proves the
	// poll was driven by the transition, not the ticker.
	src := &fakeSource{agentType: "opencode"}
	src.setTotals("ses-1", totals("ses-1", 1, 100, 0, 0, 0, 0))

	panes := newLiveSource()
	panes.set("w1:p1", workingPane("w1:p1", "opencode", "herdr:opencode", "ses-1"))
	c := NewUsageCollector(panes.snapshot, src)
	c.interval = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	c.NotifyTransition(tracker.AgentTransition{PaneID: "w1:p1", New: snapshot.AgentStatusWorking})
	awaitCalls(t, src, 1)
}

func TestCollectorTransitionOutTrailingPollThenStops(t *testing.T) {
	src := &fakeSource{agentType: "opencode"}
	src.setTotals("ses-1", totals("ses-1", 1, 100, 0, 0, 0, 0))

	panes := newLiveSource()
	panes.set("w1:p1", workingPane("w1:p1", "opencode", "herdr:opencode", "ses-1"))
	c := NewUsageCollector(panes.snapshot, src)
	c.interval = time.Hour          // no poll ticks: entry + trailing poll are both transition-driven
	c.reconcileInterval = time.Hour // no self-heal scans either, so the assertion is exact

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	c.NotifyTransition(tracker.AgentTransition{PaneID: "w1:p1", New: snapshot.AgentStatusWorking})
	awaitCalls(t, src, 1) // immediate poll on entry

	// Leaving working does one trailing poll before deactivation. The source is
	// deliberately NOT mutated yet: the trailing poll must fire purely off the
	// transition-out, regardless of what the next snapshot would say.
	c.NotifyTransition(tracker.AgentTransition{PaneID: "w1:p1", New: snapshot.AgentStatusIdle})
	awaitCalls(t, src, 2)
	if got := src.callCount(); got != 2 {
		t.Fatalf("call count after transition-out = %d, want 2 (entry + one trailing poll)", got)
	}

	// With the pane out of active, nothing re-polls it. Drive a few full sync
	// cycles through Run (the same applySync path the self-heal reconcile uses)
	// against an idle map: no polls happen, so the count holds at 2.
	panes.set("w1:p1", agentPane("w1:p1", "opencode", "herdr:opencode", "ses-1")) // source now idle
	for i := 0; i < 20; i++ {
		c.RequestSync(panes.snapshot())
		time.Sleep(2 * time.Millisecond)
	}
	if got := src.callCount(); got != 2 {
		t.Errorf("call count after idle cycles = %d, want 2 (no further polling once inactive)", got)
	}
}

func TestCollectorRequestSyncSeedsWorkingAndDropsStopped(t *testing.T) {
	src := &fakeSource{agentType: "opencode"}
	src.setTotals("ses-1", totals("ses-1", 1, 100, 0, 0, 0, 0))
	src.setTotals("ses-2", totals("ses-2", 1, 100, 0, 0, 0, 0))

	panes := newLiveSource()
	panes.set("w1:p1", workingPane("w1:p1", "opencode", "herdr:opencode", "ses-1"))
	panes.set("w1:p2", agentPane("w1:p2", "opencode", "herdr:opencode", "ses-2")) // not working
	c := NewUsageCollector(panes.snapshot, src)
	c.interval = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	// Bootstrap: the already-working pane is seeded via applySync's immediate
	// first poll; the non-working pane is never polled.
	c.RequestSync(panes.snapshot())
	awaitCalls(t, src, 1)
	if got := src.callCount(); got != 1 {
		t.Fatalf("bootstrapped call count = %d, want 1 (only the working pane)", got)
	}

	// The pane stops working while unobserved; the next sync drops it from the
	// active set, so ticks have nothing to poll.
	p := agentPane("w1:p1", "opencode", "herdr:opencode", "ses-1")
	p.Status = snapshot.AgentStatusIdle
	panes.set("w1:p1", p)

	c.RequestSync(panes.snapshot())
	time.Sleep(50 * time.Millisecond)
	if got := src.callCount(); got != 1 {
		t.Errorf("call count after idle resync = %d, want 1 (no further polls)", got)
	}
}

func TestCollectorConcurrentTransitionsAndSyncNoRace(t *testing.T) {
	// Regression for the concurrency bug this revision fixes: transitions and
	// syncs used to be applied from the caller's goroutine, racing Run over
	// the active/cursors maps. Both inlets are now channel handoffs that Run
	// itself drains. Run the suite with -race; a silent data race here means
	// the single-owner guarantee is broken.
	src := &fakeSource{agentType: "opencode"}
	src.setTotals("ses-1", totals("ses-1", 1, 100, 0, 0, 0, 0))

	panes := newLiveSource()
	panes.set("w1:p1", workingPane("w1:p1", "opencode", "herdr:opencode", "ses-1"))
	c := NewUsageCollector(panes.snapshot, src)
	c.interval = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			c.NotifyTransition(tracker.AgentTransition{PaneID: "w1:p1", New: snapshot.AgentStatusWorking})
			c.NotifyTransition(tracker.AgentTransition{PaneID: "w1:p1", New: snapshot.AgentStatusIdle})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			c.RequestSync(panes.snapshot())
		}
	}()
	wg.Wait()
}

func TestCollectorCloseEvictsActivePane(t *testing.T) {
	// Closing a working pane emits NO transition (close is not a state
	// transition), so without pollPaneInMap's ClosedAt check the pane would sit
	// in active taking a no-op poll every tick. After the close a poll tick
	// must evict it and stop the polling entirely.
	src := &fakeSource{agentType: "opencode"}
	src.setTotals("ses-1", totals("ses-1", 1, 100, 0, 0, 0, 0))

	panes := newLiveSource()
	panes.set("w1:p1", workingPane("w1:p1", "opencode", "herdr:opencode", "ses-1"))
	c := NewUsageCollector(panes.snapshot, src)
	c.interval = 5 * time.Millisecond
	c.reconcileInterval = time.Hour // keep the self-heal scan out of this path

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	c.RequestSync(panes.snapshot()) // activate the working pane
	awaitCalls(t, src, 1)

	// Close the pane mid-working: ClosedAt stamped, no transition fired.
	p := workingPane("w1:p1", "opencode", "herdr:opencode", "ses-1")
	p.ClosedAt = time.Now()
	panes.set("w1:p1", p)

	time.Sleep(40 * time.Millisecond) // several poll ticks: eviction happens
	settled := src.callCount()

	time.Sleep(40 * time.Millisecond) // several more ticks: must stay flat
	if got := src.callCount(); got != settled {
		t.Errorf("call count grew from %d to %d after close; pane not evicted from active", settled, got)
	}
}

func TestCollectorFirstSeenWorkingCaughtByReconcile(t *testing.T) {
	// An in-scope pane whose first-ever status is `working` emits no
	// transition (the tracker's first-seen upsert), so neither the transition
	// channel nor a RequestSync can activate it. Only Run's self-heal reconcile
	// scan can. Assert it gets polled within one short reconcile interval.
	src := &fakeSource{agentType: "opencode"}
	src.setTotals("ses-1", totals("ses-1", 1, 100, 0, 0, 0, 0))

	panes := newLiveSource()
	panes.set("w1:p1", workingPane("w1:p1", "opencode", "herdr:opencode", "ses-1"))
	c := NewUsageCollector(panes.snapshot, src)
	c.interval = time.Hour          // no poll ticks: only the reconcile scan can poll
	c.reconcileInterval = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	awaitCalls(t, src, 1)
}

func TestCollectorRunStopsOnCancel(t *testing.T) {
	c := NewUsageCollector(func() map[string]tracker.PaneState { return nil })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancellation")
	}
}
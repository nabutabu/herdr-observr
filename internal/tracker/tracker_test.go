package tracker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/nabutabu/herdr-observr/internal/events"
	"github.com/nabutabu/herdr-observr/internal/snapshot"
)

func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}

func workingPanes(agent string) []snapshot.Pane {
	return []snapshot.Pane{{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1", AgentStatus: snapshot.AgentStatusWorking}}
}

func TestApplyEventsTrackState(t *testing.T) {
	tr := NewTracker()
	tr.ApplyWorkspaceCreated(events.NormalizedEvent{WorkspaceID: "w1"})
	tr.ApplyPaneCreated(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1"})
	tr.ApplyAgentDetected(events.NormalizedEvent{PaneID: "w1:p1", Agent: "codex"})
	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", Agent: "codex", NewState: snapshot.AgentStatusWorking})

	tr.mu.RLock()
	defer tr.mu.RUnlock()

	if _, ok := tr.workspaces["w1"]; !ok {
		t.Error("workspace w1 not tracked")
	}
	pane, ok := tr.panes["w1:p1"]
	if !ok {
		t.Fatal("pane w1:p1 not tracked")
	}
	if pane.Agent != "codex" || pane.Status != snapshot.AgentStatusWorking || pane.TabID != "w1:t1" {
		t.Errorf("pane = %+v", pane)
	}
	if _, ok := tr.tabs["w1:t1"]; !ok {
		t.Error("tab w1:t1 not seeded from pane.created")
	}
}

func TestApplyAgentStatusChangedUpsertsUnknownPane(t *testing.T) {
	tr := NewTracker()
	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusBlocked})

	tr.mu.RLock()
	defer tr.mu.RUnlock()

	pane, ok := tr.panes["w1:p1"]
	if !ok {
		t.Fatal("pane w1:p1 not upserted")
	}
	if pane.Status != snapshot.AgentStatusBlocked {
		t.Errorf("pane status = %q, want blocked", pane.Status)
	}
}

func TestApplyAgentStatusChangedTracksDurations(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	change := func(s snapshot.AgentStatus) {
		tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: s})
	}

	change(snapshot.AgentStatusWorking) // opens working at t0
	cur = cur.Add(2 * time.Minute)
	change(snapshot.AgentStatusBlocked) // closes working: 2m
	cur = cur.Add(3 * time.Minute)
	change(snapshot.AgentStatusWorking) // closes blocked: 3m

	tr.mu.RLock()
	defer tr.mu.RUnlock()

	ag, ok := tr.agents["w1:p1"]
	if !ok {
		t.Fatal("agent w1:p1 not tracked")
	}
	if ag.Status != snapshot.AgentStatusWorking {
		t.Errorf("agent status = %q, want working", ag.Status)
	}
	if d := ag.DurationByState[snapshot.AgentStatusWorking]; d != 2*time.Minute {
		t.Errorf("working duration = %s, want 2m", d)
	}
	if d := ag.DurationByState[snapshot.AgentStatusBlocked]; d != 3*time.Minute {
		t.Errorf("blocked duration = %s, want 3m", d)
	}
}

func TestApplyAgentStatusChangedIgnoresSameState(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	change := func(s snapshot.AgentStatus) {
		tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: s})
	}

	change(snapshot.AgentStatusWorking) // opens working at t0
	cur = cur.Add(time.Minute)
	change(snapshot.AgentStatusWorking) // duplicate: must not reset the clock
	cur = cur.Add(time.Minute)
	change(snapshot.AgentStatusBlocked) // closes working: 2m, not 1m

	tr.mu.RLock()
	defer tr.mu.RUnlock()

	ag := tr.agents["w1:p1"]
	if d := ag.DurationByState[snapshot.AgentStatusWorking]; d != 2*time.Minute {
		t.Errorf("working duration = %s, want 2m (duplicate must not double-count or reset)", d)
	}
}

func TestApplyAgentStatusChangedClosesAttentionLatency(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusDone})
	if ag := tr.agents["w1:p1"]; ag.AttentionStartedAt != cur {
		t.Fatalf("entering done did not start clock: AttentionStartedAt=%v", ag.AttentionStartedAt)
	}

	cur = cur.Add(7 * time.Minute)
	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusWorking})

	select {
	case al := <-tr.AttentionLatency():
		if al.Duration != 7*time.Minute {
			t.Errorf("attention latency = %s, want 7m", al.Duration)
		}
		if al.PaneID != "w1:p1" || al.Agent != "codex" || al.WorkspaceID != "w1" {
			t.Errorf("latency metadata = %+v", al)
		}
	default:
		t.Fatal("no attention latency emitted on leaving done")
	}
}

func TestApplyAgentStatusChangedDoneDuplicateDoesNotReArmClock(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	change := func(s snapshot.AgentStatus) {
		tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: s})
	}

	change(snapshot.AgentStatusDone) // opens done at t0
	cur = cur.Add(time.Minute)
	change(snapshot.AgentStatusDone) // duplicate: must not re-open the clock
	cur = cur.Add(2 * time.Minute)
	change(snapshot.AgentStatusBlocked) // closes done: 3m, not 2m

	select {
	case al := <-tr.AttentionLatency():
		if al.Duration != 3*time.Minute {
			t.Errorf("attention latency = %s, want 3m (duplicate must not re-arm)", al.Duration)
		}
	default:
		t.Fatal("no attention latency emitted on leaving done")
	}
}

func TestApplySnapshotPreservesAttentionIntervalAcrossRebaseline(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	agent := "codex"
	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: agent, NewState: snapshot.AgentStatusDone})
	cur = cur.Add(10 * time.Minute)

	// Re-baseline with the pane still done (e.g. resubscribe for an unrelated
	// drift): the running attention clock must survive, not reset to now.
	tr.ApplySnapshot(snapshot.Snapshot{
		Panes: []snapshot.Pane{{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1", AgentStatus: snapshot.AgentStatusDone, Agent: &agent}},
	})

	ag := tr.agents["w1:p1"]
	if ag.Status != snapshot.AgentStatusDone || ag.AttentionStartedAt != cur.Add(-10*time.Minute) {
		t.Fatalf("agent state after re-baseline = %+v (clock must be preserved)", ag)
	}

	cur = cur.Add(5 * time.Minute)
	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: agent, NewState: snapshot.AgentStatusIdle})

	select {
	case al := <-tr.AttentionLatency():
		if al.Duration != 15*time.Minute {
			t.Errorf("attention latency = %s, want 15m (10m pre-rebaseline + 5m post)", al.Duration)
		}
	default:
		t.Fatal("no attention latency emitted after cross-rebaseline close")
	}
}

func TestApplySnapshotReseedsChangedStatus(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	agent := "codex"
	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: agent, NewState: snapshot.AgentStatusDone})
	cur = cur.Add(10 * time.Minute)

	// Snapshot reports working: genuine change we missed -> fresh floor at now.
	tr.ApplySnapshot(snapshot.Snapshot{
		Panes: []snapshot.Pane{{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1", AgentStatus: snapshot.AgentStatusWorking, Agent: &agent}},
	})

	ag := tr.agents["w1:p1"]
	if ag.Status != snapshot.AgentStatusWorking || ag.StateEnteredAt != cur {
		t.Fatalf("agent state after changed-status re-baseline = %+v (want fresh floor)", ag)
	}
}

func TestDiffClassifiesSeenFlipAsBenign(t *testing.T) {
	tr := NewTracker()
	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusDone})

	// Tracked done + snapshot idle = silent seen-flip, not a resubscribe-worthy drift.
	report := tr.Diff(snapshot.Snapshot{Panes: []snapshot.Pane{{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1", AgentStatus: snapshot.AgentStatusIdle}}})
	if report.Drifted() {
		t.Errorf("seen-flip must not count as drift: %+v", report.Drifts)
	}
	if len(report.SeenFlips) != 1 || report.SeenFlips[0].PaneID != "w1:p1" {
		t.Errorf("seen flips = %+v, want one for w1:p1", report.SeenFlips)
	}

	// Tracked done + snapshot working = real gap (agent resumed, event missed) -> drift.
	report = tr.Diff(snapshot.Snapshot{Panes: []snapshot.Pane{{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1", AgentStatus: snapshot.AgentStatusWorking}}})
	if !report.Drifted() {
		t.Fatal("done vs working must count as drift")
	}
	if len(report.SeenFlips) != 0 {
		t.Errorf("done vs working must not be a seen-flip: %+v", report.SeenFlips)
	}
}

func TestApplySeenFlipClosesAttentionLatency(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusDone})
	cur = cur.Add(4 * time.Minute)

	tr.ApplySeenFlip("w1:p1")

	select {
	case al := <-tr.AttentionLatency():
		if al.Duration != 4*time.Minute {
			t.Errorf("attention latency = %s, want 4m", al.Duration)
		}
	default:
		t.Fatal("no attention latency emitted on seen-flip")
	}

	ag := tr.agents["w1:p1"]
	if ag.Status != snapshot.AgentStatusIdle || !ag.AttentionStartedAt.IsZero() {
		t.Errorf("agent after seen-flip = %+v, want idle with zero clock", ag)
	}
	if d := ag.DurationByState[snapshot.AgentStatusDone]; d != 4*time.Minute {
		t.Errorf("closed done duration = %s, want 4m", d)
	}
	if pane := tr.panes["w1:p1"]; pane.Status != snapshot.AgentStatusIdle {
		t.Errorf("pane after seen-flip status = %q, want idle", pane.Status)
	}

	// The fold must be idempotent for the next reconcile tick: nothing to close now.
	cur = cur.Add(time.Minute)
	tr.ApplySeenFlip("w1:p1")
	select {
	case al := <-tr.AttentionLatency():
		t.Errorf("second seen-flip emitted another sample: %+v", al)
	default:
	}
}

func TestApplySeenFlipIsNoopWhenAgentLeftDone(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusDone})
	cur = cur.Add(2 * time.Minute)
	// The real resume event raced the reconcile diff and already closed done.
	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusWorking})
	<-tr.AttentionLatency() // drain the real close

	cur = cur.Add(time.Minute)
	tr.ApplySeenFlip("w1:p1")

	select {
	case al := <-tr.AttentionLatency():
		t.Errorf("seen-flip after real close emitted a stale sample: %+v", al)
	default:
	}
	if ag := tr.agents["w1:p1"]; ag.Status != snapshot.AgentStatusWorking {
		t.Errorf("agent status = %q, want working (seen-flip must not overwrite)", ag.Status)
	}
}

func nextTransition(t *testing.T, tr *Tracker) AgentTransition {
	t.Helper()
	select {
	case trns := <-tr.Transitions():
		return trns
	default:
		t.Fatal("no transition emitted")
		return AgentTransition{}
	}
}

func assertNoTransition(t *testing.T, tr *Tracker) {
	t.Helper()
	select {
	case trns := <-tr.Transitions():
		t.Errorf("unexpected transition emitted: %+v", trns)
	default:
	}
}

func nextStateDuration(t *testing.T, tr *Tracker) StateDuration {
	t.Helper()
	select {
	case sd := <-tr.StateDurations():
		return sd
	default:
		t.Fatal("no state duration emitted")
		return StateDuration{}
	}
}

func assertNoStateDuration(t *testing.T, tr *Tracker) {
	t.Helper()
	select {
	case sd := <-tr.StateDurations():
		t.Errorf("unexpected state duration emitted: %+v", sd)
	default:
	}
}

func TestTransitionsOnStatusChange(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }
	change := func(s snapshot.AgentStatus) {
		tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: s})
	}

	// First-seen upsert creates the agent but has no previous state to report
	// (3.3 semantics: a transition is a change between two known states).
	change(snapshot.AgentStatusWorking)
	assertNoTransition(t, tr)

	cur = cur.Add(2 * time.Minute)
	change(snapshot.AgentStatusBlocked)

	trns := nextTransition(t, tr)
	if trns.Previous != snapshot.AgentStatusWorking || trns.New != snapshot.AgentStatusBlocked {
		t.Errorf("transition = %+v, want working→blocked", trns)
	}
	if trns.Agent != "codex" || trns.PaneID != "w1:p1" || trns.WorkspaceID != "w1" {
		t.Errorf("transition identity = %+v", trns)
	}
	if trns.ObservedAt != cur {
		t.Errorf("transition ObservedAt = %v, want %v", trns.ObservedAt, cur)
	}

	cur = cur.Add(3 * time.Minute)
	change(snapshot.AgentStatusWorking)

	trns = nextTransition(t, tr)
	if trns.Previous != snapshot.AgentStatusBlocked || trns.New != snapshot.AgentStatusWorking {
		t.Errorf("transition = %+v, want blocked→working", trns)
	}
	assertNoTransition(t, tr)
}

func TestTransitionsNotEmittedOnSameState(t *testing.T) {
	tr := NewTracker()
	change := func(s snapshot.AgentStatus) {
		tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: s})
	}

	change(snapshot.AgentStatusWorking) // first-seen
	change(snapshot.AgentStatusWorking) // duplicate: idempotency no-op
	assertNoTransition(t, tr)

	change(snapshot.AgentStatusBlocked)
	if trns := nextTransition(t, tr); trns.Previous != snapshot.AgentStatusWorking || trns.New != snapshot.AgentStatusBlocked {
		t.Errorf("transition = %+v, want a single working→blocked", trns)
	}
	assertNoTransition(t, tr)
}

func TestTransitionsNotEmittedOnSnapshotBaseline(t *testing.T) {
	tr := NewTracker()
	agent := "codex"
	tr.ApplySnapshot(snapshot.Snapshot{
		Panes: []snapshot.Pane{{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1", AgentStatus: snapshot.AgentStatusWorking, Agent: &agent}},
	})
	// Re-baseline seeding establishes current state; it is not a transition.
	assertNoTransition(t, tr)
}

func TestTransitionsNotEmittedOnClose(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusDone})
	cur = cur.Add(3 * time.Minute)
	tr.ApplyPaneClosed(events.NormalizedEvent{PaneID: "w1:p1"})

	<-tr.AttentionLatency() // the close flushes attention latency, not a transition
	assertNoTransition(t, tr)
}

func TestTransitionsEmittedOnSeenFlip(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusDone})
	assertNoTransition(t, tr) // entering done from unknown is a first-seen, not a change

	cur = cur.Add(4 * time.Minute)
	tr.ApplySeenFlip("w1:p1")

	trns := nextTransition(t, tr)
	if trns.Previous != snapshot.AgentStatusDone || trns.New != snapshot.AgentStatusIdle {
		t.Errorf("seen-flip transition = %+v, want done→idle", trns)
	}
	if trns.Agent != "codex" {
		t.Errorf("seen-flip transition agent = %q, want codex", trns.Agent)
	}
	<-tr.AttentionLatency() // drain the close of the done interval

	// A second flip for an agent no longer done is a no-op: no transition.
	cur = cur.Add(time.Minute)
	tr.ApplySeenFlip("w1:p1")
	assertNoTransition(t, tr)
}

func TestStateDurationEmittedOnStatusChange(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }
	change := func(s snapshot.AgentStatus) {
		tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: s})
	}

	// First-seen upsert opens the interval with no prior state to close.
	change(snapshot.AgentStatusWorking)
	assertNoStateDuration(t, tr)

	cur = cur.Add(2 * time.Minute)
	change(snapshot.AgentStatusBlocked)

	sd := nextStateDuration(t, tr)
	if sd.State != snapshot.AgentStatusWorking || sd.Duration != 2*time.Minute {
		t.Errorf("state duration = %+v, want working for 2m", sd)
	}
	if sd.Agent != "codex" || sd.PaneID != "w1:p1" || sd.WorkspaceID != "w1" {
		t.Errorf("state duration identity = %+v", sd)
	}
	if sd.ObservedAt != cur {
		t.Errorf("state duration ObservedAt = %v, want %v", sd.ObservedAt, cur)
	}

	cur = cur.Add(3 * time.Minute)
	change(snapshot.AgentStatusWorking)

	sd = nextStateDuration(t, tr)
	if sd.State != snapshot.AgentStatusBlocked || sd.Duration != 3*time.Minute {
		t.Errorf("state duration = %+v, want blocked for 3m", sd)
	}
	assertNoStateDuration(t, tr)
}

func TestStateDurationEmittedOnSeenFlip(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusDone})
	assertNoStateDuration(t, tr) // entering done from unknown is a first-seen, not a close

	cur = cur.Add(4 * time.Minute)
	tr.ApplySeenFlip("w1:p1")

	sd := nextStateDuration(t, tr)
	if sd.State != snapshot.AgentStatusDone || sd.Duration != 4*time.Minute {
		t.Errorf("seen-flip state duration = %+v, want done for 4m", sd)
	}
	<-tr.AttentionLatency() // drain the separate done-close sample

	// A second flip for an agent no longer done is a no-op: no duration.
	cur = cur.Add(time.Minute)
	tr.ApplySeenFlip("w1:p1")
	assertNoStateDuration(t, tr)
}

func TestStateDurationEmittedOnCloseFlush(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusWorking})
	cur = cur.Add(2 * time.Minute)
	tr.ApplyPaneClosed(events.NormalizedEvent{PaneID: "w1:p1"})

	sd := nextStateDuration(t, tr)
	if sd.State != snapshot.AgentStatusWorking || sd.Duration != 2*time.Minute {
		t.Errorf("close-flush state duration = %+v, want working for 2m", sd)
	}
}

func TestStateDurationNotEmittedOnSameState(t *testing.T) {
	tr := NewTracker()
	change := func(s snapshot.AgentStatus) {
		tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: s})
	}

	change(snapshot.AgentStatusWorking) // first-seen
	change(snapshot.AgentStatusWorking) // duplicate: idempotency no-op
	assertNoStateDuration(t, tr)

	change(snapshot.AgentStatusBlocked)
	if sd := nextStateDuration(t, tr); sd.State != snapshot.AgentStatusWorking {
		t.Errorf("state duration = %+v, want a single working close", sd)
	}
	assertNoStateDuration(t, tr)
}

func TestStateDurationNotEmittedOnSnapshotBaseline(t *testing.T) {
	tr := NewTracker()
	agent := "codex"
	tr.ApplySnapshot(snapshot.Snapshot{
		Panes: []snapshot.Pane{{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1", AgentStatus: snapshot.AgentStatusWorking, Agent: &agent}},
	})
	// Re-baseline seeding establishes current state; it closes no interval.
	assertNoStateDuration(t, tr)
}

func TestStateDurationNotEmittedOnLateStatusInGrace(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusDone})
	cur = cur.Add(time.Minute)
	tr.ApplyPaneClosed(events.NormalizedEvent{PaneID: "w1:p1"})
	nextStateDuration(t, tr) // the close flush (done)
	<-tr.AttentionLatency()  // the close flush (attention latency)

	// Late event for the pane, still inside the grace window: absorbed.
	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusWorking})
	assertNoStateDuration(t, tr)
}

func TestStateDurationDoubleCascadeEmitsOnce(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	tr.ApplyWorkspaceCreated(events.NormalizedEvent{WorkspaceID: "w1"})
	tr.ApplyTabCreated(events.NormalizedEvent{TabID: "w1:t1", WorkspaceID: "w1"})
	tr.ApplyPaneCreated(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1"})
	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusDone})
	cur = cur.Add(4 * time.Minute)

	tr.ApplyTabClosed(events.NormalizedEvent{TabID: "w1:t1", WorkspaceID: "w1"})
	tr.ApplyWorkspaceClosed(events.NormalizedEvent{WorkspaceID: "w1"})

	sd := nextStateDuration(t, tr)
	if sd.State != snapshot.AgentStatusDone || sd.Duration != 4*time.Minute {
		t.Errorf("cascaded state duration = %+v, want done for 4m", sd)
	}
	assertNoStateDuration(t, tr)
	<-tr.AttentionLatency() // drain the separate done-close sample
}

func TestApplyPaneClosedFlushesDoneInterval(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusDone})
	cur = cur.Add(3 * time.Minute)
	tr.ApplyPaneClosed(events.NormalizedEvent{PaneID: "w1:p1"})

	select {
	case al := <-tr.AttentionLatency():
		if al.Duration != 3*time.Minute {
			t.Errorf("flushed attention latency = %s, want 3m", al.Duration)
		}
	default:
		t.Fatal("no attention latency emitted on pane close while done")
	}

	// Within the grace window the records are retained (2.7), counts already
	// decremented, durations freed.
	tr.mu.RLock()
	ag, agentOK := tr.agents["w1:p1"]
	pane, paneOK := tr.panes["w1:p1"]
	tr.mu.RUnlock()
	if !agentOK || ag.ClosedAt.IsZero() {
		t.Error("agent not retained with ClosedAt after close")
	}
	if ag.DurationByState != nil {
		t.Errorf("DurationByState not freed at close: %+v", ag.DurationByState)
	}
	if !paneOK || pane.ClosedAt.IsZero() {
		t.Error("pane not retained with ClosedAt after close")
	}

	// After the grace window the records are evicted.
	cur = cur.Add(16 * time.Second)
	tr.EvictExpired()
	tr.mu.RLock()
	_, agentGone := tr.agents["w1:p1"]
	_, paneGone := tr.panes["w1:p1"]
	tr.mu.RUnlock()
	if agentGone || paneGone {
		t.Error("pane/agent still tracked after eviction")
	}
}

func TestApplyPaneClosedForeignNoLatency(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusWorking})
	tr.ApplyPaneClosed(events.NormalizedEvent{PaneID: "w1:p1"})

	select {
	case al := <-tr.AttentionLatency():
		t.Errorf("closing a non-done pane emitted a sample: %+v", al)
	default:
	}

	cur = cur.Add(16 * time.Second)
	tr.EvictExpired()
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	if _, ok := tr.agents["w1:p1"]; ok {
		t.Error("agent of closed pane still tracked after eviction")
	}
	if _, ok := tr.panes["w1:p1"]; ok {
		t.Error("closed pane still tracked after eviction")
	}
}

// TestLateStatusChangeIgnoredDuringGrace verifies 2.7's core purpose: a status
// event arriving for a recently-closed pane is absorbed, not applied — it must
// not resurrect a phantom agent, re-emit attention latency, or move the counts.
func TestLateStatusChangeIgnoredDuringGrace(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusDone})
	cur = cur.Add(3 * time.Minute)
	tr.ApplyPaneClosed(events.NormalizedEvent{PaneID: "w1:p1"})
	<-tr.AttentionLatency() // drain the close flush

	// Late event for the pane, still inside the grace window.
	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusWorking})

	select {
	case al := <-tr.AttentionLatency():
		t.Errorf("late status change emitted a second sample: %+v", al)
	default:
	}
	c := tr.Counts()
	if got := c.Global[snapshot.AgentStatusWorking]; got != 0 {
		t.Errorf("late status change re-counted a working agent: %d", got)
	}

	tr.mu.RLock()
	ag, agentOK := tr.agents["w1:p1"]
	pane, paneOK := tr.panes["w1:p1"]
	tr.mu.RUnlock()
	if !agentOK || ag.ClosedAt.IsZero() {
		t.Fatal("agent not retained in grace window")
	}
	if ag.Status != snapshot.AgentStatusDone {
		t.Errorf("late status change mutated retained agent status: %q", ag.Status)
	}
	if !paneOK || pane.ClosedAt.IsZero() {
		t.Fatal("pane not retained in grace window")
	}

	cur = cur.Add(16 * time.Second)
	tr.EvictExpired()
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	if _, ok := tr.agents["w1:p1"]; ok {
		t.Error("agent still tracked after eviction")
	}
	if _, ok := tr.panes["w1:p1"]; ok {
		t.Error("pane still tracked after eviction")
	}
}

// TestWorkspaceClosedFlushesAttentionLatency verifies the 2.7 fix: closing a
// workspace must flush done agents' attention-latency intervals, matching
// ApplyPaneClosed/ApplyTabClosed — previously the interval was silently dropped.
func TestWorkspaceClosedFlushesAttentionLatency(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	tr.ApplyPaneCreated(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1"})
	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusDone})
	cur = cur.Add(5 * time.Minute)
	tr.ApplyWorkspaceClosed(events.NormalizedEvent{WorkspaceID: "w1"})

	select {
	case al := <-tr.AttentionLatency():
		if al.Duration != 5*time.Minute {
			t.Errorf("workspace-close attention latency = %s, want 5m", al.Duration)
		}
		if al.PaneID != "w1:p1" || al.WorkspaceID != "w1" {
			t.Errorf("workspace-close latency metadata = %+v", al)
		}
	default:
		t.Fatal("workspace close dropped a done agent's attention-latency interval")
	}
}

// TestClosedAgentIgnoresSeenFlipDuringGrace verifies a stale reconcile-driven
// seen-flip for a pane closed within its grace window is a no-op: the done
// interval was already flushed at close, so it must not emit again.
func TestClosedAgentIgnoresSeenFlipDuringGrace(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusDone})
	cur = cur.Add(time.Minute)
	tr.ApplyPaneClosed(events.NormalizedEvent{PaneID: "w1:p1"})
	<-tr.AttentionLatency() // drain the close flush

	tr.ApplySeenFlip("w1:p1")

	select {
	case al := <-tr.AttentionLatency():
		t.Errorf("seen-flip on closing agent emitted a sample: %+v", al)
	default:
	}
	c := tr.Counts()
	if got := c.Global[snapshot.AgentStatusIdle]; got != 0 {
		t.Errorf("seen-flip on closing agent counted idle: %d", got)
	}
}

// TestTabClosedThenWorkspaceClosedCascadesOnce verifies 2.7's cascade
// idempotency where it matters most: closing a workspace's last tab emits
// tab.closed followed immediately by workspace.closed (findings #8). Both
// cascades overlap, so the done agent's attention-latency interval and count
// decrement must happen exactly once.
func TestTabClosedThenWorkspaceClosedCascadesOnce(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	tr.ApplyWorkspaceCreated(events.NormalizedEvent{WorkspaceID: "w1"})
	tr.ApplyTabCreated(events.NormalizedEvent{TabID: "w1:t1", WorkspaceID: "w1"})
	tr.ApplyPaneCreated(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1"})
	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusDone})
	cur = cur.Add(4 * time.Minute)

	tr.ApplyTabClosed(events.NormalizedEvent{TabID: "w1:t1", WorkspaceID: "w1"})
	tr.ApplyWorkspaceClosed(events.NormalizedEvent{WorkspaceID: "w1"})

	select {
	case al := <-tr.AttentionLatency():
		if al.Duration != 4*time.Minute {
			t.Errorf("attention latency = %s, want 4m", al.Duration)
		}
	default:
		t.Fatal("no attention latency emitted across the double cascade")
	}
	select {
	case al := <-tr.AttentionLatency():
		t.Errorf("second cascade emitted a duplicate sample: %+v", al)
	default:
	}

	c := tr.Counts()
	if got := c.Global[snapshot.AgentStatusDone]; got != 0 {
		t.Errorf("done count = %d, want 0", got)
	}
	if _, ok := c.Workspace["w1"]; ok {
		t.Error("w1 count bucket survived a double cascade")
	}

	tr.mu.RLock()
	ag, present := tr.agents["w1:p1"]
	tr.mu.RUnlock()
	if !present || ag.ClosedAt.IsZero() {
		t.Fatal("agent of double-cascaded pane not retained in grace window")
	}

	cur = cur.Add(16 * time.Second)
	tr.EvictExpired()
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	if _, ok := tr.agents["w1:p1"]; ok {
		t.Error("agent still tracked after eviction")
	}
	if _, ok := tr.panes["w1:p1"]; ok {
		t.Error("pane still tracked after eviction")
	}
	if _, ok := tr.tabs["w1:t1"]; ok {
		t.Error("tab still tracked after eviction")
	}
}

// TestDiffSkipsClosedEntitiesInGrace verifies closed panes/tabs inside their
// retention window don't surface as drift against a stale snapshot — they are
// intentionally absent from live tracking until eviction (2.7).
func TestDiffSkipsClosedEntitiesInGrace(t *testing.T) {
	snap := snapshot.Snapshot{
		Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}},
		Tabs:       []snapshot.Tab{{TabID: "w1:t1", WorkspaceID: "w1"}},
		Panes:      workingPanes(""),
	}
	tr := NewTracker()
	tr.ApplySnapshot(snap)

	tr.ApplyPaneClosed(events.NormalizedEvent{PaneID: "w1:p1"})
	tr.ApplyTabClosed(events.NormalizedEvent{TabID: "w1:t1", WorkspaceID: "w1"})

	if report := tr.Diff(snap); report.Drifted() {
		t.Errorf("closed entities in grace window drifted: %+v", report.Drifts)
	}
}

// TestClosedEntitiesClearedByRebaseline verifies a re-baseline wipes retained
// close-grace records immediately: the snapshot is ground truth and never
// contains a genuinely closed entity, so there is no need to wait for the
// grace window.
func TestClosedEntitiesClearedByRebaseline(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	agent := "codex"
	tr.ApplySnapshot(snapshot.Snapshot{
		Panes: []snapshot.Pane{{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1", AgentStatus: snapshot.AgentStatusDone, Agent: &agent}},
	})
	tr.ApplyPaneClosed(events.NormalizedEvent{PaneID: "w1:p1"})
	<-tr.AttentionLatency() // drain close flush

	tr.ApplySnapshot(snapshot.Snapshot{Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}}})

	tr.mu.RLock()
	defer tr.mu.RUnlock()
	if _, ok := tr.panes["w1:p1"]; ok {
		t.Error("closed pane survived a re-baseline that omits it")
	}
	if _, ok := tr.agents["w1:p1"]; ok {
		t.Error("closed agent survived a re-baseline that omits it")
	}
}

// TestIdempotentApplySameEventSequence verifies that applying the full event
// lifecycle twice produces identical tracked state — the core 2.5 contract.
// Events carry no sequence number (0.4), so idempotent application is the
// only dedup mechanism.
func TestIdempotentApplySameEventSequence(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	seq := []events.NormalizedEvent{
		{WorkspaceID: "w1"},
		{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1"},
		{PaneID: "w1:p1", Agent: "codex"},
		{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusWorking},
	}

	// First pass: build up state.
	tr.ApplyWorkspaceCreated(seq[0])
	tr.ApplyPaneCreated(seq[1])
	tr.ApplyAgentDetected(seq[2])
	tr.ApplyAgentStatusChanged(seq[3])
	cur = cur.Add(2 * time.Minute)
	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusBlocked})

	tr.mu.RLock()
	snap1 := captureTrackerState(t, tr)
	tr.mu.RUnlock()

	// Second pass: apply the same events again (simulate a duplicate delivery).
	tr.ApplyWorkspaceCreated(seq[0])
	tr.ApplyPaneCreated(seq[1])
	tr.ApplyAgentDetected(seq[2])
	// Duplicate status transitions — must be no-ops.
	tr.ApplyAgentStatusChanged(seq[3])
	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusBlocked})

	tr.mu.RLock()
	snap2 := captureTrackerState(t, tr)
	tr.mu.RUnlock()

	drainAttentionLatency(t, tr)

	if !reflect.DeepEqual(snap1, snap2) {
		t.Errorf("state diverged after duplicate event sequence:\n  first:  %+v\n  second: %+v", snap1, snap2)
	}
}

// capturedState is a test helper that captures tracked state as a comparable struct.
type capturedState struct {
	Workspaces map[string]WorkspaceState
	Panes      map[string]PaneState
	Tabs       map[string]TabState
	Agents     map[string]AgentState
}

func captureTrackerState(t *testing.T, tr *Tracker) capturedState {
	t.Helper()
	ws := cloneWorkspaces(tr.workspaces)
	for k, v := range ws {
		v.UpdatedAt = time.Time{}
		ws[k] = v
	}
	panes := clonePanes(tr.panes)
	for k, v := range panes {
		v.UpdatedAt = time.Time{}
		panes[k] = v
	}
	tabs := cloneTabs(tr.tabs)
	for k, v := range tabs {
		v.UpdatedAt = time.Time{}
		tabs[k] = v
	}
	return capturedState{
		Workspaces: ws,
		Panes:      panes,
		Tabs:       tabs,
		Agents:     cloneAgents(tr.agents),
	}
}

func cloneWorkspaces(m map[string]WorkspaceState) map[string]WorkspaceState {
	out := make(map[string]WorkspaceState, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func clonePanes(m map[string]PaneState) map[string]PaneState {
	out := make(map[string]PaneState, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneTabs(m map[string]TabState) map[string]TabState {
	out := make(map[string]TabState, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneAgents(m map[string]AgentState) map[string]AgentState {
	out := make(map[string]AgentState, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func drainAttentionLatency(t *testing.T, tr *Tracker) {
	t.Helper()
	for {
		select {
		case <-tr.AttentionLatency():
		default:
			return
		}
	}
}

// TestApplyPaneCreatedDuplicateIsBenign verifies that a duplicate pane.created
// event overwrites the same map key with harmless results — the only field
// that changes is UpdatedAt, which is expected.
func TestApplyPaneCreatedDuplicateIsBenign(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	ev := events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1"}
	tr.ApplyPaneCreated(ev)

	tr.mu.RLock()
	first := tr.panes["w1:p1"]
	firstTab := tr.tabs["w1:t1"]
	tr.mu.RUnlock()

	cur = cur.Add(time.Minute)
	tr.ApplyPaneCreated(ev) // duplicate

	tr.mu.RLock()
	second := tr.panes["w1:p1"]
	secondTab := tr.tabs["w1:t1"]
	tr.mu.RUnlock()

	if first.PaneID != second.PaneID || first.WorkspaceID != second.WorkspaceID || first.TabID != second.TabID {
		t.Errorf("pane struct changed on duplicate: first=%+v, second=%+v", first, second)
	}
	if firstTab.TabID != secondTab.TabID || firstTab.WorkspaceID != secondTab.WorkspaceID {
		t.Errorf("tab struct changed on duplicate: first=%+v, second=%+v", firstTab, secondTab)
	}
	if !second.UpdatedAt.After(first.UpdatedAt) {
		t.Error("UpdatedAt not refreshed on duplicate (expected benign update)")
	}
}

func TestApplySnapshotSeedsAndResetsAgents(t *testing.T) {
	agent := "codex"
	tr := NewTracker()
	tr.ApplySnapshot(snapshot.Snapshot{
		Panes: []snapshot.Pane{{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1", AgentStatus: snapshot.AgentStatusWorking, Agent: &agent}},
	})

	tr.mu.RLock()
	ag, ok := tr.agents["w1:p1"]
	tr.mu.RUnlock()
	if !ok {
		t.Fatal("agent w1:p1 not seeded from snapshot")
	}
	if ag.Status != snapshot.AgentStatusWorking || ag.AgentType != "codex" {
		t.Errorf("seeded agent = %+v", ag)
	}
	if ag.StateEnteredAt.IsZero() {
		t.Error("seeded agent has zero StateEnteredAt")
	}

	// A later snapshot with no agents must reset the agents map.
	tr.ApplySnapshot(snapshot.Snapshot{
		Panes: []snapshot.Pane{{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1", AgentStatus: snapshot.AgentStatusIdle}},
	})
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	if _, ok := tr.agents["w1:p1"]; ok {
		t.Error("agents map not reset on snapshot re-apply")
	}
}

func TestApplyWorkspaceClosedCascades(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	tr.ApplyWorkspaceCreated(events.NormalizedEvent{WorkspaceID: "w1"})
	tr.ApplyPaneCreated(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1"})
	tr.ApplyWorkspaceClosed(events.NormalizedEvent{WorkspaceID: "w1"})

	tr.mu.RLock()
	_, wsPresent := tr.workspaces["w1"]
	pane, paneOK := tr.panes["w1:p1"]
	tab, tabOK := tr.tabs["w1:t1"]
	tr.mu.RUnlock()

	if wsPresent {
		t.Error("workspace w1 still tracked after close")
	}
	// The cascade previously deleted immediately; now the entities enter the
	// retention grace window (2.7) so late events are absorbed.
	if !paneOK || pane.ClosedAt.IsZero() {
		t.Error("pane w1:p1 not retained with ClosedAt after workspace close")
	}
	if !tabOK || tab.ClosedAt.IsZero() {
		t.Error("tab w1:t1 not retained with ClosedAt after workspace close")
	}

	cur = cur.Add(16 * time.Second)
	tr.EvictExpired()
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	if _, ok := tr.panes["w1:p1"]; ok {
		t.Error("pane w1:p1 still tracked after eviction")
	}
	if _, ok := tr.tabs["w1:t1"]; ok {
		t.Error("tab w1:t1 still tracked after eviction")
	}
}

func TestApplySnapshotSeedsBaseline(t *testing.T) {
	tr := NewTracker()
	agent := "codex"
	tr.ApplySnapshot(snapshot.Snapshot{
		Workspaces: []snapshot.Workspace{{WorkspaceID: "w1", Label: "herdr"}},
		Tabs:       []snapshot.Tab{{TabID: "w1:t1", WorkspaceID: "w1", Label: "~"}},
		Panes:      []snapshot.Pane{{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1", AgentStatus: snapshot.AgentStatusWorking, Agent: &agent}},
	})

	tr.mu.RLock()
	defer tr.mu.RUnlock()

	ws := tr.workspaces["w1"]
	if ws.Label != "herdr" {
		t.Errorf("workspace label = %q, want herdr", ws.Label)
	}
	pane := tr.panes["w1:p1"]
	if pane.Agent != "codex" || pane.Status != snapshot.AgentStatusWorking {
		t.Errorf("pane = %+v", pane)
	}
	if tab := tr.tabs["w1:t1"]; tab.Label != "~" {
		t.Errorf("tab label = %q, want ~", tab.Label)
	}
}

func TestDiffNoDrift(t *testing.T) {
	snap := snapshot.Snapshot{
		Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}},
		Tabs:       []snapshot.Tab{{TabID: "w1:t1", WorkspaceID: "w1"}},
		Panes:      workingPanes(""),
	}

	tr := NewTracker()
	tr.ApplyWorkspaceCreated(events.NormalizedEvent{WorkspaceID: "w1"})
	tr.ApplyPaneCreated(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1"})
	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "", NewState: snapshot.AgentStatusWorking})

	if report := tr.Diff(snap); report.Drifted() {
		t.Fatalf("unexpected drift: %+v", report.Drifts)
	}
}

func TestDiffDetectsStatusChange(t *testing.T) {
	snap := snapshot.Snapshot{
		Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}},
		Panes:      workingPanes(""),
	}

	tr := NewTracker()
	tr.ApplySnapshot(snap)
	// Event stream says blocked; snapshot says working -> drift.
	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", NewState: snapshot.AgentStatusBlocked})

	report := tr.Diff(snap)
	if !report.Drifted() {
		t.Fatal("expected drift after status change not mirrored in snapshot")
	}
	if len(report.Drifts) != 1 || report.Drifts[0].Kind != "pane" || report.Drifts[0].ID != "w1:p1" {
		t.Errorf("drifts = %+v", report.Drifts)
	}
}

func TestDiffDetectsAddedPane(t *testing.T) {
	tr := NewTracker()
	tr.ApplySnapshot(snapshot.Snapshot{
		Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}},
		Panes:      []snapshot.Pane{{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1", AgentStatus: snapshot.AgentStatusIdle}},
	})

	// Snapshot now contains a pane the tracker has never seen.
	snap := snapshot.Snapshot{
		Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}},
		Panes: []snapshot.Pane{
			{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1", AgentStatus: snapshot.AgentStatusIdle},
			{PaneID: "w1:p2", WorkspaceID: "w1", TabID: "w1:t2", AgentStatus: snapshot.AgentStatusWorking},
		},
	}

	report := tr.Diff(snap)
	if !report.Drifted() {
		t.Fatal("expected drift for added pane")
	}
	if report.Drifts[0].Kind != "pane" || report.Drifts[0].ID != "w1:p2" {
		t.Errorf("drifts = %+v", report.Drifts)
	}
}

func TestDiffDetectsRemovedPane(t *testing.T) {
	snap := snapshot.Snapshot{
		Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}},
		Panes:      workingPanes(""),
	}

	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }
	tr.ApplySnapshot(snap)
	// Pane closed in the stream, but the snapshot it was built from still
	// lists it (2.7). Within the retention grace window that is a known
	// transitional state, not a drift: the pane is genuinely closed, so
	// nothing is missing on the stream's side and no resubscribe is warranted.
	tr.ApplyPaneClosed(events.NormalizedEvent{PaneID: "w1:p1"})

	if report := tr.Diff(snap); report.Drifted() {
		t.Fatalf("closed pane in grace window must not drift: %+v", report.Drifts)
	}

	// Once the grace window elapses the pane is evicted, and a snapshot that
	// still lists it is a genuine removed-pane gap -> drift.
	cur = cur.Add(16 * time.Second)
	tr.EvictExpired()

	report := tr.Diff(snap)
	if !report.Drifted() {
		t.Fatal("expected drift for removed pane after eviction")
	}
	if report.Drifts[0].Kind != "pane" || report.Drifts[0].ID != "w1:p1" {
		t.Errorf("drifts = %+v", report.Drifts)
	}
}

func TestDiffDetectsAddedWorkspace(t *testing.T) {
	tr := NewTracker()
	tr.ApplySnapshot(snapshot.Snapshot{Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}}})

	snap := snapshot.Snapshot{Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}, {WorkspaceID: "w2"}}}

	report := tr.Diff(snap)
	if !report.Drifted() {
		t.Fatal("expected drift for added workspace")
	}
	if report.Drifts[0].Kind != "workspace" || report.Drifts[0].ID != "w2" {
		t.Errorf("drifts = %+v", report.Drifts)
	}
}

func TestDiffDetectsTabMembership(t *testing.T) {
	base := func(tabs []snapshot.Tab) snapshot.Snapshot {
		return snapshot.Snapshot{
			Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}},
			Tabs:       tabs,
			Panes:      workingPanes(""),
		}
	}

	// Snapshot gained a tab the tracker never saw -> drift.
	tr := NewTracker()
	tr.ApplySnapshot(base([]snapshot.Tab{{TabID: "w1:t1", WorkspaceID: "w1"}}))
	report := tr.Diff(base([]snapshot.Tab{{TabID: "w1:t1", WorkspaceID: "w1"}, {TabID: "w1:t2", WorkspaceID: "w1"}}))
	if !report.Drifted() {
		t.Fatal("expected drift for added tab")
	}
	if report.Drifts[0].Kind != "tab" || report.Drifts[0].ID != "w1:t2" {
		t.Errorf("drifts = %+v", report.Drifts)
	}

	// Tracker still holds a tab the snapshot dropped (e.g. missed tab.closed)
	// -> drift.
	tr = NewTracker()
	tr.ApplySnapshot(base([]snapshot.Tab{{TabID: "w1:t1", WorkspaceID: "w1"}, {TabID: "w1:t2", WorkspaceID: "w1"}}))
	report = tr.Diff(base([]snapshot.Tab{{TabID: "w1:t1", WorkspaceID: "w1"}}))
	if !report.Drifted() {
		t.Fatal("expected drift for removed tab")
	}
	if report.Drifts[0].Kind != "tab" || report.Drifts[0].ID != "w1:t2" {
		t.Errorf("drifts = %+v", report.Drifts)
	}
}

func TestDiffIgnoresTabMetadataChanges(t *testing.T) {
	snap := snapshot.Snapshot{
		Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}},
		Tabs:       []snapshot.Tab{{TabID: "w1:t1", WorkspaceID: "w1", Label: "renamed"}},
		Panes:      workingPanes(""),
	}

	tr := NewTracker()
	tr.ApplySnapshot(snapshot.Snapshot{
		Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}},
		Tabs:       []snapshot.Tab{{TabID: "w1:t1", WorkspaceID: "w1", Label: "old"}},
		Panes:      workingPanes(""),
	})

	if report := tr.Diff(snap); report.Drifted() {
		t.Fatalf("tab label changes must not drift: %+v", report.Drifts)
	}
}

func TestApplyTabLifecycleTracksState(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	tr.ApplyTabCreated(events.NormalizedEvent{TabID: "w1:t1", WorkspaceID: "w1", Label: "herdr-observr"})

	tr.mu.RLock()
	tab := tr.tabs["w1:t1"]
	tr.mu.RUnlock()
	if tab.Label != "herdr-observr" || tab.WorkspaceID != "w1" {
		t.Fatalf("tab after created = %+v", tab)
	}

	tr.ApplyTabRenamed(events.NormalizedEvent{TabID: "w1:t1", Label: "agents"})
	tr.mu.RLock()
	tab = tr.tabs["w1:t1"]
	tr.mu.RUnlock()
	if tab.Label != "agents" {
		t.Errorf("tab label after rename = %q, want agents", tab.Label)
	}

	tr.ApplyTabClosed(events.NormalizedEvent{TabID: "w1:t1", WorkspaceID: "w1"})
	tr.mu.RLock()
	tab, ok := tr.tabs["w1:t1"]
	tr.mu.RUnlock()
	if !ok {
		t.Fatal("tab evicted before its grace window")
	}
	if tab.ClosedAt.IsZero() {
		t.Error("tab not marked closed after tab.closed")
	}

	cur = cur.Add(16 * time.Second)
	tr.EvictExpired()
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	if _, still := tr.tabs["w1:t1"]; still {
		t.Error("tab still tracked after eviction")
	}
}

func TestApplyTabClosedCascadesPanesAndAgents(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }

	tr.ApplyTabCreated(events.NormalizedEvent{TabID: "w1:t1", WorkspaceID: "w1"})
	tr.ApplyTabCreated(events.NormalizedEvent{TabID: "w1:t2", WorkspaceID: "w1"})
	tr.ApplyPaneCreated(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1"})
	tr.ApplyPaneCreated(events.NormalizedEvent{PaneID: "w1:p2", WorkspaceID: "w1", TabID: "w1:t1"})
	tr.ApplyPaneCreated(events.NormalizedEvent{PaneID: "w1:p3", WorkspaceID: "w1", TabID: "w1:t2"})
	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusDone})
	cur = cur.Add(3 * time.Minute)
	tr.ApplyAgentStatusChanged(events.NormalizedEvent{PaneID: "w1:p2", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusWorking})

	// Closing t1 emits no pane.closed for its panes (herdr handle_tab_close),
	// so the cascade must remove them and flush the done interval.
	tr.ApplyTabClosed(events.NormalizedEvent{TabID: "w1:t1", WorkspaceID: "w1"})

	select {
	case al := <-tr.AttentionLatency():
		if al.PaneID != "w1:p1" || al.Duration != 3*time.Minute {
			t.Errorf("cascaded attention latency = %+v, want done pane w1:p1 after 3m", al)
		}
	default:
		t.Fatal("no attention latency flushed for done pane in closed tab")
	}

	tr.mu.RLock()
	tab1, t1OK := tr.tabs["w1:t1"]
	if _, ok := tr.tabs["w1:t2"]; !ok {
		t.Error("tab w1:t2 must survive the cascade")
	}
	p1, p1OK := tr.panes["w1:p1"]
	ag1, ag1OK := tr.agents["w1:p1"]
	p2, p2OK := tr.panes["w1:p2"]
	_, ag2OK := tr.agents["w1:p2"]
	if _, ok := tr.panes["w1:p3"]; !ok {
		t.Error("pane w1:p3 (other tab) must survive")
	}
	tr.mu.RUnlock()

	// The cascaded entities enter the retention grace window (2.7): retained
	// with ClosedAt until eviction, not deleted on the spot.
	if !t1OK || tab1.ClosedAt.IsZero() {
		t.Error("tab w1:t1 not retained with ClosedAt after close")
	}
	if !p1OK || p1.ClosedAt.IsZero() {
		t.Error("pane w1:p1 not retained with ClosedAt after close")
	}
	if !ag1OK || ag1.ClosedAt.IsZero() {
		t.Error("agent w1:p1 not retained with ClosedAt after close")
	}
	if !p2OK || p2.ClosedAt.IsZero() {
		t.Error("pane w1:p2 not retained with ClosedAt after close")
	}
	if !ag2OK {
		t.Error("agent w1:p2 not retained after close")
	}

	cur = cur.Add(16 * time.Second)
	tr.EvictExpired()

	tr.mu.RLock()
	defer tr.mu.RUnlock()
	if _, ok := tr.tabs["w1:t1"]; ok {
		t.Error("tab w1:t1 still tracked after eviction")
	}
	if _, ok := tr.tabs["w1:t2"]; !ok {
		t.Error("tab w1:t2 must survive")
	}
	if _, ok := tr.panes["w1:p1"]; ok {
		t.Error("pane w1:p1 still tracked after eviction")
	}
	if _, ok := tr.agents["w1:p1"]; ok {
		t.Error("agent w1:p1 still tracked after eviction")
	}
	if _, ok := tr.panes["w1:p2"]; ok {
		t.Error("pane w1:p2 still tracked after eviction")
	}
	if _, ok := tr.agents["w1:p2"]; ok {
		t.Error("agent w1:p2 still tracked after eviction")
	}
	if _, ok := tr.panes["w1:p3"]; !ok {
		t.Error("pane w1:p3 (other tab) must survive")
	}
}

// startSnapshotStub runs a unix socket server that accepts connections, reads
// one session.snapshot request, and answers with the JSON produced by respond.
// Multiple connections are supported (each RPC opens its own).
func startSnapshotStub(t *testing.T, respond func() string) string {
	t.Helper()
	sockPath := filepath.Join(t.TempDir(), "herdr-tracker-test.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				line, err := bufio.NewReader(conn).ReadBytes('\n')
				if err != nil {
					return
				}
				var req struct {
					Method string `json:"method"`
				}
				if err := json.Unmarshal(line, &req); err != nil {
					return
				}
				if req.Method != "session.snapshot" {
					fmt.Fprintf(conn, `{"id":"herdr-observr","error":{"code":"MethodNotFound","message":"no such method: %s"}}`+"\n", req.Method)
					return
				}
				body := respond()
				if body == "" {
					conn.Write([]byte("this is not json\n"))
					return
				}
				fmt.Fprintf(conn, `{"id":"herdr-observr","result":%s}`+"\n", body)
			}(conn)
		}
	}()

	return sockPath
}

func TestRunCallsOnDrift(t *testing.T) {
	// Snapshot says the pane is blocked; event-fed tracker says working.
	driftBody := `{"type":"session_snapshot","snapshot":{"workspaces":[{"workspace_id":"w1"}],"panes":[{"pane_id":"w1:p1","workspace_id":"w1","tab_id":"w1:t1","agent_status":"blocked"}]}}`
	sock := startSnapshotStub(t, func() string { return driftBody })
	t.Setenv("HERDR_SOCKET_PATH", sock)

	tr := NewTracker()
	tr.ApplySnapshot(snapshot.Snapshot{Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}}, Panes: workingPanes("")})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fired := make(chan struct{}, 1)
	go tr.Run(ctx, 20*time.Millisecond, func(DiffReport) { fired <- struct{}{} })

	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatal("onReport not called despite drift")
	}
}

func TestRunSilentWhenInSync(t *testing.T) {
	syncBody := `{"type":"session_snapshot","snapshot":{"workspaces":[{"workspace_id":"w1"}],"panes":[{"pane_id":"w1:p1","workspace_id":"w1","tab_id":"w1:t1","agent_status":"working"}]}}`
	sock := startSnapshotStub(t, func() string { return syncBody })
	t.Setenv("HERDR_SOCKET_PATH", sock)

	tr := NewTracker()
	tr.ApplySnapshot(snapshot.Snapshot{Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}}, Panes: workingPanes("")})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	called := make(chan struct{}, 1)
	go tr.Run(ctx, 20*time.Millisecond, func(DiffReport) { called <- struct{}{} })

	select {
	case <-called:
		t.Fatal("onReport called despite matching state")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestRunSurvivesFetchError(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	body := `{"type":"session_snapshot","snapshot":{"workspaces":[{"workspace_id":"w1"}],"panes":[{"pane_id":"w1:p1","workspace_id":"w1","tab_id":"w1:t1","agent_status":"blocked"}]}}`
	sock := startSnapshotStub(t, func() string {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls <= 2 {
			return "" // malformed response -> fetch error
		}
		return body
	})
	t.Setenv("HERDR_SOCKET_PATH", sock)

	tr := NewTracker()
	tr.ApplySnapshot(snapshot.Snapshot{Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}}, Panes: workingPanes("")})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fired := make(chan struct{}, 1)
	go tr.Run(ctx, 20*time.Millisecond, func(DiffReport) { fired <- struct{}{} })

	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatal("loop did not survive fetch errors and fire on a later good tick")
	}
}

func TestRunsStopsOnCancel(t *testing.T) {
	sock := startSnapshotStub(t, func() string {
		return `{"type":"session_snapshot","snapshot":{"workspaces":[{"workspace_id":"w1"}],"panes":[{"pane_id":"w1:p1","workspace_id":"w1","tab_id":"w1:t1","agent_status":"working"}]}}`
	})
	t.Setenv("HERDR_SOCKET_PATH", sock)

	tr := NewTracker()
	tr.ApplySnapshot(snapshot.Snapshot{Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}}, Panes: workingPanes("")})

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		tr.Run(ctx, 20*time.Millisecond, func(DiffReport) {})
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}
}

package tracker

import (
	"testing"
	"time"

	"github.com/nabutabu/herdr-scribe/internal/events"
	"github.com/nabutabu/herdr-scribe/internal/snapshot"
)

// statusChange builds the minimal event shape a status transition carries on
// the wire (agent, agent_status, pane_id, workspace_id).
func statusChange(paneID, wsID, agent string, s snapshot.AgentStatus) events.NormalizedEvent {
	return events.NormalizedEvent{PaneID: paneID, WorkspaceID: wsID, Agent: agent, NewState: s}
}

// TestCountsTrackTransitions verifies the incremental counter updates across a
// multi-agent, multi-workspace transition sequence (2.6): global and
// per-workspace counts stay in step at every step.
func TestCountsTrackTransitions(t *testing.T) {
	tr := NewTracker()
	change := func(pane, ws, agent string, s snapshot.AgentStatus) {
		tr.ApplyAgentStatusChanged(statusChange(pane, ws, agent, s))
	}

	change("w1:p1", "w1", "codex", snapshot.AgentStatusWorking)
	change("w1:p2", "w1", "codex", snapshot.AgentStatusBlocked)
	change("w2:p1", "w2", "codex", snapshot.AgentStatusWorking)

	c := tr.Counts()
	if got := c.Global[snapshot.AgentStatusWorking]; got != 2 {
		t.Errorf("global working = %d, want 2", got)
	}
	if got := c.Global[snapshot.AgentStatusBlocked]; got != 1 {
		t.Errorf("global blocked = %d, want 1", got)
	}
	if got := c.Workspace["w1"][snapshot.AgentStatusWorking]; got != 1 {
		t.Errorf("w1 working = %d, want 1", got)
	}
	if got := c.Workspace["w1"][snapshot.AgentStatusBlocked]; got != 1 {
		t.Errorf("w1 blocked = %d, want 1", got)
	}
	if got := c.Workspace["w2"][snapshot.AgentStatusWorking]; got != 1 {
		t.Errorf("w2 working = %d, want 1", got)
	}

	change("w1:p1", "w1", "codex", snapshot.AgentStatusBlocked)
	c = tr.Counts()
	if got := c.Global[snapshot.AgentStatusWorking]; got != 1 {
		t.Errorf("global working = %d, want 1", got)
	}
	if got := c.Global[snapshot.AgentStatusBlocked]; got != 2 {
		t.Errorf("global blocked = %d, want 2", got)
	}
	if got := c.Workspace["w1"][snapshot.AgentStatusWorking]; got != 0 {
		t.Errorf("w1 working = %d, want 0", got)
	}
	if got := c.Workspace["w1"][snapshot.AgentStatusBlocked]; got != 2 {
		t.Errorf("w1 blocked = %d, want 2", got)
	}

	change("w1:p1", "w1", "codex", snapshot.AgentStatusWorking)
	c = tr.Counts()
	if got := c.Global[snapshot.AgentStatusWorking]; got != 2 {
		t.Errorf("global working = %d, want 2", got)
	}
	if got := c.Global[snapshot.AgentStatusBlocked]; got != 1 {
		t.Errorf("global blocked = %d, want 1", got)
	}
	if got := c.Workspace["w1"][snapshot.AgentStatusWorking]; got != 1 {
		t.Errorf("w1 working = %d, want 1", got)
	}
	if got := c.Workspace["w1"][snapshot.AgentStatusBlocked]; got != 1 {
		t.Errorf("w1 blocked = %d, want 1", got)
	}
}

// TestCountsDuplicateSameStateNoDoubleCount verifies idempotent application
// (2.5): a repeated status event is a no-op and must not inflate the counts.
func TestCountsDuplicateSameStateNoDoubleCount(t *testing.T) {
	tr := NewTracker()
	tr.ApplyAgentStatusChanged(statusChange("w1:p1", "w1", "codex", snapshot.AgentStatusWorking))
	tr.ApplyAgentStatusChanged(statusChange("w1:p1", "w1", "codex", snapshot.AgentStatusWorking))
	tr.ApplyAgentStatusChanged(statusChange("w1:p1", "w1", "codex", snapshot.AgentStatusWorking))

	c := tr.Counts()
	if got := c.Global[snapshot.AgentStatusWorking]; got != 1 {
		t.Errorf("global working = %d, want 1 (duplicate events must not double-count)", got)
	}
	if got := c.Workspace["w1"][snapshot.AgentStatusWorking]; got != 1 {
		t.Errorf("w1 working = %d, want 1", got)
	}
}

// TestCountsSeenFlipAdjusts verifies the done->idle seen-flip (2.4) moves one
// agent out of done and into idle in both the global and per-workspace counts.
func TestCountsSeenFlipAdjusts(t *testing.T) {
	tr := NewTracker()
	tr.ApplyAgentStatusChanged(statusChange("w1:p1", "w1", "codex", snapshot.AgentStatusDone))
	tr.ApplyAgentStatusChanged(statusChange("w1:p2", "w1", "codex", snapshot.AgentStatusWorking))

	tr.ApplySeenFlip("w1:p1")

	c := tr.Counts()
	if got := c.Global[snapshot.AgentStatusDone]; got != 0 {
		t.Errorf("global done = %d, want 0", got)
	}
	if got := c.Global[snapshot.AgentStatusIdle]; got != 1 {
		t.Errorf("global idle = %d, want 1", got)
	}
	if got := c.Global[snapshot.AgentStatusWorking]; got != 1 {
		t.Errorf("global working = %d, want 1", got)
	}
	if got := c.Workspace["w1"][snapshot.AgentStatusDone]; got != 0 {
		t.Errorf("w1 done = %d, want 0", got)
	}
	if got := c.Workspace["w1"][snapshot.AgentStatusIdle]; got != 1 {
		t.Errorf("w1 idle = %d, want 1", got)
	}

	// A second seen-flip (stale reconcile tick) must not move counts again.
	tr.ApplySeenFlip("w1:p1")
	c = tr.Counts()
	if got := c.Global[snapshot.AgentStatusIdle]; got != 1 {
		t.Errorf("global idle after stale seen-flip = %d, want 1", got)
	}
}

// TestCountsPaneClosedAdjusts verifies a closed pane drops its agent from the
// counts, whatever state it was in.
func TestCountsPaneClosedAdjusts(t *testing.T) {
	tr := NewTracker()
	tr.ApplyAgentStatusChanged(statusChange("w1:p1", "w1", "codex", snapshot.AgentStatusBlocked))
	tr.ApplyAgentStatusChanged(statusChange("w1:p2", "w1", "codex", snapshot.AgentStatusWorking))

	tr.ApplyPaneClosed(events.NormalizedEvent{PaneID: "w1:p1"})

	c := tr.Counts()
	if got := c.Global[snapshot.AgentStatusBlocked]; got != 0 {
		t.Errorf("global blocked = %d, want 0", got)
	}
	if got := c.Global[snapshot.AgentStatusWorking]; got != 1 {
		t.Errorf("global working = %d, want 1", got)
	}
	if got := c.Workspace["w1"][snapshot.AgentStatusBlocked]; got != 0 {
		t.Errorf("w1 blocked = %d, want 0", got)
	}
	if got := c.Workspace["w1"][snapshot.AgentStatusWorking]; got != 1 {
		t.Errorf("w1 working = %d, want 1", got)
	}
}

// TestCountsWorkspaceClosedCleansAgentsAndCounts verifies the cascade (2.6
// bundled fix): closing a workspace removes its agents and their counts, while
// leaving other workspaces untouched.
func TestCountsWorkspaceClosedCleansAgentsAndCounts(t *testing.T) {
	cur := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	tr := NewTracker()
	tr.now = func() time.Time { return cur }
	tr.ApplyAgentStatusChanged(statusChange("w1:p1", "w1", "codex", snapshot.AgentStatusWorking))
	tr.ApplyAgentStatusChanged(statusChange("w1:p2", "w1", "codex", snapshot.AgentStatusBlocked))
	tr.ApplyAgentStatusChanged(statusChange("w2:p1", "w2", "codex", snapshot.AgentStatusWorking))

	tr.ApplyWorkspaceClosed(events.NormalizedEvent{WorkspaceID: "w1"})

	c := tr.Counts()
	if got := c.Global[snapshot.AgentStatusWorking]; got != 1 {
		t.Errorf("global working = %d, want 1 (only w2:p1 remains)", got)
	}
	if got := c.Global[snapshot.AgentStatusBlocked]; got != 0 {
		t.Errorf("global blocked = %d, want 0", got)
	}
	if _, ok := c.Workspace["w1"]; ok {
		t.Error("w1 counts must be dropped entirely after workspace close")
	}
	if got := c.Workspace["w2"][snapshot.AgentStatusWorking]; got != 1 {
		t.Errorf("w2 working = %d, want 1", got)
	}

	// Counts are dropped at close even though the agent records are retained
	// in their grace window (2.7), so they can't leak into the gauges.
	tr.mu.RLock()
	ag1, w1p1Present := tr.agents["w1:p1"]
	ag2, w1p2Present := tr.agents["w1:p2"]
	tr.mu.RUnlock()
	if !w1p1Present || ag1.ClosedAt.IsZero() {
		t.Error("w1:p1 not retained with ClosedAt during grace window")
	}
	if !w1p2Present || ag2.ClosedAt.IsZero() {
		t.Error("w1:p2 not retained with ClosedAt during grace window")
	}

	cur = cur.Add(16 * time.Second)
	tr.EvictExpired()
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	if _, still := tr.agents["w1:p1"]; still {
		t.Error("w1:p1 still tracked after eviction")
	}
	if _, still := tr.agents["w1:p2"]; still {
		t.Error("w1:p2 still tracked after eviction")
	}
}

// TestCountsApplySnapshotRecomputeFromScratch verifies a re-baseline rebuilds
// the counters from the authoritative snapshot, wholesale — including dropping
// agents the events had tracked that the snapshot no longer reports.
func TestCountsApplySnapshotRecomputeFromScratch(t *testing.T) {
	tr := NewTracker()
	agent := "codex"
	tr.ApplyAgentStatusChanged(statusChange("w1:p1", "w1", agent, snapshot.AgentStatusBlocked))

	tr.ApplySnapshot(snapshot.Snapshot{
		Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}, {WorkspaceID: "w2"}},
		Tabs: []snapshot.Tab{
			{TabID: "w1:t1", WorkspaceID: "w1"},
			{TabID: "w2:t1", WorkspaceID: "w2"},
		},
		Panes: []snapshot.Pane{
			{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1", AgentStatus: snapshot.AgentStatusWorking, Agent: &agent},
			{PaneID: "w1:p2", WorkspaceID: "w1", TabID: "w1:t1", AgentStatus: snapshot.AgentStatusBlocked, Agent: &agent},
			{PaneID: "w2:p1", WorkspaceID: "w2", TabID: "w2:t1", AgentStatus: snapshot.AgentStatusWorking, Agent: &agent},
		},
	})

	c := tr.Counts()
	if got := c.Global[snapshot.AgentStatusWorking]; got != 2 {
		t.Errorf("global working = %d, want 2", got)
	}
	if got := c.Global[snapshot.AgentStatusBlocked]; got != 1 {
		t.Errorf("global blocked = %d, want 1", got)
	}
	if got := c.Workspace["w1"][snapshot.AgentStatusWorking]; got != 1 {
		t.Errorf("w1 working = %d, want 1", got)
	}
	if got := c.Workspace["w1"][snapshot.AgentStatusBlocked]; got != 1 {
		t.Errorf("w1 blocked = %d, want 1", got)
	}
	if got := c.Workspace["w2"][snapshot.AgentStatusWorking]; got != 1 {
		t.Errorf("w2 working = %d, want 1", got)
	}

	// A follow-up re-baseline that drops w1:p2 must recompute, not accumulate.
	tr.ApplySnapshot(snapshot.Snapshot{
		Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}, {WorkspaceID: "w2"}},
		Panes: []snapshot.Pane{
			{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1", AgentStatus: snapshot.AgentStatusWorking, Agent: &agent},
			{PaneID: "w2:p1", WorkspaceID: "w2", TabID: "w2:t1", AgentStatus: snapshot.AgentStatusWorking, Agent: &agent},
		},
	})
	c = tr.Counts()
	if got := c.Global[snapshot.AgentStatusWorking]; got != 2 {
		t.Errorf("global working after re-baseline = %d, want 2", got)
	}
	if got := c.Global[snapshot.AgentStatusBlocked]; got != 0 {
		t.Errorf("global blocked after re-baseline = %d, want 0", got)
	}
}

// TestCountsReturnsCopies verifies Counts() hands out deep copies: mutating the
// returned structure must not corrupt the tracker's internal counters.
func TestCountsReturnsCopies(t *testing.T) {
	tr := NewTracker()
	tr.ApplyAgentStatusChanged(statusChange("w1:p1", "w1", "codex", snapshot.AgentStatusWorking))

	c := tr.Counts()
	c.Global[snapshot.AgentStatusWorking] = 99
	c.Workspace["w1"][snapshot.AgentStatusWorking] = 99

	got := tr.Counts()
	if got.Global[snapshot.AgentStatusWorking] != 1 {
		t.Errorf("global working after mutation = %d, want 1", got.Global[snapshot.AgentStatusWorking])
	}
	if got.Workspace["w1"][snapshot.AgentStatusWorking] != 1 {
		t.Errorf("w1 working after mutation = %d, want 1", got.Workspace["w1"][snapshot.AgentStatusWorking])
	}
}

// TestCountsAgentMovesWorkspace covers the stale-workspace edge in
// ApplyAgentStatusChanged: the tracked agent still claims its old workspace
// when the transition event arrives with a new one. The old bucket must be
// decremented, the new one incremented.
func TestCountsAgentMovesWorkspace(t *testing.T) {
	tr := NewTracker()
	tr.ApplyAgentStatusChanged(statusChange("w1:p1", "w1", "codex", snapshot.AgentStatusWorking))
	tr.ApplyAgentStatusChanged(statusChange("w1:p1", "w2", "codex", snapshot.AgentStatusBlocked))

	c := tr.Counts()
	if got := c.Global[snapshot.AgentStatusWorking]; got != 0 {
		t.Errorf("global working = %d, want 0", got)
	}
	if got := c.Global[snapshot.AgentStatusBlocked]; got != 1 {
		t.Errorf("global blocked = %d, want 1", got)
	}
	if got := c.Workspace["w1"][snapshot.AgentStatusWorking]; got != 0 {
		t.Errorf("w1 working = %d, want 0 (old workspace must be decremented)", got)
	}
	if got := c.Workspace["w2"][snapshot.AgentStatusBlocked]; got != 1 {
		t.Errorf("w2 blocked = %d, want 1", got)
	}
}

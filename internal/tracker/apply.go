package tracker

import (
	"log/slog"
	"time"

	"github.com/nabutabu/herdr-observr/internal/events"
	"github.com/nabutabu/herdr-observr/internal/snapshot"
)

// ApplyWorkspaceCreated folds a workspace.created event into the tracked state.
// Idempotent (2.5): a duplicate event overwrites the same map key with
// identical data (aside from UpdatedAt, which is benign).
func (t *Tracker) ApplyWorkspaceCreated(ev events.NormalizedEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.workspaces[ev.WorkspaceID] = WorkspaceState{WorkspaceID: ev.WorkspaceID, UpdatedAt: t.now()}
}

// ApplyWorkspaceClosed closes a workspace and cascades its tabs, panes, and
// agents — their own close events may not arrive. Each entity entered the
// retention grace window via the close helpers instead of being dropped
// immediately (2.7): a late `done` agent's attention-latency interval is
// flushed exactly once (a tab.closed that already cascaded this workspace
// left its entities ClosedAt, so the cascade is idempotent). The workspace
// record itself is deleted outright — nothing late can arrive for a
// workspace id standing alone.
func (t *Tracker) ApplyWorkspaceClosed(ev events.NormalizedEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	delete(t.workspaces, ev.WorkspaceID)
	for id := range t.tabs {
		if t.tabs[id].WorkspaceID == ev.WorkspaceID {
			t.closeTabLocked(id, now)
		}
	}
	for id := range t.panes {
		if t.panes[id].WorkspaceID == ev.WorkspaceID {
			t.closePaneLocked(id, now)
		}
	}
	for id := range t.agents {
		if t.agents[id].WorkspaceID == ev.WorkspaceID {
			t.closeAgentLocked(id, now)
		}
	}
}

// ApplyTabCreated records a tab. tab.created is pushed before the root
// pane.created (herdr src/app/creation.rs), so in the normal flow the tab
// exists before panes seed it; this being idempotent, ordering between a
// later duplicate and a pane-seeded tab is also harmless.
// Idempotent (2.5): duplicate event overwrites the same map key with
// identical data (aside from UpdatedAt, which is benign).
func (t *Tracker) ApplyTabCreated(ev events.NormalizedEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	tab := t.tabs[ev.TabID]
	tab.TabID = ev.TabID
	tab.WorkspaceID = ev.WorkspaceID
	tab.Label = ev.Label
	tab.UpdatedAt = now
	t.tabs[ev.TabID] = tab
}

// ApplyTabClosed closes a tab and cascades its panes and agents. Herdr emits
// no pane.closed for the panes of a closed tab — closing a tab destroys its
// panes silently (herdr src/app/api/tabs.rs handle_tab_close), so without this
// cascade the tab's panes/agents would linger (and leak their state counts).
// Closed entities enter the retention grace window (2.7). Done agents get
// their attention-latency interval flushed, mirroring ApplyPaneClosed. When
// the closed tab was the workspace's last, Herdr follows up with
// workspace.closed, whose cascade is idempotent against this one: the close
// helpers no-op on already-closed entities, so exactly one attention-latency
// flush and one count decrement happen per agent.
func (t *Tracker) ApplyTabClosed(ev events.NormalizedEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	for id, pane := range t.panes {
		if pane.TabID != ev.TabID {
			continue
		}
		t.closeAgentLocked(id, now)
		t.closePaneLocked(id, now)
	}
	t.closeTabLocked(ev.TabID, now)
}

// ApplyTabRenamed updates a tab's label. Pushes carry no pane/agent lifecycle
// information; only the label changes.
// Idempotent (2.5): duplicate event sets the same label.
func (t *Tracker) ApplyTabRenamed(ev events.NormalizedEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	tab := t.tabs[ev.TabID]
	tab.TabID = ev.TabID
	if ev.WorkspaceID != "" {
		tab.WorkspaceID = ev.WorkspaceID
	}
	tab.Label = ev.Label
	tab.UpdatedAt = t.now()
	t.tabs[ev.TabID] = tab
}

// ApplyPaneCreated records a pane and seeds tab membership. A pane's tab is
// normally already known (seeded by ApplySnapshot at baseline or by a prior
// tab.created); the seed here is an idempotent backstop for panes that arrive
// without a preceding tab event.
// Idempotent (2.5): duplicate event overwrites the same map keys harmlessly.
func (t *Tracker) ApplyPaneCreated(ev events.NormalizedEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	if pane, ok := t.panes[ev.PaneID]; ok && !pane.ClosedAt.IsZero() {
		// A created event for a pane still in its grace window is an
		// out-of-order duplicate (2.7): don't silently resurrect it.
		slog.Debug("ignoring pane.created for closing pane", "pane_id", ev.PaneID)
		return
	}
	t.panes[ev.PaneID] = PaneState{PaneID: ev.PaneID, WorkspaceID: ev.WorkspaceID, TabID: ev.TabID, UpdatedAt: now}
	if ev.TabID != "" {
		tab := t.tabs[ev.TabID]
		tab.TabID = ev.TabID
		tab.WorkspaceID = ev.WorkspaceID
		tab.UpdatedAt = now
		t.tabs[ev.TabID] = tab
	}
}

// ApplyPaneClosed closes a pane. If its agent was still `done`, the
// attention-latency interval is flushed rather than silently dropped — the
// pane may close while unseen (2.7's flush-on-close). The pane and agent then
// enter the retention grace window (2.7): counts are decremented immediately
// but the records survive until EvictExpired, so a late status event for the
// pane is absorbed instead of re-creating a phantom. Idempotent — closing an
// already-closed pane is a no-op.
func (t *Tracker) ApplyPaneClosed(ev events.NormalizedEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	t.closeAgentLocked(ev.PaneID, now)
	t.closePaneLocked(ev.PaneID, now)
}

// ApplyAgentDetected records which agent holds a pane.
// Idempotent (2.5): duplicate event sets the same agent string.
func (t *Tracker) ApplyAgentDetected(ev events.NormalizedEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if pane, ok := t.panes[ev.PaneID]; ok && !pane.ClosedAt.IsZero() {
		slog.Debug("ignoring agent.detected for closing pane", "pane_id", ev.PaneID)
		return
	}
	pane := t.panes[ev.PaneID]
	pane.Agent = ev.Agent
	pane.UpdatedAt = t.now()
	t.panes[ev.PaneID] = pane
}

// ApplyAgentStatusChanged updates the pane and, for the pane's agent, closes
// out the duration of the previous state and opens the new one (2.3). Same
// state re-applied is a no-op (idempotency seam, 2.5). Entering `done` starts
// the attention-latency clock; leaving it (via a status change event — the
// event path; the silent seen path is ApplySeenFlip) emits the closed
// interval (2.4).
func (t *Tracker) ApplyAgentStatusChanged(ev events.NormalizedEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()

	// A late status event for a pane/agent in its close grace window (2.7)
	// must be absorbed, not applied: the entity's durations were flushed and
	// counts decremented at close, so applying it would re-create a phantom
	// that leaks a state count until the next re-baseline.
	if pane, ok := t.panes[ev.PaneID]; ok && !pane.ClosedAt.IsZero() {
		slog.Debug("ignoring late status change for closing pane", "pane_id", ev.PaneID)
		return
	}
	if ag, ok := t.agents[ev.PaneID]; ok && !ag.ClosedAt.IsZero() {
		slog.Debug("ignoring late status change for closing agent", "pane_id", ev.PaneID)
		return
	}

	// Upsert: a status change may arrive for a pane whose created event was
	// missed or which predates the subscription.
	pane := t.panes[ev.PaneID]
	pane.PaneID = ev.PaneID
	pane.WorkspaceID = ev.WorkspaceID
	pane.Agent = ev.Agent
	pane.Status = ev.NewState
	pane.UpdatedAt = now
	t.panes[ev.PaneID] = pane

	ag, ok := t.agents[ev.PaneID]
	if !ok {
		tabID := ev.TabID
		if tabID == "" {
			tabID = pane.TabID
		}
		state := AgentState{
			PaneID:          ev.PaneID,
			WorkspaceID:     ev.WorkspaceID,
			TabID:           tabID,
			AgentType:       ev.Agent,
			Status:          ev.NewState,
			StateEnteredAt:  now,
			DurationByState: map[snapshot.AgentStatus]time.Duration{},
		}
		if ev.NewState == snapshot.AgentStatusDone {
			state.AttentionStartedAt = now
		}
		t.agents[ev.PaneID] = state
		t.incrStateLocked(state.WorkspaceID, state.Status)
		return
	}

	if ag.Status == ev.NewState {
		return
	}

	prevStatus := ag.Status
	t.decrStateLocked(ag.WorkspaceID, prevStatus)
	closed := now.Sub(ag.StateEnteredAt)
	ag.DurationByState[prevStatus] += closed
	t.emitStateDuration(StateDuration{
		PaneID:      ag.PaneID,
		WorkspaceID: ag.WorkspaceID,
		Agent:       ag.AgentType,
		State:       prevStatus,
		Duration:    closed,
		ObservedAt:  now,
	})
	ag.Status = ev.NewState
	ag.StateEnteredAt = now
	ag.WorkspaceID = ev.WorkspaceID
	t.incrStateLocked(ag.WorkspaceID, ev.NewState)
	if ev.Agent != "" {
		ag.AgentType = ev.Agent
	}

	if ev.NewState == snapshot.AgentStatusDone {
		// Entering done starts the attention-latency clock. The same-state
		// guard above means a duplicate done event can never re-open it.
		ag.AttentionStartedAt = now
	} else if prevStatus == snapshot.AgentStatusDone {
		t.emitAttentionLatency(AttentionLatency{
			PaneID:      ag.PaneID,
			WorkspaceID: ag.WorkspaceID,
			Agent:       ag.AgentType,
			Duration:    now.Sub(ag.AttentionStartedAt),
			ObservedAt:  now,
		})
		ag.AttentionStartedAt = time.Time{}
	}
	t.agents[ev.PaneID] = ag
	t.emitTransition(AgentTransition{
		PaneID:      ag.PaneID,
		WorkspaceID: ag.WorkspaceID,
		Agent:       ag.AgentType,
		Previous:    prevStatus,
		New:         ev.NewState,
		ObservedAt:  now,
	})
}

// ApplySeenFlip closes out a reconcile-detected done→idle transition: the
// silent "user looked at it" case, where herdr flips pane.seen with no event
// and only the next session.snapshot shows idle. It is a no-op when the agent
// is no longer tracked as `done` — a real status-changed event may have raced
// the reconcile diff and already closed the interval via
// ApplyAgentStatusChanged.
func (t *Tracker) ApplySeenFlip(paneID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()

	ag, ok := t.agents[paneID]
	// No-op when no longer tracked as `done` — a real status-changed event may
	// have raced the reconcile diff and already closed the interval — or when
	// the agent is in its close grace window (2.7), whose done interval was
	// already flushed at close.
	if !ok || ag.Status != snapshot.AgentStatusDone || !ag.ClosedAt.IsZero() {
		return
	}

	t.decrStateLocked(ag.WorkspaceID, snapshot.AgentStatusDone)
	closed := now.Sub(ag.StateEnteredAt)
	ag.DurationByState[ag.Status] += closed
	t.emitAttentionLatency(AttentionLatency{
		PaneID:      ag.PaneID,
		WorkspaceID: ag.WorkspaceID,
		Agent:       ag.AgentType,
		Duration:    now.Sub(ag.AttentionStartedAt),
		ObservedAt:  now,
	})
	t.emitStateDuration(StateDuration{
		PaneID:      ag.PaneID,
		WorkspaceID: ag.WorkspaceID,
		Agent:       ag.AgentType,
		State:       ag.Status,
		Duration:    closed,
		ObservedAt:  now,
	})
	ag.Status = snapshot.AgentStatusIdle
	ag.StateEnteredAt = now
	ag.AttentionStartedAt = time.Time{}
	t.incrStateLocked(ag.WorkspaceID, snapshot.AgentStatusIdle)
	t.agents[paneID] = ag
	t.emitTransition(AgentTransition{
		PaneID:      ag.PaneID,
		WorkspaceID: ag.WorkspaceID,
		Agent:       ag.AgentType,
		Previous:    snapshot.AgentStatusDone,
		New:         snapshot.AgentStatusIdle,
		ObservedAt:  now,
	})

	if pane, ok := t.panes[paneID]; ok {
		pane.Status = snapshot.AgentStatusIdle
		pane.UpdatedAt = now
		t.panes[paneID] = pane
	}
}

// ApplySnapshot folds a full session.snapshot into the tracked state. Used as
// the one-shot baseline on startup and again after a reconnect re-bootstrap,
// when the event stream may have a gap. After this, the stream owns the state
// until the next re-bootstrap.
//
// A re-baseline must not restart clocks for agents whose status is unchanged:
// a pane sitting in `done` across a resubscribe (which can be triggered by an
// unrelated drift) would otherwise truncate its attention-latency interval to
// the time since re-baseline (Reconcile is primary, not a backstop). For such
// agents carry the prior tracking forward; only new-to-us or changed-status
// agents seed at `now` — a conservative floor that undercounts rather than
// fabricates. This also stops 2.3's cumulative per-state durations from being
// wiped on every drift-triggered resubscribe.
func (t *Tracker) ApplySnapshot(snap snapshot.Snapshot) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()

	// Copy, don't alias: clear(t.agents) below would otherwise empty this
	// snapshot too (maps are reference types).
	prevAgents := make(map[string]AgentState, len(t.agents))
	for k, v := range t.agents {
		prevAgents[k] = v
	}

	clear(t.workspaces)
	clear(t.tabs)
	clear(t.panes)
	clear(t.agents)
	t.resetCountsLocked()

	for _, ws := range snap.Workspaces {
		t.workspaces[ws.WorkspaceID] = WorkspaceState{WorkspaceID: ws.WorkspaceID, Label: ws.Label, UpdatedAt: now}
	}
	for _, tab := range snap.Tabs {
		t.tabs[tab.TabID] = TabState{TabID: tab.TabID, WorkspaceID: tab.WorkspaceID, Label: tab.Label, UpdatedAt: now}
	}
	for _, pane := range snap.Panes {
		state := PaneState{PaneID: pane.PaneID, WorkspaceID: pane.WorkspaceID, TabID: pane.TabID, Status: pane.AgentStatus, UpdatedAt: now}
		if pane.Agent != nil {
			state.Agent = *pane.Agent
		}
		t.panes[pane.PaneID] = state

		// Seed an agent record so post-bootstrap transitions have a prior
		// state to close out a duration against.
		if pane.Agent != nil {
			seed := AgentState{
				PaneID:          pane.PaneID,
				WorkspaceID:     pane.WorkspaceID,
				TabID:           pane.TabID,
				AgentType:       *pane.Agent,
				Status:          pane.AgentStatus,
				StateEnteredAt:  now,
				DurationByState: map[snapshot.AgentStatus]time.Duration{},
			}
			if prev, ok := prevAgents[pane.PaneID]; ok && prev.Status == pane.AgentStatus {
				seed.StateEnteredAt = prev.StateEnteredAt
				seed.DurationByState = prev.DurationByState
				seed.AttentionStartedAt = prev.AttentionStartedAt
			} else if pane.AgentStatus == snapshot.AgentStatusDone {
				seed.AttentionStartedAt = now
			}
			t.agents[pane.PaneID] = seed
			t.incrStateLocked(seed.WorkspaceID, seed.Status)
		}
	}
}

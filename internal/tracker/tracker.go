package tracker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nabutabu/herdr-scribe/internal/events"
	"github.com/nabutabu/herdr-scribe/internal/snapshot"
)

type WorkspaceState struct {
	WorkspaceID string
	Label       string
	UpdatedAt   time.Time
}

type PaneState struct {
	PaneID      string
	WorkspaceID string
	TabID       string
	Agent       string
	Status      snapshot.AgentStatus
	UpdatedAt   time.Time
}

type TabState struct {
	TabID       string
	WorkspaceID string
	Label       string // only ever known via snapshot — no event carries it
	UpdatedAt   time.Time
}

type AgentState struct {
	PaneID      string
	WorkspaceID string
	TabID       string
	AgentType   string // e.g. "codex" — from the event's Agent field

	Status         snapshot.AgentStatus
	StateEnteredAt time.Time

	// DurationByState holds only *closed* intervals — time already spent in
	// a state before the most recent transition out of it. The open
	// interval for the current Status is deliberately not kept here;
	// callers wanting an as-of-now total add time.Since(StateEnteredAt).
	DurationByState map[snapshot.AgentStatus]time.Duration

	// AttentionStartedAt is set when Status transitions into
	// snapshot.AgentStatusDone, zero otherwise. Kept separate from
	// DurationByState because attention latency is its own MVP metric
	// (2.4, 3.5), not folded into generic "time in done".
	AttentionStartedAt time.Time
}

// AttentionLatency is emitted when an agent leaves `done` — the elapsed time
// between entering done and being "seen". Phase 3 (3.5) wires this to an OTel
// histogram; this layer only computes and surfaces it.
//
// Wire fact (herdr src/app/api.rs emit_pane_state_update): entering `done`
// (working/blocked completion to idle while the pane is unseen) pushes a
// pane.agent_status_changed event, but leaving `done` by merely being *seen*
// (focus/navigation flips pane.seen, herdr src/app/actions.rs
// mark_active_tab_seen) emits nothing at all — the status only reverts to
// idle in session.snapshot. So there are two close paths: a real status
// change event (resume), and a reconcile-detected done→idle seen-flip.
type AttentionLatency struct {
	PaneID      string
	WorkspaceID string
	Agent       string
	Duration    time.Duration
	ObservedAt  time.Time
}

// Tracker holds the live picture of the session as currently known. Steady
// state is event-driven — the app's dispatcher calls the per-kind Apply*
// methods; the bootstrap snapshot is folded in once via ApplySnapshot. It is
// never adopted from a snapshot after that — Run's reconcile step only diffs
// and signals (reports drifts AND seen-flips to app.Run), it never mutates, so
// that diffing has an independent truth to compare against (0.4: events carry
// no sequence number and the stream can silently stall).
type Tracker struct {
	mu         sync.RWMutex
	workspaces map[string]WorkspaceState
	tabs       map[string]TabState
	panes      map[string]PaneState
	agents     map[string]AgentState

	// attentionLatency surfaces closed done-intervals. Non-blocking send with
	// drop-and-warn on backpressure, matching Subscriber's events channel
	// idiom.
	attentionLatency chan AttentionLatency

	// now is the clock used by the Apply* methods for duration math. Defaults
	// to time.Now; tests substitute a fake to make duration assertions
	// deterministic.
	now func() time.Time
}

func NewTracker() *Tracker {
	return &Tracker{
		workspaces:       map[string]WorkspaceState{},
		tabs:             map[string]TabState{},
		panes:            map[string]PaneState{},
		agents:           map[string]AgentState{},
		attentionLatency: make(chan AttentionLatency, 64),
		now:              time.Now,
	}
}

// AttentionLatency delivers closed attention-latency intervals as they are
// observed (2.4). Phase 3.5 consumes this channel; the app currently just
// logs from it.
func (t *Tracker) AttentionLatency() <-chan AttentionLatency { return t.attentionLatency }

func (t *Tracker) emitAttentionLatency(al AttentionLatency) {
	select {
	case t.attentionLatency <- al:
	default:
		slog.Warn("attention latency channel full; dropping", "pane_id", al.PaneID, "agent", al.Agent)
	}
}

// ApplyWorkspaceCreated folds a workspace.created event into the tracked state.
// Idempotent (2.5): a duplicate event overwrites the same map key with
// identical data (aside from UpdatedAt, which is benign).
func (t *Tracker) ApplyWorkspaceCreated(ev events.NormalizedEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.workspaces[ev.WorkspaceID] = WorkspaceState{WorkspaceID: ev.WorkspaceID, UpdatedAt: t.now()}
}

// ApplyWorkspaceClosed removes a workspace and cascades its tabs and panes —
// their own close events may not arrive.
// Idempotent (2.5): deleting already-missing keys is a no-op.
func (t *Tracker) ApplyWorkspaceClosed(ev events.NormalizedEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.workspaces, ev.WorkspaceID)
	for id, tab := range t.tabs {
		if tab.WorkspaceID == ev.WorkspaceID {
			delete(t.tabs, id)
		}
	}
	for id, pane := range t.panes {
		if pane.WorkspaceID == ev.WorkspaceID {
			delete(t.panes, id)
		}
	}
}

// ApplyPaneCreated records a pane and seeds tab membership. No tab events
// exist (0.2), so pane.created is the only event that can seed a tab.
// Idempotent (2.5): duplicate event overwrites the same map keys harmlessly.
func (t *Tracker) ApplyPaneCreated(ev events.NormalizedEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	t.panes[ev.PaneID] = PaneState{PaneID: ev.PaneID, WorkspaceID: ev.WorkspaceID, TabID: ev.TabID, UpdatedAt: now}
	if ev.TabID != "" {
		tab := t.tabs[ev.TabID]
		tab.TabID = ev.TabID
		tab.WorkspaceID = ev.WorkspaceID
		tab.UpdatedAt = now
		t.tabs[ev.TabID] = tab
	}
}

// ApplyPaneClosed removes a pane. If its agent was still `done`, the
// attention-latency interval is flushed rather than silently dropped — the
// pane may close while unseen (2.7's flush-on-close).
// Idempotent (2.5): deleting an already-missing key is a no-op.
func (t *Tracker) ApplyPaneClosed(ev events.NormalizedEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	if ag, ok := t.agents[ev.PaneID]; ok && ag.Status == snapshot.AgentStatusDone {
		t.emitAttentionLatency(AttentionLatency{
			PaneID:      ag.PaneID,
			WorkspaceID: ag.WorkspaceID,
			Agent:       ag.AgentType,
			Duration:    now.Sub(ag.AttentionStartedAt),
			ObservedAt:  now,
		})
	}
	delete(t.agents, ev.PaneID)
	delete(t.panes, ev.PaneID)
}

// ApplyAgentDetected records which agent holds a pane.
// Idempotent (2.5): duplicate event sets the same agent string.
func (t *Tracker) ApplyAgentDetected(ev events.NormalizedEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
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
		return
	}

	if ag.Status == ev.NewState {
		return
	}

	prevStatus := ag.Status
	ag.DurationByState[prevStatus] += now.Sub(ag.StateEnteredAt)
	ag.Status = ev.NewState
	ag.StateEnteredAt = now
	ag.WorkspaceID = ev.WorkspaceID
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
	if !ok || ag.Status != snapshot.AgentStatusDone {
		return
	}

	ag.DurationByState[ag.Status] += now.Sub(ag.StateEnteredAt)
	t.emitAttentionLatency(AttentionLatency{
		PaneID:      ag.PaneID,
		WorkspaceID: ag.WorkspaceID,
		Agent:       ag.AgentType,
		Duration:    now.Sub(ag.AttentionStartedAt),
		ObservedAt:  now,
	})
	ag.Status = snapshot.AgentStatusIdle
	ag.StateEnteredAt = now
	ag.AttentionStartedAt = time.Time{}
	t.agents[paneID] = ag

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
		}
	}
}

// Drift is one observed difference between tracked state and a snapshot.
type Drift struct {
	Kind    string // "workspace" | "pane"
	ID      string
	Tracked string // "" when absent from the tracker
	Actual  string // "" when absent from the snapshot
}

// SeenFlip is a pane whose tracked status was `done` but the fresh snapshot
// reports `idle` — the silent "the user looked at it" transition (2.4). Herdr
// pushes no event for it: pane.seen is flipped by focus/navigation without a
// PaneStateUpdate (herdr src/app/actions.rs mark_active_tab_seen), so the
// status only reverts to idle inside session.snapshot. Reconciliation is the
// sole detector. Unlike a Drift, a SeenFlip is benign — it must not tear down
// a healthy subscription.
type SeenFlip struct {
	PaneID      string
	WorkspaceID string
}

type DiffReport struct {
	Drifts    []Drift
	SeenFlips []SeenFlip
}

func (r DiffReport) Drifted() bool { return len(r.Drifts) > 0 }

func (r DiffReport) Empty() bool { return len(r.Drifts) == 0 && len(r.SeenFlips) == 0 }

// Diff compares tracked state against a fresh snapshot without mutating.
// Workspaces and panes participate; tabs deliberately do not — events can
// never remove a tab (0.2), so tab membership is not a reliable drift probe
// and would only manufacture false resubscribes.
func (t *Tracker) Diff(snap snapshot.Snapshot) DiffReport {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var drifts []Drift
	var flips []SeenFlip

	for id := range t.workspaces {
		if !hasWorkspace(snap.Workspaces, id) {
			drifts = append(drifts, Drift{Kind: "workspace", ID: id, Tracked: "present"})
		}
	}
	for _, ws := range snap.Workspaces {
		if _, ok := t.workspaces[ws.WorkspaceID]; !ok {
			drifts = append(drifts, Drift{Kind: "workspace", ID: ws.WorkspaceID, Actual: "present"})
		}
	}

	snapPanes := make(map[string]snapshot.Pane, len(snap.Panes))
	for _, pane := range snap.Panes {
		snapPanes[pane.PaneID] = pane
	}

	for id, pane := range t.panes {
		sp, ok := snapPanes[id]
		if !ok {
			drifts = append(drifts, Drift{Kind: "pane", ID: id, Tracked: describePaneTracked(pane)})
			continue
		}
		// Tracked done + snapshot idle is the silent seen-flip, not a drift:
		// the stream is healthy, the user just looked. Any other mismatch is
		// a genuine missing-event gap -> resubscribe.
		if pane.Status == snapshot.AgentStatusDone && sp.AgentStatus == snapshot.AgentStatusIdle {
			flips = append(flips, SeenFlip{PaneID: id, WorkspaceID: sp.WorkspaceID})
			continue
		}
		tracked := describePaneTracked(pane)
		actual := describePaneSnapshot(sp)
		if tracked != actual {
			drifts = append(drifts, Drift{Kind: "pane", ID: id, Tracked: tracked, Actual: actual})
		}
	}
	for id, sp := range snapPanes {
		if _, ok := t.panes[id]; !ok {
			drifts = append(drifts, Drift{Kind: "pane", ID: id, Actual: describePaneSnapshot(sp)})
		}
	}

	return DiffReport{Drifts: drifts, SeenFlips: flips}
}

// Run owns the periodic liveness check (1.8). It opens a fresh short-lived
// session.snapshot connection each interval, diffs it against the tracked
// state, and calls onReport with the result when anything disagrees — either a
// genuine drift (the sole signal for forcing a subscription re-create) or a
// benign seen-flip (2.4). Run never mutates tracker state: applying a
// seen-flip is App.Run's job via ApplySeenFlip, so the single-writer rule
// holds.
func (t *Tracker) Run(ctx context.Context, interval time.Duration, onReport func(DiffReport)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.reconcile(ctx, onReport)
		}
	}
}

// reconcile performs one liveness probe. A fetch failure is logged and skipped
// — that is the loud-failure path owned by the subscriber's reconnect logic,
// not this check (0.4: this loop exists for the silent failure mode).
func (t *Tracker) reconcile(ctx context.Context, onReport func(DiffReport)) {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	resp, err := snapshot.Fetch(tctx)
	cancel()
	if err != nil {
		slog.Warn("reconcile: session.snapshot failed", "error", err)
		return
	}

	report := t.Diff(resp.Snapshot)
	if report.Empty() {
		return
	}
	for _, d := range report.Drifts {
		slog.Warn("reconcile: state drift", "kind", d.Kind, "id", d.ID, "tracked", d.Tracked, "actual", d.Actual)
	}
	for _, sf := range report.SeenFlips {
		slog.Info("reconcile: done pane seen via snapshot", "pane_id", sf.PaneID)
	}
	onReport(report)
}

func hasWorkspace(ws []snapshot.Workspace, id string) bool {
	for _, w := range ws {
		if w.WorkspaceID == id {
			return true
		}
	}
	return false
}

func describePaneTracked(p PaneState) string {
	return fmt.Sprintf("workspace=%s tab=%s agent=%s status=%s", p.WorkspaceID, p.TabID, p.Agent, p.Status)
}

func describePaneSnapshot(p snapshot.Pane) string {
	agent := ""
	if p.Agent != nil {
		agent = *p.Agent
	}
	return fmt.Sprintf("workspace=%s tab=%s agent=%s status=%s", p.WorkspaceID, p.TabID, agent, p.AgentStatus)
}

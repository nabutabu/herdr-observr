package tracker

import (
	"log/slog"
	"sync"
	"time"

	"github.com/nabutabu/herdr-scribe/internal/snapshot"
)

// Tracker holds the live picture of the session as currently known. Steady
// state is event-driven — the app's dispatcher calls the per-kind Apply*
// methods; the bootstrap snapshot is folded in once via ApplySnapshot. It is
// never adopted from a snapshot after that — Run's reconcile step only diffs
// and signals (reports drifts AND seen-flips to app.Run), it never mutates, so
// that diffing has an independent truth to compare against (0.4: events carry
// no sequence number and the stream can silently stall).
//
// The tracked types live in types.go, the Apply* writers in apply.go, and the
// read-only reconciliation (Diff/Run/reconcile) in diff.go.
type Tracker struct {
	mu         sync.RWMutex
	workspaces map[string]WorkspaceState
	tabs       map[string]TabState
	panes      map[string]PaneState
	agents     map[string]AgentState

	// stateCounts and workspaceStateCounts maintain the live per-state agent
	// counts (2.6), updated incrementally on every transition and recomputed
	// wholesale by ApplySnapshot. Phase 3.6/3.7 read them via Counts().
	stateCounts          map[snapshot.AgentStatus]int
	workspaceStateCounts map[string]map[snapshot.AgentStatus]int

	// attentionLatency surfaces closed done-intervals. Non-blocking send with
	// drop-and-warn on backpressure, matching Subscriber's events channel
	// idiom.
	attentionLatency chan AttentionLatency

	// transitions surfaces genuine agent state transitions (3.3's
	// herdr.agent.state.transitions counter). Non-blocking send with
	// drop-and-warn on backpressure, same idiom as attentionLatency: under
	// sustained backpressure an increment is the least-bad loss, never a
	// block on the state machine.
	transitions chan AgentTransition

	// graceWindow is how long a closed pane/tab/agent is retained after its
	// close is applied, so late-arriving events are absorbed instead of
	// re-creating a phantom (2.7). Defaults to DefaultGraceWindow; EvictExpired
	// drops records older than this.
	graceWindow time.Duration

	// now is the clock used by the Apply* methods for duration math. Defaults
	// to time.Now; tests substitute a fake to make duration assertions
	// deterministic.
	now func() time.Time
}

// DefaultGraceWindow is the retention window for closed entities (2.7):
// long enough to absorb late events after a close, short enough that
// closed panes never linger meaningfully in memory (no disk persistence).
const DefaultGraceWindow = 15 * time.Second

func NewTracker() *Tracker {
	return &Tracker{
		workspaces:           map[string]WorkspaceState{},
		tabs:                 map[string]TabState{},
		panes:                map[string]PaneState{},
		agents:               map[string]AgentState{},
		attentionLatency:     make(chan AttentionLatency, 64),
		transitions:          make(chan AgentTransition, 64),
		now:                  time.Now,
		graceWindow:          DefaultGraceWindow,
		stateCounts:          map[snapshot.AgentStatus]int{},
		workspaceStateCounts: map[string]map[snapshot.AgentStatus]int{},
	}
}

// AttentionLatency delivers closed attention-latency intervals as they are
// observed (2.4). Phase 3.5 consumes this channel; the app currently just
// logs from it.
func (t *Tracker) AttentionLatency() <-chan AttentionLatency { return t.attentionLatency }

// Transitions delivers genuine agent state transitions as they are observed
// (2.3). Phase 3.3 consumes this channel to increment
// herdr.agent.state.transitions.
func (t *Tracker) Transitions() <-chan AgentTransition { return t.transitions }

// Tabs returns the tracked tab state as a shallow copy. Safe to call from any
// goroutine; phase 3 exporters snapshot it rather than reading under the
// tracker lock.
func (t *Tracker) Tabs() map[string]TabState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	tabs := make(map[string]TabState, len(t.tabs))
	for k, v := range t.tabs {
		tabs[k] = v
	}
	return tabs
}

// Agents returns the tracked agent state as a shallow copy. Safe to call from
// any goroutine; phase 3 exporters snapshot it rather than reading under the
// tracker lock.
func (t *Tracker) Agents() map[string]AgentState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	agents := make(map[string]AgentState, len(t.agents))
	for k, v := range t.agents {
		agents[k] = v
	}
	return agents
}

func (t *Tracker) emitAttentionLatency(al AttentionLatency) {
	select {
	case t.attentionLatency <- al:
	default:
		slog.Warn("attention latency channel full; dropping", "pane_id", al.PaneID, "agent", al.Agent)
	}
}

func (t *Tracker) emitTransition(tr AgentTransition) {
	select {
	case t.transitions <- tr:
	default:
		slog.Warn("transition channel full; dropping", "pane_id", tr.PaneID, "agent", tr.Agent, "previous", tr.Previous, "new", tr.New)
	}
}

// GraceWindow returns the configured retention window for closed entities
// (2.7).
func (t *Tracker) GraceWindow() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.graceWindow
}

// EvictExpired drops closed entities whose grace window has elapsed (2.7).
// Live entities (ClosedAt zero) are never touched. Called from app.Run's
// event loop on its own ticker, so all tracker mutation stays on the
// single-writer goroutine.
func (t *Tracker) EvictExpired() {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	for id, tab := range t.tabs {
		if !tab.ClosedAt.IsZero() && now.Sub(tab.ClosedAt) > t.graceWindow {
			delete(t.tabs, id)
		}
	}
	for id, pane := range t.panes {
		if !pane.ClosedAt.IsZero() && now.Sub(pane.ClosedAt) > t.graceWindow {
			delete(t.panes, id)
		}
	}
	for id, ag := range t.agents {
		if !ag.ClosedAt.IsZero() && now.Sub(ag.ClosedAt) > t.graceWindow {
			delete(t.agents, id)
		}
	}
}

// closeAgentLocked marks a tracked agent closed (2.7): final state interval
// flushed, attention-latency emitted if it was still `done`, the freed
// DurationByState map nilled, the state count decremented immediately, and
// ClosedAt stamped so the record survives its grace window. Idempotent:
// an already-closed agent is a no-op — this is what keeps the workspace/tab
// close cascades safe to overlap (a tab.closed followed by workspace.closed
// must flush attention latency and decrement each agent exactly once).
func (t *Tracker) closeAgentLocked(id string, now time.Time) {
	ag, ok := t.agents[id]
	if !ok || !ag.ClosedAt.IsZero() {
		return
	}
	if ag.Status == snapshot.AgentStatusDone {
		t.emitAttentionLatency(AttentionLatency{
			PaneID:      ag.PaneID,
			WorkspaceID: ag.WorkspaceID,
			Agent:       ag.AgentType,
			Duration:    now.Sub(ag.AttentionStartedAt),
			ObservedAt:  now,
		})
	}
	ag.DurationByState[ag.Status] += now.Sub(ag.StateEnteredAt)
	ag.DurationByState = nil
	ag.ClosedAt = now
	t.agents[id] = ag
	t.decrStateLocked(ag.WorkspaceID, ag.Status)
}

// closePaneLocked marks a tracked pane closed (2.7). Idempotent: an
// already-closed pane is a no-op.
func (t *Tracker) closePaneLocked(id string, now time.Time) {
	pane, ok := t.panes[id]
	if !ok || !pane.ClosedAt.IsZero() {
		return
	}
	pane.ClosedAt = now
	t.panes[id] = pane
}

// closeTabLocked marks a tracked tab closed (2.7). Idempotent: an
// already-closed tab is a no-op. A tab.closed arriving after a
// workspace.closed already cascaded it is therefore absorbed.
func (t *Tracker) closeTabLocked(id string, now time.Time) {
	tab, ok := t.tabs[id]
	if !ok || !tab.ClosedAt.IsZero() {
		return
	}
	tab.ClosedAt = now
	t.tabs[id] = tab
}

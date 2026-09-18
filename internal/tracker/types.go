package tracker

import (
	"time"

	"github.com/nabutabu/herdr-observr/internal/snapshot"
)

// WorkspaceState is the tracker's record for one workspace. Only identity and
// label survive; the workspace's wire metadata (counts, focus, tokens) is not
// consumed.
type WorkspaceState struct {
	WorkspaceID string
	Label       string
	UpdatedAt   time.Time
}

// PaneState is the tracker's record for one pane: identity, tab membership,
// the agent currently holding it, the agent status the tracker last saw, and
// the pane's current agent_session attribution as reported by the snapshot.
type PaneState struct {
	PaneID      string
	WorkspaceID string
	TabID       string
	Agent       string
	Status      snapshot.AgentStatus
	// AgentSession is the wire agent_session attribution from the latest
	// snapshot (nil when the pane has no agent session). It answers "which
	// session is frontmost right now", not "which session has this pane run
	// all along" — the value can flip between sessions in one pane, so it must
	// not be treated as a stable pane↔session binding. Nil on event-created
	// panes; only ApplySnapshot populates it.
	AgentSession *snapshot.AgentSessionInfo
	UpdatedAt    time.Time

	// ClosedAt is set when the pane's close is applied (2.7). Non-zero means
	// the pane is in the bounded-retention grace window: live tracking over,
	// but the record is retained briefly so late events are absorbed instead
	// of creating a phantom. Zero means the pane is live.
	ClosedAt time.Time
}

// TabState is the tracker's record for one tab. Label comes from
// tab.created/tab.renamed events and the snapshot.
type TabState struct {
	TabID       string
	WorkspaceID string
	Label       string
	UpdatedAt   time.Time

	// ClosedAt set means the tab is in the retention grace window (2.7),
	// same semantics as PaneState.ClosedAt.
	ClosedAt time.Time
}

// AgentState is the tracker's record for one agent, keyed by the pane it
// holds (the wire scopes status events per pane). It carries the agent's type
// (e.g. "codex"), current status, duration-by-state accounting, and the open
// attention-latency clock.
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
	//
	// On close (2.7) the open interval is flushed into this map and the map
	// is then freed (set to nil). Each closed interval is surfaced at exit
	// time via the StateDuration channel (3.4), so the map here is pure
	// accounting rather than the export path; only identity + ClosedAt
	// metadata is retained through the grace window.
	DurationByState map[snapshot.AgentStatus]time.Duration

	// AttentionStartedAt is set when Status transitions into
	// snapshot.AgentStatusDone, zero otherwise. Kept separate from
	// DurationByState because attention latency is its own MVP metric
	// (2.4, 3.5), not folded into generic "time in done".
	AttentionStartedAt time.Time

	// ClosedAt set means the agent's pane/tab/workspace closed and the agent
	// is in the retention grace window (2.7). State counts are already
	// decremented; the record survives only so late status events don't
	// re-create a phantom. Zero means the agent is live.
	ClosedAt time.Time
}

// AgentTransition is one observed change from one tracked agent state to
// another — a genuine status change (2.3), whether event-driven
// (ApplyAgentStatusChanged) or reconcile-detected (ApplySeenFlip's silent
// done→idle). It feeds the 3.3 OTel counter (herdr.agent.state.transitions);
// this layer only computes and surfaces it.
//
// Never emitted for same-state re-applies, first-seen upserts (no previous
// state to report), re-baseline seeding, or close-time flushes — those are
// not transitions.
type AgentTransition struct {
	PaneID      string
	WorkspaceID string
	Agent       string
	Previous    snapshot.AgentStatus
	New         snapshot.AgentStatus
	ObservedAt  time.Time
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

// StateDuration is emitted whenever a tracked agent's interval in a state is
// closed (2.3): the elapsed time it spent in State, recorded exactly once at
// the moment the agent leaves that state. Its three close sites are the real
// transition (ApplyAgentStatusChanged), the silent done→idle close
// (ApplySeenFlip), and the close-time flush (closeAgentLocked). It feeds the
// 3.4 OTel histogram (herdr.agent.state.duration); this layer only computes
// and surfaces it.
//
// Never emitted for first-seen upserts (no prior state), same-state re-applies,
// or re-baseline seeding — nothing is closed there. The closed interval is the
// same value accumulated into DurationByState, surfaced here at exit time,
// which is what makes the 3.4 histogram a *closed* duration metric rather than
// a re-sampled live age.
type StateDuration struct {
	PaneID      string
	WorkspaceID string
	Agent       string
	State       snapshot.AgentStatus
	Duration    time.Duration
	ObservedAt  time.Time
}

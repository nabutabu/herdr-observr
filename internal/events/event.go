// internal/events/event.go
package events

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/nabutabu/herdr-scribe/internal/snapshot"
)

// Kind identifies which lifecycle event a NormalizedEvent represents.
// Deliberately a closed set matching the scoped subscription in
// subscription.go (BuildParams) — anything outside this set is unrecognized.
type Kind string

const (
	KindWorkspaceCreated    Kind = "workspace_created"
	KindWorkspaceClosed     Kind = "workspace_closed"
	KindTabCreated          Kind = "tab_created"
	KindTabClosed           Kind = "tab_closed"
	KindTabRenamed          Kind = "tab_renamed"
	KindPaneCreated         Kind = "pane_created"
	KindPaneClosed          Kind = "pane_closed"
	KindAgentDetected       Kind = "pane_agent_detected"
	KindAgentStatusChanged  Kind = "pane_agent_status_changed"
	KindSubscriptionStarted Kind = "subscription_started"
)

type frameKind int

const (
	frameUnknown frameKind = iota
	frameEvent
	frameAck
)

// wireFrame covers every field that appears in either the subscribe ack
// envelope (including subscription_started) or a pushed event, so a single
// json.Unmarshal can classify AND normalize in one pass.
type wireFrame struct {
	// Ack envelope
	ID     string          `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  json.RawMessage `json:"error"`

	// Pushed event envelope. The "event" name is authoritative; "data"
	// carries the payload.
	Event string       `json:"event"`
	Data  eventPayload `json:"data"`
}

// eventPayload is the body Herdr pushes inside the "data" key. Ids may be
// flattened directly here or nested in a per-entity object: workspace_created,
// tab_created, and pane_created carry full entity snapshots (data.workspace /
// data.tab / data.pane), while pane_closed, pane.agent_detected, and
// pane.agent_status_changed flatten pane_id/workspace_id at top level. "type"
// mirrors the event name on most pushes but is absent on
// pane.agent_status_changed.
type eventPayload struct {
	Type        string               `json:"type"`
	WorkspaceID string               `json:"workspace_id"`
	PaneID      string               `json:"pane_id"`
	TabID       string               `json:"tab_id"`
	Agent       string               `json:"agent"`
	AgentStatus snapshot.AgentStatus `json:"agent_status"`
	Label       string               `json:"label"`

	Workspace *workspaceRef `json:"workspace"`
	Tab       *tabRef       `json:"tab"`
	Pane      *paneRef      `json:"pane"`
}

type workspaceRef struct {
	WorkspaceID string `json:"workspace_id"`
}

// tabRef is the tab payload on tab.created (a full TabInfo:
// {tab_id, workspace_id, number, label, focused, pane_count, agent_status}).
// Only the id/label fields are modeled; the rest is display/geometry metadata.
type tabRef struct {
	TabID       string `json:"tab_id"`
	WorkspaceID string `json:"workspace_id"`
	Label       string `json:"label"`
}

type paneRef struct {
	PaneID      string `json:"pane_id"`
	WorkspaceID string `json:"workspace_id"`
	TabID       string `json:"tab_id"`
}

// NormalizedEvent is the seam between Herdr's wire format and everything
// downstream (Phase 2's state tracker, Phase 3's OTel export). Fields are
// flat rather than a variant-per-Kind type to keep consumers simple; unused
// fields for a given Kind are left zero-valued.
//
// Deliberately excluded, per 0.4: sequence numbers, revisions, display
// fields, and previous-state. None of these are present on the wire for
// pane.agent_status_changed, and previous-state in particular is Phase 2's
// responsibility (2.2's in-memory agent struct), not this layer's.
type NormalizedEvent struct {
	Kind        Kind
	WorkspaceID string
	PaneID      string // zero for workspace/tab-level events
	TabID       string // populated for pane.created and all tab.* events

	// Agent identifies which agent this event concerns. Populated for
	// AgentDetected and AgentStatusChanged only.
	Agent string

	// NewState is the state an AgentStatusChanged event is reporting.
	// Zero value for all other Kinds.
	NewState snapshot.AgentStatus

	// Label is the tab label reported by tab.created/tab.renamed. Zero for
	// all other Kinds.
	Label string

	// Raw preserves the original wire frame for debugging/replay. Not
	// intended for downstream logic to depend on.
	Raw json.RawMessage
}

// ErrUnrecognizedEvent means the "type" field didn't match any Kind this
// normalizer knows about. Callers should log and skip, same pattern as
// Subscriber's classify() for unknown frames — protocol drift (0.2) should
// degrade gracefully here too.
var ErrUnrecognizedEvent = fmt.Errorf("unrecognized event type")

// Normalize parses a single event frame outside the subscription stream —
// useful for tests and tooling. The stream itself (Subscriber.run) calls
// parseFrame directly to avoid decoding each frame twice.
func Normalize(raw json.RawMessage) (NormalizedEvent, error) {
	kind, ev, err := parseFrame(raw)
	if err != nil {
		return NormalizedEvent{}, err
	}
	if kind != frameEvent {
		return NormalizedEvent{}, fmt.Errorf("%w: not a pushed event", ErrUnrecognizedEvent)
	}
	return ev, nil
}

// parseFrame decodes a raw wire frame exactly once and returns both its
// classification and, for a recognized pushed event, the normalized form.
// Replaces the old classify() + Normalize() split, which decoded every
// event frame twice.
func parseFrame(raw json.RawMessage) (frameKind, NormalizedEvent, error) {
	var w wireFrame
	if err := json.Unmarshal(raw, &w); err != nil {
		return frameUnknown, NormalizedEvent{}, fmt.Errorf("parsing frame %q: %w", string(raw), err)
	}

	// No top-level "event" — either an ack (subscription_started has its
	// "type" nested under result, so it lands here) or truly unknown.
	if w.Event == "" {
		if w.ID != "" || w.Result != nil || w.Error != nil {
			return frameAck, NormalizedEvent{}, nil
		}
		return frameUnknown, NormalizedEvent{}, nil
	}

	ev := NormalizedEvent{Raw: raw}
	if w.Data.Workspace != nil {
		ev.WorkspaceID = w.Data.Workspace.WorkspaceID
	}
	if w.Data.Tab != nil {
		ev.TabID = w.Data.Tab.TabID
		ev.WorkspaceID = w.Data.Tab.WorkspaceID
		ev.Label = w.Data.Tab.Label
	}
	if w.Data.Pane != nil {
		ev.PaneID = w.Data.Pane.PaneID
		ev.WorkspaceID = w.Data.Pane.WorkspaceID
		ev.TabID = w.Data.Pane.TabID
	}
	if ev.WorkspaceID == "" {
		ev.WorkspaceID = w.Data.WorkspaceID
	}
	if ev.PaneID == "" {
		ev.PaneID = w.Data.PaneID
	}
	if ev.TabID == "" {
		ev.TabID = w.Data.TabID
	}
	if ev.Label == "" {
		ev.Label = w.Data.Label
	}
	ev.Agent = w.Data.Agent
	ev.NewState = w.Data.AgentStatus

	// The top-level "event" is the authoritative name; data.type is missing
	// on pane.agent_status_changed. Herdr is inconsistent about naming style —
	// pane.agent_status_changed arrives dotted while everything else is
	// underscored — so collapse dots before matching.
	name := w.Event
	if name == "" {
		name = w.Data.Type
	}
	kind := Kind(strings.ReplaceAll(name, ".", "_"))

	switch kind {
	case KindWorkspaceCreated,
		KindWorkspaceClosed,
		KindTabCreated,
		KindTabClosed,
		KindTabRenamed,
		KindPaneCreated,
		KindPaneClosed,
		KindAgentDetected,
		KindAgentStatusChanged:
		ev.Kind = kind
	default:
		// Recognized envelope but an event we didn't actually scope out
		// (protocol drift, or e.g. pane.scroll_changed sending anyway).
		return frameEvent, NormalizedEvent{}, fmt.Errorf("%w: %q", ErrUnrecognizedEvent, name)
	}

	return frameEvent, ev, nil
}

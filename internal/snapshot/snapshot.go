package snapshot

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nabutabu/herdr-observr/internal/client"
)

// Response is the top-level session.snapshot envelope. The wire frame also
// carries a "type" field; json.Unmarshal ignores it here.
type Response struct {
	Snapshot Snapshot `json:"snapshot"`
}

// Snapshot holds only the parts of a session.snapshot the plugin consumes:
// workspace/tab/pane identity collections. The live wire shape carries much
// more — version, protocol, focused_* ids, an agents[] collection, per-pane
// terminal titles/cwd/scroll geometry, token maps, and per-entity counts —
// which are intentionally not modeled: json.Unmarshal ignores unknown fields,
// so a full live packet still parses, and keeping pane/terminal content and
// UI-only metadata out of the structs (and out of the tests) keeps the
// privacy constraint — no pane/terminal content on the wire — visible in the
// contract itself rather than trusting every caller to filter.
type Snapshot struct {
	Workspaces []Workspace `json:"workspaces"`
	Tabs       []Tab       `json:"tabs"`
	Panes      []Pane      `json:"panes"`
}

// Tab mirrors the fields of the wire tabs[] collection that the tracker
// consumes: identity and label drive tab tracking and membership drift
// (finding #8). The rest of the wire tab shape (number, focused,
// pane_count, agent_status) is metadata the tracker never reads.
type Tab struct {
	TabID       string `json:"tab_id"`
	WorkspaceID string `json:"workspace_id"`
	Label       string `json:"label"`
}

func Fetch(ctx context.Context) (Response, error) {
	raw, err := client.Call(ctx, "session.snapshot", map[string]any{})
	if err != nil {
		return Response{}, fmt.Errorf("session snapshot: %w", err)
	}

	var resp Response
	if err := json.Unmarshal(raw, &resp); err != nil {
		return Response{}, fmt.Errorf("parsing session snapshot %q: %w", string(raw), err)
	}

	return resp, nil
}

type AgentStatus string

const (
	AgentStatusIdle    AgentStatus = "idle"
	AgentStatusWorking AgentStatus = "working"
	AgentStatusBlocked AgentStatus = "blocked"
	AgentStatusDone    AgentStatus = "done"
	AgentStatusUnknown AgentStatus = "unknown"
)

// Workspace holds only workspace identity for the tracker's workspace map and
// drift diffs. The wire shape's counts, focus, active_tab_id, agent_status,
// and tokens are not consumed.
type Workspace struct {
	WorkspaceID string `json:"workspace_id"`
	Label       string `json:"label"`
}

// Pane holds only the fields the tracker mirrors: pane/workspace/tab identity,
// the agent status, and the held agent's type (seed for AgentState.AgentType).
// Everything else on the wire pane shape — terminal_id, focused, revision,
// cwd, labels/titles, scroll geometry, session refs, state_labels, tokens —
// is intentionally not captured (privacy: no pane/terminal content).
type Pane struct {
	PaneID      string      `json:"pane_id"`
	WorkspaceID string      `json:"workspace_id"`
	TabID       string      `json:"tab_id"`
	AgentStatus AgentStatus `json:"agent_status"`
	Agent       *string     `json:"agent"`
}

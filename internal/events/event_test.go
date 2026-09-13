package events

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/nabutabu/herdr-scribe/internal/snapshot"
)

func TestNormalizeWorkspaceEvents(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		kind      Kind
		workspace string
	}{
		{
			name:      "workspace.created",
			raw:       `{"data":{"type":"workspace_created","workspace":{"active_tab_id":"wS:t1","agent_status":"unknown","focused":true,"label":"herdr-scribe","number":2,"pane_count":1,"tab_count":1,"workspace_id":"wS"}},"event":"workspace_created"}`,
			kind:      KindWorkspaceCreated,
			workspace: "wS",
		},
		{
			name:      "workspace.closed",
			raw:       `{"data":{"type":"workspace_closed","workspace":{"active_tab_id":"wS:t1","agent_status":"unknown","focused":true,"label":"herdr-scribe","number":2,"pane_count":1,"tab_count":1,"workspace_id":"wS"},"workspace_id":"wS"},"event":"workspace_closed"}`,
			kind:      KindWorkspaceClosed,
			workspace: "wS",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev, err := Normalize(json.RawMessage(tt.raw))
			if err != nil {
				t.Fatalf("Normalize: %v", err)
			}
			if ev.Kind != tt.kind {
				t.Errorf("Kind = %q, want %q", ev.Kind, tt.kind)
			}
			if ev.WorkspaceID != tt.workspace {
				t.Errorf("WorkspaceID = %q, want %q", ev.WorkspaceID, tt.workspace)
			}
			if ev.PaneID != "" || ev.TabID != "" || ev.Agent != "" || ev.NewState != "" {
				t.Errorf("unexpected fields populated: %+v", ev)
			}
		})
	}
}

func TestNormalizePaneEvents(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		kind      Kind
		pane      string
		workspace string
		tab       string
	}{
		{
			name:      "pane.created with tab_id",
			raw:       `{"data":{"pane":{"agent_status":"unknown","cwd":"/home/nabutabu/programs/herdr-scribe","focused":false,"pane_id":"wT:p2","revision":0,"scroll":{"max_offset_from_bottom":0,"offset_from_bottom":0,"viewport_rows":22},"tab_id":"wT:t2","terminal_id":"term_65b27dfff8dfa14","workspace_id":"wT"},"type":"pane_created"},"event":"pane_created"}`,
			kind:      KindPaneCreated,
			pane:      "wT:p2",
			workspace: "wT",
			tab:       "wT:t2",
		},
		{
			name:      "pane.created without tab_id",
			raw:       `{"data":{"pane":{"pane_id":"w1:p1","workspace_id":"w1"},"type":"pane_created"},"event":"pane_created"}`,
			kind:      KindPaneCreated,
			pane:      "w1:p1",
			workspace: "w1",
		},
		{
			name:      "pane.closed",
			raw:       `{"data":{"pane_id":"w1:p1","type":"pane_closed","workspace_id":"w1"},"event":"pane_closed"}`,
			kind:      KindPaneClosed,
			pane:      "w1:p1",
			workspace: "w1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev, err := Normalize(json.RawMessage(tt.raw))
			if err != nil {
				t.Fatalf("Normalize: %v", err)
			}
			if ev.Kind != tt.kind {
				t.Errorf("Kind = %q, want %q", ev.Kind, tt.kind)
			}
			if ev.PaneID != tt.pane {
				t.Errorf("PaneID = %q, want %q", ev.PaneID, tt.pane)
			}
			if ev.WorkspaceID != tt.workspace {
				t.Errorf("WorkspaceID = %q, want %q", ev.WorkspaceID, tt.workspace)
			}
			if ev.TabID != tt.tab {
				t.Errorf("TabID = %q, want %q", ev.TabID, tt.tab)
			}
			if ev.Agent != "" || ev.NewState != "" {
				t.Errorf("unexpected fields populated: %+v", ev)
			}
		})
	}
}

func TestNormalizeTabEvents(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		kind      Kind
		tab       string
		workspace string
		label     string
	}{
		{
			name:      "tab.created with nested tab object",
			raw:       `{"data":{"tab":{"agent_status":"working","focused":true,"label":"herdr-scribe","number":2,"pane_count":1,"tab_id":"w1:t1","workspace_id":"w1"},"type":"tab_created"},"event":"tab_created"}`,
			kind:      KindTabCreated,
			tab:       "w1:t1",
			workspace: "w1",
			label:     "herdr-scribe",
		},
		{
			name:      "tab.closed flat",
			raw:       `{"data":{"tab_id":"w1:t1","type":"tab_closed","workspace_id":"w1"},"event":"tab_closed"}`,
			kind:      KindTabClosed,
			tab:       "w1:t1",
			workspace: "w1",
		},
		{
			name:      "tab.renamed flat with label",
			raw:       `{"data":{"label":"agents","tab_id":"w1:t1","type":"tab_renamed","workspace_id":"w1"},"event":"tab_renamed"}`,
			kind:      KindTabRenamed,
			tab:       "w1:t1",
			workspace: "w1",
			label:     "agents",
		},
		{
			name:      "tab.created dotted wire name",
			raw:       `{"data":{"tab":{"label":"~","pane_count":0,"tab_id":"w1:t2","workspace_id":"w1"},"type":"tab_created"},"event":"tab.created"}`,
			kind:      KindTabCreated,
			tab:       "w1:t2",
			workspace: "w1",
			label:     "~",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev, err := Normalize(json.RawMessage(tt.raw))
			if err != nil {
				t.Fatalf("Normalize: %v", err)
			}
			if ev.Kind != tt.kind {
				t.Errorf("Kind = %q, want %q", ev.Kind, tt.kind)
			}
			if ev.TabID != tt.tab {
				t.Errorf("TabID = %q, want %q", ev.TabID, tt.tab)
			}
			if ev.WorkspaceID != tt.workspace {
				t.Errorf("WorkspaceID = %q, want %q", ev.WorkspaceID, tt.workspace)
			}
			if ev.Label != tt.label {
				t.Errorf("Label = %q, want %q", ev.Label, tt.label)
			}
			if ev.PaneID != "" || ev.Agent != "" || ev.NewState != "" {
				t.Errorf("unexpected fields populated: %+v", ev)
			}
		})
	}
}

func TestNormalizeAgentDetected(t *testing.T) {
	raw := `{"data":{"agent":"codex","pane_id":"w1:p1","type":"pane_agent_detected","workspace_id":"w1"},"event":"pane_agent_detected"}`
	ev, err := Normalize(json.RawMessage(raw))
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if ev.Kind != KindAgentDetected {
		t.Errorf("Kind = %q, want %q", ev.Kind, KindAgentDetected)
	}
	if ev.Agent != "codex" {
		t.Errorf("Agent = %q, want %q", ev.Agent, "codex")
	}
	if ev.PaneID != "w1:p1" || ev.WorkspaceID != "w1" {
		t.Errorf("PaneID/WorkspaceID = %q/%q, want w1:p1/w1", ev.PaneID, ev.WorkspaceID)
	}
	if ev.TabID != "" || ev.NewState != "" {
		t.Errorf("unexpected fields populated: %+v", ev)
	}
}

func TestNormalizeAgentStatusChanged(t *testing.T) {
	tests := []struct {
		status snapshot.AgentStatus
	}{
		{snapshot.AgentStatusIdle},
		{snapshot.AgentStatusWorking},
		{snapshot.AgentStatusBlocked},
		{snapshot.AgentStatusDone},
		{snapshot.AgentStatusUnknown},
	}

	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			raw := `{"data":{"agent":"codex","agent_status":"` + string(tt.status) + `","pane_id":"w1:p1","workspace_id":"w1"},"event":"pane.agent_status_changed"}`

			ev, err := Normalize(json.RawMessage(raw))
			if err != nil {
				t.Fatalf("Normalize: %v", err)
			}
			if ev.Kind != KindAgentStatusChanged {
				t.Errorf("Kind = %q, want %q", ev.Kind, KindAgentStatusChanged)
			}
			if ev.Agent != "codex" {
				t.Errorf("Agent = %q, want %q", ev.Agent, "codex")
			}
			if ev.NewState != tt.status {
				t.Errorf("NewState = %q, want %q", ev.NewState, tt.status)
			}
		})
	}
}

func TestNormalizeAgentStatusChangedUnderscoreName(t *testing.T) {
	raw := `{"data":{"agent":"codex","agent_status":"working","pane_id":"w1:p1","workspace_id":"w1","type":"pane_agent_status_changed"},"event":"pane_agent_status_changed"}`
	ev, err := Normalize(json.RawMessage(raw))
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if ev.Kind != KindAgentStatusChanged {
		t.Errorf("Kind = %q, want %q", ev.Kind, KindAgentStatusChanged)
	}
	if ev.NewState != snapshot.AgentStatusWorking {
		t.Errorf("NewState = %q, want %q", ev.NewState, snapshot.AgentStatusWorking)
	}
}

func TestNormalizeUnknownStatusValue(t *testing.T) {
	raw := `{"data":{"agent":"codex","agent_status":"flying","pane_id":"w1:p1","workspace_id":"w1"},"event":"pane.agent_status_changed"}`
	ev, err := Normalize(json.RawMessage(raw))
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if ev.NewState != snapshot.AgentStatus("flying") {
		t.Errorf("NewState = %q, want %q", ev.NewState, snapshot.AgentStatus("flying"))
	}
}

func TestNormalizeMissingAgentStatus(t *testing.T) {
	raw := `{"data":{"agent":"codex","pane_id":"w1:p1","workspace_id":"w1"},"event":"pane.agent_status_changed"}`
	ev, err := Normalize(json.RawMessage(raw))
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if ev.Kind != KindAgentStatusChanged {
		t.Errorf("Kind = %q, want %q", ev.Kind, KindAgentStatusChanged)
	}
	if ev.NewState != "" {
		t.Errorf("NewState = %q, want empty", ev.NewState)
	}
}

func TestNormalizeToleratesExtraFields(t *testing.T) {
	raw := `{"data":{"agent":"codex","agent_status":"blocked","pane_id":"w1:p1","workspace_id":"w1","tab_id":"w1:t1","revision":42,"state_change_seq":7,"display_agent":"Codex CLI","display_agent_status":"Blocked"},"event":"pane.agent_status_changed"}`
	ev, err := Normalize(json.RawMessage(raw))
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if ev.NewState != snapshot.AgentStatusBlocked {
		t.Errorf("NewState = %q, want %q", ev.NewState, snapshot.AgentStatusBlocked)
	}
	if ev.Agent != "codex" || ev.PaneID != "w1:p1" || ev.WorkspaceID != "w1" {
		t.Errorf("unexpected ids: %+v", ev)
	}
}

func TestNormalizeUnknownType(t *testing.T) {
	raw := `{"data":{"pane_id":"w1:p1","type":"pane_scroll_changed"},"event":"pane.scroll_changed"}`
	if _, err := Normalize(json.RawMessage(raw)); err == nil {
		t.Fatal("expected error for unknown event type")
	}
}

func TestNormalizeInvalidJSON(t *testing.T) {
	for _, raw := range []string{"", "not json", `{"type":`} {
		if _, err := Normalize(json.RawMessage(raw)); err == nil {
			t.Errorf("Normalize(%q): expected error", raw)
		}
	}
}

func TestNormalizeRejectsAck(t *testing.T) {
	for _, raw := range []string{
		`{"id":"herdr-scribe","result":{"ok":true}}`,
		`{"id":"herdr-scribe","error":{"code":"MethodNotFound","message":"nope"}}`,
	} {
		if _, err := Normalize(json.RawMessage(raw)); err == nil {
			t.Errorf("Normalize(%q): expected error for non-event frame", raw)
		}
	}
}

func TestNormalizePreservesRaw(t *testing.T) {
	raw := `{"data":{"pane":{"pane_id":"w1:p1","workspace_id":"w1","tab_id":"w1:t1"},"type":"pane_created"},"event":"pane_created"}`
	ev, err := Normalize(json.RawMessage(raw))
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if string(ev.Raw) != raw {
		t.Errorf("Raw = %q, want %q", ev.Raw, raw)
	}
}

func TestNormalizeErrorIdentifiesEvent(t *testing.T) {
	raw := `{"data":{"pane_id":"w1:p1"},"event":"pane.scroll_changed"}`
	_, err := Normalize(json.RawMessage(raw))
	if err == nil {
		t.Fatal("expected error")
	}
	if want := "pane.scroll_changed"; !strings.Contains(err.Error(), want) {
		t.Errorf("error %q should identify event type %q", err.Error(), want)
	}
}

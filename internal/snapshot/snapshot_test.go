package snapshot

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// liveSnapshotJSON is the full wire shape of a real session.snapshot. The
// parse structs deliberately capture only a subset of it, so this fixture
// doubles as the regression guard for unknown-field tolerance: every field the
// tracker doesn't consume must be silently ignored, never erroring the parse.
const liveSnapshotJSON = `{"type":"session_snapshot","snapshot":{"version":"0.9.0","protocol":22,"focused_workspace_id":"w3","focused_tab_id":"w3:t1","focused_pane_id":"w3:p1","workspaces":[{"workspace_id":"w3","number":1,"label":"~","focused":true,"pane_count":1,"tab_count":1,"active_tab_id":"w3:t1","agent_status":"unknown"}],"panes":[{"pane_id":"w3:p1","terminal_id":"term_65aef03e0a82e1","workspace_id":"w3","tab_id":"w3:t1","focused":true,"cwd":"/home/nabutabu","foreground_cwd":"/home/nabutabu","terminal_title":"nabutabu@NavyaAlienware:~","terminal_title_stripped":"nabutabu@NavyaAlienware:~","agent_status":"unknown","agent_session":{"source":"herdr:opencode","agent":"opencode","kind":"id","value":"ses_4f7d2fae1c4b9a03"},"scroll":{"offset_from_bottom":0,"max_offset_from_bottom":0,"viewport_rows":24},"revision":1}],"agents":[]}}`

func TestUnmarshalLiveSnapshot(t *testing.T) {
	var resp Response
	if err := json.Unmarshal([]byte(liveSnapshotJSON), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	snap := resp.Snapshot
	if len(snap.Workspaces) != 1 {
		t.Fatalf("workspaces = %d, want 1", len(snap.Workspaces))
	}
	ws := snap.Workspaces[0]
	if ws.WorkspaceID != "w3" || ws.Label != "~" {
		t.Errorf("workspace = %+v", ws)
	}

	if len(snap.Panes) != 1 {
		t.Fatalf("panes = %d, want 1", len(snap.Panes))
	}
	pane := snap.Panes[0]
	if pane.PaneID != "w3:p1" || pane.WorkspaceID != "w3" || pane.TabID != "w3:t1" {
		t.Errorf("pane = %+v", pane)
	}
	if pane.AgentStatus != AgentStatusUnknown {
		t.Errorf("pane agent_status = %q, want unknown", pane.AgentStatus)
	}
	if sess := pane.AgentSession; sess == nil {
		t.Error("pane agent_session = nil, want populated")
	} else if sess.Source != "herdr:opencode" || sess.Agent != "opencode" {
		t.Errorf("pane agent_session = %+v", sess)
	} else if sess.Kind != AgentSessionRefKindID {
		t.Errorf("pane agent_session kind = %q, want id", sess.Kind)
	} else if sess.Value != "ses_4f7d2fae1c4b9a03" {
		t.Errorf("pane agent_session value = %q, want ses_4f7d2fae1c4b9a03", sess.Value)
	}
}

func TestUnmarshalPaneAgent(t *testing.T) {
	var pane Pane
	if err := json.Unmarshal([]byte(`{"pane_id":"w1:p1","agent":"codex","agent_status":"working"}`), &pane); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if pane.Agent == nil || *pane.Agent != "codex" {
		t.Errorf("pane agent = %v, want codex", pane.Agent)
	}
	if pane.AgentStatus != AgentStatusWorking {
		t.Errorf("pane agent_status = %q, want working", pane.AgentStatus)
	}
}

func TestUnmarshalPaneAgentSessionAbsent(t *testing.T) {
	var pane Pane
	if err := json.Unmarshal([]byte(`{"pane_id":"w1:p1","agent":"codex","agent_status":"working"}`), &pane); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if pane.AgentSession != nil {
		t.Errorf("pane agent_session = %+v, want nil when absent", pane.AgentSession)
	}
}

func TestUnmarshalTabShape(t *testing.T) {
	var resp Response
	if err := json.Unmarshal([]byte(`{"type":"session_snapshot","snapshot":{"tabs":[{"tab_id":"w3:t1","workspace_id":"w3","label":"~","number":1,"focused":true,"pane_count":1,"agent_status":"working"}]}}`), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(resp.Snapshot.Tabs) != 1 {
		t.Fatalf("tabs = %d, want 1", len(resp.Snapshot.Tabs))
	}
	tab := resp.Snapshot.Tabs[0]
	if tab.TabID != "w3:t1" || tab.WorkspaceID != "w3" || tab.Label != "~" {
		t.Errorf("tab = %+v", tab)
	}
}

// stubSockPath returns a Unix socket path short enough for macOS's 104-byte
// sockaddr_un.sun_path limit. t.TempDir() embeds the full test name and can
// alone exceed the limit on macOS runners, so use a short random-named dir.
func stubSockPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "herdr-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "h.sock")
}

func startStubServer(t *testing.T, handler func(*bufio.Reader, net.Conn) error) string {
	t.Helper()
	sockPath := stubSockPath(t)
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if err := handler(bufio.NewReader(conn), conn); err != nil {
			t.Errorf("stub server: %v", err)
		}
	}()

	return sockPath
}

func TestFetch(t *testing.T) {
	sock := startStubServer(t, func(r *bufio.Reader, conn net.Conn) error {
		line, err := r.ReadBytes('\n')
		if err != nil {
			return err
		}
		if err := json.Unmarshal(line, &struct {
			ID     string `json:"id"`
			Method string `json:"method"`
		}{}); err != nil {
			return fmt.Errorf("parsing request: %w", err)
		}
		_, err = conn.Write([]byte(`{"id":"herdr-observr","result":{"type":"session_snapshot","snapshot":{"version":"0.9.0","protocol":22,"tabs":[{"tab_id":"w1:t1","workspace_id":"w1"}]}}}` + "\n"))
		return err
	})
	t.Setenv("HERDR_SOCKET_PATH", sock)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := Fetch(ctx)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(resp.Snapshot.Tabs) != 1 || resp.Snapshot.Tabs[0].TabID != "w1:t1" {
		t.Errorf("snapshot = %+v", resp.Snapshot)
	}
}

func TestFetchUnmarshalError(t *testing.T) {
	sock := startStubServer(t, func(r *bufio.Reader, conn net.Conn) error {
		if _, err := r.ReadBytes('\n'); err != nil {
			return err
		}
		_, err := conn.Write([]byte("this is not json\n"))
		return err
	})
	t.Setenv("HERDR_SOCKET_PATH", sock)

	if _, err := Fetch(context.Background()); err == nil {
		t.Fatal("expected error for malformed response")
	}
}

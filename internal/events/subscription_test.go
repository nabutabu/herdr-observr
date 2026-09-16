package events

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/nabutabu/herdr-observr/internal/client"
)

func TestBuildParams(t *testing.T) {
	params := BuildParams([]string{"w1:p1", "w1:p2"})

	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	want := `{"subscriptions":[{"type":"workspace.created"},{"type":"workspace.closed"},{"type":"tab.created"},{"type":"tab.closed"},{"type":"tab.renamed"},{"type":"pane.created"},{"type":"pane.closed"},{"type":"pane.agent_detected"},{"type":"pane.agent_status_changed","pane_id":"w1:p1"},{"type":"pane.agent_status_changed","pane_id":"w1:p2"}]}`
	if string(raw) != want {
		t.Errorf("params JSON = %s, want %s", string(raw), want)
	}
}

func TestBuildParamsGlobalEvents(t *testing.T) {
	params := BuildParams(nil)

	subs := params["subscriptions"].([]Subscription)
	wantGlobal := []SubscriptionType{
		SubscribeWorkspaceCreated,
		SubscribeWorkspaceClosed,
		SubscribeTabCreated,
		SubscribeTabClosed,
		SubscribeTabRenamed,
		SubscribePaneCreated,
		SubscribePaneClosed,
		SubscribePaneAgentDetected,
	}

	seen := make(map[SubscriptionType]int)
	for _, s := range subs {
		seen[s.Type]++
	}

	if len(subs) != len(wantGlobal) {
		t.Fatalf("subscriptions = %d, want %d", len(subs), len(wantGlobal))
	}
	for _, typ := range wantGlobal {
		if seen[typ] != 1 {
			t.Errorf("subscription %q count = %d, want 1", typ, seen[typ])
		}
	}
}

func TestBuildParamsPaneScoped(t *testing.T) {
	params := BuildParams([]string{"w1:p1", "w1:p2", "w1:p3"})

	subs := params["subscriptions"].([]Subscription)
	var scoped []Subscription
	for _, s := range subs {
		if s.Type == SubscribePaneAgentStatusChanged {
			scoped = append(scoped, s)
		}
	}

	if len(scoped) != 3 {
		t.Fatalf("agent_status_changed subscriptions = %d, want 3", len(scoped))
	}

	wantPanes := []string{"w1:p1", "w1:p2", "w1:p3"}
	for i, s := range scoped {
		if s.PaneID != wantPanes[i] {
			t.Errorf("subscription %d pane_id = %q, want %q", i, s.PaneID, wantPanes[i])
		}
	}
}

func TestBuildParamsNoPanesSkipsScoped(t *testing.T) {
	params := BuildParams(nil)

	subs := params["subscriptions"].([]Subscription)
	for _, s := range subs {
		if s.Type == SubscribePaneAgentStatusChanged {
			t.Errorf("unexpected agent_status_changed subscription: %+v", s)
		}
		if s.PaneID != "" {
			t.Errorf("subscription %q has unexpected pane_id: %+v", s.Type, s)
		}
	}
}

func TestBuildParamsExcludesNoise(t *testing.T) {
	for _, paneIDs := range [][]string{nil, {"w1:p1"}} {
		params := BuildParams(paneIDs)
		subs := params["subscriptions"].([]Subscription)
		for _, s := range subs {
			switch s.Type {
			case "pane.scroll_changed", "pane.output_matched", "pane.updated":
				t.Errorf("noise event subscribed: %q", s.Type)
			}
		}
	}
}

func TestSubscribeFromSnapshotReturnsSubscribedPaneIDs(t *testing.T) {
	const snapshotBody = `{"type":"session_snapshot","snapshot":{"workspaces":[{"workspace_id":"w1"}],"tabs":[{"tab_id":"w1:t1","workspace_id":"w1","label":"agents"}],"panes":[{"pane_id":"w1:p1","workspace_id":"w1","tab_id":"w1:t1"},{"pane_id":"w1:p2","workspace_id":"w1","tab_id":"w1:t1"}]}}`

	sockPath := filepath.Join(t.TempDir(), "herdr-subscribe-from-snapshot.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	// One connection for the session.snapshot RPC, one for the subscription.
	errCh := make(chan error, 2)
	go func() {
		for i := 0; i < 2; i++ {
			conn, err := ln.Accept()
			if err != nil {
				errCh <- err
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				r := bufio.NewReader(conn)
				line, err := r.ReadBytes('\n')
				if err != nil {
					errCh <- fmt.Errorf("reading request: %w", err)
					return
				}
				var req client.Request
				if err := json.Unmarshal(line, &req); err != nil {
					errCh <- fmt.Errorf("parsing request %q: %w", string(line), err)
					return
				}
				switch req.Method {
				case "session.snapshot":
					fmt.Fprintf(conn, `{"id":"herdr-observr","result":%s}`+"\n", snapshotBody)
				case "events.subscribe":
					if err := compareSubscriptions(req.Params, BuildParams([]string{"w1:p1", "w1:p2"})); err != nil {
						errCh <- err
						return
					}
					fmt.Fprintf(conn, `{"id":"herdr-observr","result":{"ok":true}}`+"\n")
				default:
					errCh <- fmt.Errorf("unexpected method %q", req.Method)
				}
				errCh <- nil
			}(conn)
		}
	}()

	setSocketPath(t, sockPath)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	paneIDs, sub, resp, ok := SubscribeFromSnapshot(ctx)
	if !ok {
		t.Fatal("SubscribeFromSnapshot failed")
	}
	defer sub.Close()

	if !reflect.DeepEqual(paneIDs, []string{"w1:p1", "w1:p2"}) {
		t.Errorf("paneIDs = %v, want [w1:p1 w1:p2]", paneIDs)
	}
	if len(resp.Snapshot.Panes) != 2 || resp.Snapshot.Panes[0].PaneID != "w1:p1" {
		t.Errorf("resp.Snapshot.Panes = %+v, want 2 panes starting with w1:p1", resp.Snapshot.Panes)
	}

	for i := 0; i < 2; i++ {
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("stub server: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("stub server did not finish")
		}
	}
}

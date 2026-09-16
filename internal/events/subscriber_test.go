package events

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/nabutabu/herdr-observr/internal/client"
	"github.com/nabutabu/herdr-observr/internal/snapshot"
)

func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}

// compareSubscriptions compares the subscription payloads semantically,
// ignoring JSON key ordering differences between struct-marshalled params and
// received-then-remarshalled params.
func compareSubscriptions(got, want map[string]any) error {
	var gotSubs, wantSubs []Subscription
	gotRaw, err := json.Marshal(got["subscriptions"])
	if err != nil {
		return fmt.Errorf("marshalling received subscriptions: %w", err)
	}
	wantRaw, err := json.Marshal(want["subscriptions"])
	if err != nil {
		return fmt.Errorf("marshalling wanted subscriptions: %w", err)
	}
	if err := json.Unmarshal(gotRaw, &gotSubs); err != nil {
		return fmt.Errorf("unmarshalling received subscriptions: %w", err)
	}
	if err := json.Unmarshal(wantRaw, &wantSubs); err != nil {
		return fmt.Errorf("unmarshalling wanted subscriptions: %w", err)
	}
	if !reflect.DeepEqual(gotSubs, wantSubs) {
		return fmt.Errorf("subscriptions = %+v, want %+v", gotSubs, wantSubs)
	}
	return nil
}

func startStubServer(t *testing.T, handler func(*bufio.Reader, net.Conn) error) (string, <-chan error) {
	t.Helper()
	sockPath := filepath.Join(t.TempDir(), "herdr-events-test.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	errCh := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()
		errCh <- handler(bufio.NewReader(conn), conn)
	}()

	return sockPath, errCh
}

func startEventsSubscriber(t *testing.T, params map[string]any) *Subscriber {
	t.Helper()
	sub, err := NewSubscriber(params)
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	t.Cleanup(func() { sub.Close() })
	return sub
}

func setSocketPath(t *testing.T, path string) {
	t.Helper()
	t.Setenv("HERDR_SOCKET_PATH", path)
}

func readSubscribeFrame(r *bufio.Reader) (*client.Request, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("reading subscribe frame: %w", err)
	}
	var req client.Request
	if err := json.Unmarshal(line, &req); err != nil {
		return nil, fmt.Errorf("parsing subscribe frame %q: %w", string(line), err)
	}
	if req.ID != "herdr-observr" || req.Method != "events.subscribe" {
		return nil, fmt.Errorf("unexpected subscribe frame: %+v", req)
	}
	return &req, nil
}

func TestSubscribeWritesSingleFrame(t *testing.T) {
	sock, errCh := startStubServer(t, func(r *bufio.Reader, conn net.Conn) error {
		req, err := readSubscribeFrame(r)
		if err != nil {
			return err
		}

		if err := compareSubscriptions(req.Params, BuildParams(nil)); err != nil {
			return err
		}

		// No writes may follow the subscribe frame on this connection: a read
		// that returns data (rather than EOF/deadline) is a violation.
		conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		if extra, err := r.ReadBytes('\n'); err == nil {
			return fmt.Errorf("unexpected client write after subscribe: %q", extra)
		}
		return nil
	})
	setSocketPath(t, sock)

	sub := startEventsSubscriber(t, BuildParams(nil))
	if err := sub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("stub server: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stub server did not finish")
	}
}

func TestSubscribePaneScopedParams(t *testing.T) {
	sock, errCh := startStubServer(t, func(r *bufio.Reader, conn net.Conn) error {
		req, err := readSubscribeFrame(r)
		if err != nil {
			return err
		}

		if err := compareSubscriptions(req.Params, BuildParams([]string{"w1:p1", "w1:p2"})); err != nil {
			return err
		}
		return nil
	})
	setSocketPath(t, sock)

	startEventsSubscriber(t, BuildParams([]string{"w1:p1", "w1:p2"})).Close()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("stub server: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stub server did not finish")
	}
}

func TestSubscribeDeliversNormalizedEvents(t *testing.T) {
	events := []struct {
		raw   string
		kind  Kind
		pane  string
		tab   string
		agent string
		state snapshot.AgentStatus
	}{
		{
			raw:   `{"data":{"type":"workspace_created","workspace":{"active_tab_id":"wS:t1","agent_status":"unknown","focused":true,"label":"herdr-observr","number":2,"pane_count":1,"tab_count":1,"workspace_id":"wS"}},"event":"workspace_created"}`,
			kind:  KindWorkspaceCreated,
			pane:  "",
			tab:   "",
			agent: "",
			state: "",
		},
		{
			raw:   `{"data":{"pane":{"pane_id":"w1:p1","workspace_id":"w1","tab_id":"w1:t1"},"type":"pane_created"},"event":"pane_created"}`,
			kind:  KindPaneCreated,
			pane:  "w1:p1",
			tab:   "w1:t1",
			agent: "",
			state: "",
		},
		{
			raw:   `{"data":{"agent":"codex","pane_id":"w1:p1","type":"pane_agent_detected","workspace_id":"w1"},"event":"pane_agent_detected"}`,
			kind:  KindAgentDetected,
			pane:  "w1:p1",
			tab:   "",
			agent: "codex",
			state: "",
		},
		{
			raw:   `{"data":{"agent":"codex","agent_status":"working","pane_id":"w1:p1","workspace_id":"w1"},"event":"pane.agent_status_changed"}`,
			kind:  KindAgentStatusChanged,
			pane:  "w1:p1",
			tab:   "",
			agent: "codex",
			state: snapshot.AgentStatusWorking,
		},
	}

	sock, _ := startStubServer(t, func(r *bufio.Reader, conn net.Conn) error {
		if _, err := readSubscribeFrame(r); err != nil {
			return err
		}
		for _, e := range events {
			if _, err := fmt.Fprintf(conn, "%s\n", e.raw); err != nil {
				return err
			}
		}
		return nil
	})
	setSocketPath(t, sock)

	sub := startEventsSubscriber(t, BuildParams(nil))
	for i, want := range events {
		select {
		case got, ok := <-sub.Events():
			if !ok {
				t.Fatalf("events channel closed before event %d", i)
			}
			if got.Kind != want.kind {
				t.Errorf("event %d Kind = %q, want %q", i, got.Kind, want.kind)
			}
			if got.PaneID != want.pane {
				t.Errorf("event %d PaneID = %q, want %q", i, got.PaneID, want.pane)
			}
			if got.TabID != want.tab {
				t.Errorf("event %d TabID = %q, want %q", i, got.TabID, want.tab)
			}
			if got.Agent != want.agent {
				t.Errorf("event %d Agent = %q, want %q", i, got.Agent, want.agent)
			}
			if got.NewState != want.state {
				t.Errorf("event %d NewState = %q, want %q", i, got.NewState, want.state)
			}
			if string(got.Raw) != want.raw {
				t.Errorf("event %d Raw = %q, want %q", i, got.Raw, want.raw)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for event %d", i)
		}
	}
}

func TestSubscribeDropsAck(t *testing.T) {
	const ev = `{"data":{"pane_id":"w1:p1","type":"pane_created","workspace_id":"w1"},"event":"pane_created"}`

	sock, _ := startStubServer(t, func(r *bufio.Reader, conn net.Conn) error {
		if _, err := readSubscribeFrame(r); err != nil {
			return err
		}
		for _, frame := range []string{
			`{"id":"herdr-observr","result":{"ok":true}}`,
			ev,
		} {
			if _, err := fmt.Fprintf(conn, "%s\n", frame); err != nil {
				return err
			}
		}
		return nil
	})
	setSocketPath(t, sock)

	sub := startEventsSubscriber(t, BuildParams(nil))
	select {
	case got, ok := <-sub.Events():
		if !ok {
			t.Fatal("events channel closed before the real event")
		}
		if got.Kind != KindPaneCreated || got.PaneID != "w1:p1" {
			t.Errorf("event = %+v, want pane.created for w1:p1", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event after ack")
	}

	// The ack must not be delivered; nothing but the ack may follow.
	select {
	case _, ok := <-sub.Events():
		if ok {
			t.Error("unexpected extra event after ack was dropped")
		}
	case <-time.After(150 * time.Millisecond):
	}
}

func TestSubscribeSkipsMalformed(t *testing.T) {
	const ev = `{"data":{"pane_id":"w1:p1","type":"pane_closed","workspace_id":"w1"},"event":"pane_closed"}`

	sock, _ := startStubServer(t, func(r *bufio.Reader, conn net.Conn) error {
		if _, err := readSubscribeFrame(r); err != nil {
			return err
		}
		for _, frame := range []string{
			"this is not json at all",
			`{"unrelated":true}`,
			ev,
		} {
			if _, err := fmt.Fprintf(conn, "%s\n", frame); err != nil {
				return err
			}
		}
		return nil
	})
	setSocketPath(t, sock)

	sub := startEventsSubscriber(t, BuildParams(nil))
	select {
	case got, ok := <-sub.Events():
		if !ok {
			t.Fatal("events channel closed before the real event")
		}
		if got.Kind != KindPaneClosed || got.PaneID != "w1:p1" {
			t.Errorf("event = %+v, want pane.closed for w1:p1", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event after malformed frames")
	}
}

func TestSubscribeSurfacesEOF(t *testing.T) {
	sock, _ := startStubServer(t, func(r *bufio.Reader, conn net.Conn) error {
		if _, err := readSubscribeFrame(r); err != nil {
			return err
		}
		// Close the connection without pushing anything.
		return nil
	})
	setSocketPath(t, sock)

	sub := startEventsSubscriber(t, BuildParams(nil))
	select {
	case err := <-sub.Err():
		if err == nil {
			t.Fatal("expected a stream error after EOF")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stream error")
	}

	if _, ok := <-sub.Events(); ok {
		t.Error("events channel still open after stream error")
	}
}

func TestSubscribeCloseIdempotent(t *testing.T) {
	sock, errCh := startStubServer(t, func(r *bufio.Reader, conn net.Conn) error {
		if _, err := readSubscribeFrame(r); err != nil {
			return err
		}
		// Keep the connection open until the client closes it.
		buf := make([]byte, 1)
		for {
			if _, err := conn.Read(buf); err != nil {
				return nil
			}
		}
	})
	setSocketPath(t, sock)

	sub := startEventsSubscriber(t, BuildParams(nil))
	if err := sub.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	select {
	case <-sub.done:
	case <-time.After(2 * time.Second):
		t.Fatal("reader goroutine did not exit after Close")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("stub server: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stub server did not finish")
	}
}

func TestParseFrame(t *testing.T) {
	event := json.RawMessage(`{"data":{"pane":{"pane_id":"w1:p1","workspace_id":"w1","tab_id":"w1:t1"},"type":"pane_created"},"event":"pane_created"}`)
	kind, ev, err := parseFrame(event)
	if err != nil {
		t.Fatalf("parseFrame(event): %v", err)
	}
	if kind != frameEvent {
		t.Errorf("kind = %v, want frameEvent", kind)
	}
	if ev.Kind != KindPaneCreated || ev.PaneID != "w1:p1" {
		t.Errorf("ev = %+v, want pane.created for w1:p1", ev)
	}

	ack := json.RawMessage(`{"id":"herdr-observr","result":{"ok":true}}`)
	if kind, _, err := parseFrame(ack); err != nil || kind != frameAck {
		t.Errorf("parseFrame(ack) = (%v, %v), want (frameAck, nil)", kind, err)
	}

	errResp := json.RawMessage(`{"id":"herdr-observr","error":{"code":"MethodNotFound","message":"nope"}}`)
	if kind, _, err := parseFrame(errResp); err != nil || kind != frameAck {
		t.Errorf("parseFrame(error response) = (%v, %v), want (frameAck, nil)", kind, err)
	}

	unknownType := json.RawMessage(`{"data":{"pane_id":"w1:p1"},"event":"pane.scroll_changed"}`)
	kind, _, err = parseFrame(unknownType)
	if kind != frameEvent {
		t.Errorf("kind = %v, want frameEvent", kind)
	}
	if err == nil {
		t.Error("parseFrame(unknown type): expected error")
	}

	unknown := json.RawMessage(`{"unrelated":1}`)
	if kind, _, err := parseFrame(unknown); err != nil || kind != frameUnknown {
		t.Errorf("parseFrame(unknown) = (%v, %v), want (frameUnknown, nil)", kind, err)
	}

	garbage := json.RawMessage(`not json`)
	if kind, _, err := parseFrame(garbage); err == nil || kind != frameUnknown {
		t.Errorf("parseFrame(garbage) = (%v, %v), want (frameUnknown, error)", kind, err)
	}
}

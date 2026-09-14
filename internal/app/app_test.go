package app

import (
	"testing"

	"github.com/nabutabu/herdr-scribe/internal/events"
	"github.com/nabutabu/herdr-scribe/internal/tracker"
)

func TestPaneCreatedNeedsResubscribe(t *testing.T) {
	a := &App{scope: newScope([]string{"w1:p1"})}

	cases := []struct {
		name string
		ev   events.NormalizedEvent
		want bool
	}{
		{name: "new pane", ev: events.NormalizedEvent{Kind: events.KindPaneCreated, PaneID: "w1:p2"}, want: true},
		{name: "already subscribed pane", ev: events.NormalizedEvent{Kind: events.KindPaneCreated, PaneID: "w1:p1"}, want: false},
		{name: "empty pane id", ev: events.NormalizedEvent{Kind: events.KindPaneCreated}, want: true},
		{name: "workspace event ignores pane id", ev: events.NormalizedEvent{Kind: events.KindWorkspaceCreated, PaneID: "w1:p9"}, want: false},
		{name: "tab event ignores pane id", ev: events.NormalizedEvent{Kind: events.KindTabCreated, PaneID: "w1:p9", TabID: "w1:t1"}, want: false},
		{name: "status change not resubscribe-worthy", ev: events.NormalizedEvent{Kind: events.KindAgentStatusChanged, PaneID: "w1:p2"}, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := a.paneCreatedNeedsResubscribe(tc.ev); got != tc.want {
				t.Errorf("paneCreatedNeedsResubscribe(%+v) = %v, want %v", tc.ev, got, tc.want)
			}
		})
	}

	nilScope := &App{}
	if got := nilScope.paneCreatedNeedsResubscribe(events.NormalizedEvent{Kind: events.KindPaneCreated, PaneID: "w1:p1"}); !got {
		t.Error("nil scope must report every pane.created as needing a resubscribe")
	}
}

func TestScope(t *testing.T) {
	s := newScope([]string{"w1:p1", "w2:p1"})
	if len(s.subscribedPanes) != 2 {
		t.Fatalf("subscribedPanes = %v, want 2 panes", s.subscribedPanes)
	}
	for _, want := range []string{"w1:p1", "w2:p1"} {
		if !s.subscribed(want) {
			t.Errorf("scope not subscribed to %q: %v", want, s.subscribedPanes)
		}
	}
	if s.subscribed("w1:p9") {
		t.Errorf("scope unexpectedly subscribed to w1:p9: %v", s.subscribedPanes)
	}
	if newScope(nil).subscribed("w1:p1") {
		t.Error("empty scope must not cover any pane")
	}
}

func TestSignalResubscribeCoalesces(t *testing.T) {
	ch := make(chan struct{}, 1)
	signalResubscribe(ch)
	signalResubscribe(ch)
	select {
	case <-ch:
	default:
		t.Fatal("expected one pending signal")
	}
	select {
	case <-ch:
		t.Fatal("signals must coalesce into the single-buffer channel")
	default:
	}
}

func TestHandleEventRoutesTabKinds(t *testing.T) {
	a := &App{tr: tracker.NewTracker()}

	a.handleEvent(events.NormalizedEvent{Kind: events.KindTabCreated, TabID: "w1:t1", WorkspaceID: "w1", Label: "herdr-scribe"})
	a.handleEvent(events.NormalizedEvent{Kind: events.KindTabRenamed, TabID: "w1:t1", Label: "agents"})
	a.handleEvent(events.NormalizedEvent{Kind: events.KindTabClosed, TabID: "w1:t1", WorkspaceID: "w1"})

	// After the close the tab is retained in its grace window (2.7) until the
	// app's evict ticker drops it; eviction itself is covered by the tracker
	// tests with a controllable clock, since ClosedAt is tracker-internal.
	if got := len(a.tr.Tabs()); got != 1 {
		t.Errorf("tab after full lifecycle = %d entries, want 1 (retained during grace)", got)
	}
}

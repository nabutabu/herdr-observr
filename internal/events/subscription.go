package events

import (
	"context"
	"log/slog"

	"github.com/nabutabu/herdr-scribe/internal/snapshot"
)

// SubscriptionType is the event name Herdr accepts in an events.subscribe
// request (dotted form, e.g. "pane.created"), distinct from the underscore
// wire name pushed back on the event stream.
type SubscriptionType string

const (
	SubscribeWorkspaceCreated       SubscriptionType = "workspace.created"
	SubscribeWorkspaceClosed        SubscriptionType = "workspace.closed"
	SubscribeTabCreated             SubscriptionType = "tab.created"
	SubscribeTabClosed              SubscriptionType = "tab.closed"
	SubscribeTabRenamed             SubscriptionType = "tab.renamed"
	SubscribePaneCreated            SubscriptionType = "pane.created"
	SubscribePaneClosed             SubscriptionType = "pane.closed"
	SubscribePaneAgentDetected      SubscriptionType = "pane.agent_detected"
	SubscribePaneAgentStatusChanged SubscriptionType = "pane.agent_status_changed"
)

type Subscription struct {
	Type   SubscriptionType `json:"type"`
	PaneID string           `json:"pane_id,omitempty"`
}

// BuildParams builds the events.subscribe params for a subscription scoped to
// the given pane IDs. workspace/tab/pane lifecycle events are kind-scoped (no
// pane_id): every workspace, tab, and pane matches, including ones created
// after this subscribe. Only pane.agent_status_changed is per-pane — herdr
// requires an existing pane_id and probes it at subscribe time, so a pane not
// yet created can never be pre-subscribed. BuildParams takes the pane IDs to
// cover at this instant; callers fold new panes in only by re-subscribing.
func BuildParams(paneIDs []string) map[string]any {
	subscriptions := []Subscription{
		{Type: SubscribeWorkspaceCreated},
		{Type: SubscribeWorkspaceClosed},
		{Type: SubscribeTabCreated},
		{Type: SubscribeTabClosed},
		{Type: SubscribeTabRenamed},
		{Type: SubscribePaneCreated},
		{Type: SubscribePaneClosed},
		{Type: SubscribePaneAgentDetected},
	}
	for _, paneID := range paneIDs {
		subscriptions = append(subscriptions, Subscription{Type: SubscribePaneAgentStatusChanged, PaneID: paneID})
	}

	return map[string]any{"subscriptions": subscriptions}
}

// SubscribeFromSnapshot fetches a fresh session.snapshot (returned to the
// caller for bootstrapping the tracker), computes the pane IDs it will scope
// pane.agent_status_changed to, and establishes the subscription. paneIDs is
// that authoritative cover set — the exact list BuildParams serialized, derived
// from the returned snapshot. The caller owns Close() on sub; on failure sub is
// nil and ok is false.
func SubscribeFromSnapshot(ctx context.Context) (paneIDs []string, sub *Subscriber, resp snapshot.Response, ok bool) {
	resp, err := snapshot.Fetch(ctx)
	if err != nil {
		slog.Error("session snapshot failed", "error", err)
		return nil, nil, snapshot.Response{}, false
	}

	paneIDs = make([]string, 0, len(resp.Snapshot.Panes))
	for _, pane := range resp.Snapshot.Panes {
		paneIDs = append(paneIDs, pane.PaneID)
	}
	slog.Info("subscribing from session snapshot", "pane_count", len(paneIDs), "pane_ids", paneIDs)

	sub, err = NewSubscriber(BuildParams(paneIDs))
	if err != nil {
		slog.Error("subscribe failed", "error", err)
		return nil, nil, snapshot.Response{}, false
	}
	return paneIDs, sub, resp, true
}

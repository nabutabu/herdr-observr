package tracker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nabutabu/herdr-observr/internal/snapshot"
)

// Drift is one observed difference between tracked state and a snapshot.
type Drift struct {
	Kind    string // "workspace" | "tab" | "pane"
	ID      string
	Tracked string // "" when absent from the tracker
	Actual  string // "" when absent from the snapshot
}

// SeenFlip is a pane whose tracked status was `done` but the fresh snapshot
// reports `idle` — the silent "the user looked at it" transition (2.4). Herdr
// pushes no event for it: pane.seen is flipped by focus/navigation without a
// PaneStateUpdate (herdr src/app/actions.rs mark_active_tab_seen), so the
// status only reverts to idle inside session.snapshot. Reconciliation is the
// sole detector. Unlike a Drift, a SeenFlip is benign — it must not tear down
// a healthy subscription.
type SeenFlip struct {
	PaneID      string
	WorkspaceID string
}

type DiffReport struct {
	Drifts    []Drift
	SeenFlips []SeenFlip
}

func (r DiffReport) Drifted() bool { return len(r.Drifts) > 0 }

func (r DiffReport) Empty() bool { return len(r.Drifts) == 0 && len(r.SeenFlips) == 0 }

// Diff compares tracked state against a fresh snapshot without mutating.
// Workspaces, tabs, and panes participate. Tabs are compared on membership
// only: a missed tab.updated/rename, label, or focus change is metadata churn
// that self-heals at the next re-baseline and is deliberately not a drift.
// Membership drift is load-bearing now that tab.created/tab.closed are
// subscribed (a missed tab.close event is a genuine event gap, same as a
// missed pane.close), so a tab present in exactly one side is reported.
func (t *Tracker) Diff(snap snapshot.Snapshot) DiffReport {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var drifts []Drift
	var flips []SeenFlip

	for id := range t.workspaces {
		if !hasWorkspace(snap.Workspaces, id) {
			drifts = append(drifts, Drift{Kind: "workspace", ID: id, Tracked: "present"})
		}
	}
	for _, ws := range snap.Workspaces {
		if _, ok := t.workspaces[ws.WorkspaceID]; !ok {
			drifts = append(drifts, Drift{Kind: "workspace", ID: ws.WorkspaceID, Actual: "present"})
		}
	}

	snapTabs := make(map[string]struct{}, len(snap.Tabs))
	for _, tab := range snap.Tabs {
		snapTabs[tab.TabID] = struct{}{}
	}
	for id := range t.tabs {
		if !t.tabs[id].ClosedAt.IsZero() {
			continue // in close grace window (2.7): intentionally absent from live tracking
		}
		if _, ok := snapTabs[id]; !ok {
			drifts = append(drifts, Drift{Kind: "tab", ID: id, Tracked: "present"})
		}
	}
	for _, tab := range snap.Tabs {
		if _, ok := t.tabs[tab.TabID]; !ok {
			drifts = append(drifts, Drift{Kind: "tab", ID: tab.TabID, Actual: "present"})
		}
	}

	snapPanes := make(map[string]snapshot.Pane, len(snap.Panes))
	for _, pane := range snap.Panes {
		snapPanes[pane.PaneID] = pane
	}

	for id, pane := range t.panes {
		if !pane.ClosedAt.IsZero() {
			continue // in close grace window (2.7): intentionally absent from live tracking
		}
		sp, ok := snapPanes[id]
		if !ok {
			drifts = append(drifts, Drift{Kind: "pane", ID: id, Tracked: describePaneTracked(pane)})
			continue
		}
		// Tracked done + snapshot idle is the silent seen-flip, not a drift:
		// the stream is healthy, the user just looked. Any other mismatch is
		// a genuine missing-event gap -> resubscribe.
		if pane.Status == snapshot.AgentStatusDone && sp.AgentStatus == snapshot.AgentStatusIdle {
			flips = append(flips, SeenFlip{PaneID: id, WorkspaceID: sp.WorkspaceID})
			continue
		}
		tracked := describePaneTracked(pane)
		actual := describePaneSnapshot(sp)
		if tracked != actual {
			drifts = append(drifts, Drift{Kind: "pane", ID: id, Tracked: tracked, Actual: actual})
		}
	}
	for id, sp := range snapPanes {
		if _, ok := t.panes[id]; !ok {
			drifts = append(drifts, Drift{Kind: "pane", ID: id, Actual: describePaneSnapshot(sp)})
		}
	}

	return DiffReport{Drifts: drifts, SeenFlips: flips}
}

// Run owns the periodic liveness check (1.8). It opens a fresh short-lived
// session.snapshot connection each interval, diffs it against the tracked
// state, and calls onReport with the result when anything disagrees — either a
// genuine drift (the sole signal for forcing a subscription re-create) or a
// benign seen-flip (2.4). Run never mutates tracker state: applying a
// seen-flip is App.Run's job via ApplySeenFlip, so the single-writer rule
// holds.
func (t *Tracker) Run(ctx context.Context, interval time.Duration, onReport func(DiffReport)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.reconcile(ctx, onReport)
		}
	}
}

// reconcile performs one liveness probe. A fetch failure is logged and skipped
// — that is the loud-failure path owned by the subscriber's reconnect logic,
// not this check (0.4: this loop exists for the silent failure mode).
func (t *Tracker) reconcile(ctx context.Context, onReport func(DiffReport)) {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	resp, err := snapshot.Fetch(tctx)
	cancel()
	if err != nil {
		slog.Warn("reconcile: session.snapshot failed", "error", err)
		return
	}

	report := t.Diff(resp.Snapshot)
	if report.Empty() {
		return
	}
	for _, d := range report.Drifts {
		slog.Warn("reconcile: state drift", "kind", d.Kind, "id", d.ID, "tracked", d.Tracked, "actual", d.Actual)
	}
	for _, sf := range report.SeenFlips {
		slog.Info("reconcile: done pane seen via snapshot", "pane_id", sf.PaneID)
	}
	onReport(report)
}

func hasWorkspace(ws []snapshot.Workspace, id string) bool {
	for _, w := range ws {
		if w.WorkspaceID == id {
			return true
		}
	}
	return false
}

func describePaneTracked(p PaneState) string {
	return fmt.Sprintf("workspace=%s tab=%s agent=%s status=%s", p.WorkspaceID, p.TabID, p.Agent, p.Status)
}

func describePaneSnapshot(p snapshot.Pane) string {
	agent := ""
	if p.Agent != nil {
		agent = *p.Agent
	}
	return fmt.Sprintf("workspace=%s tab=%s agent=%s status=%s", p.WorkspaceID, p.TabID, agent, p.AgentStatus)
}

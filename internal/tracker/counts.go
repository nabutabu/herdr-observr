package tracker

import (
	"log/slog"

	"github.com/nabutabu/herdr-observr/internal/snapshot"
)

// AgentCounts is a point-in-time copy of the live per-state agent counts,
// globally and per workspace (2.6). Phase 3.6/3.7 wire these to gauges
// (herdr.agent.active/blocked/idle and herdr.workspace.agent.concurrent); this
// layer only maintains and serves them.
//
// A state absent from a map means zero agents in that state — no zero-valued
// entries are ever stored.
type AgentCounts struct {
	Global    map[snapshot.AgentStatus]int
	Workspace map[string]map[snapshot.AgentStatus]int
}

// Counts returns the current per-state agent counts — global and per
// workspace — as a deep copy. Safe to call from any goroutine; Phase 3 gauge
// exporters snapshot it periodically rather than polling under the tracker
// lock.
func (t *Tracker) Counts() AgentCounts {
	t.mu.RLock()
	defer t.mu.RUnlock()

	global := make(map[snapshot.AgentStatus]int, len(t.stateCounts))
	for s, n := range t.stateCounts {
		global[s] = n
	}
	workspace := make(map[string]map[snapshot.AgentStatus]int, len(t.workspaceStateCounts))
	for ws, counts := range t.workspaceStateCounts {
		wsCounts := make(map[snapshot.AgentStatus]int, len(counts))
		for s, n := range counts {
			wsCounts[s] = n
		}
		workspace[ws] = wsCounts
	}
	return AgentCounts{Global: global, Workspace: workspace}
}

// incrStateLocked records one more agent in state s under workspace ws. Called
// by the Apply* methods with t.mu already held. The workspace bucket is
// created on demand — a status event may arrive for an unknown workspace whose
// workspace.created event was missed.
func (t *Tracker) incrStateLocked(ws string, s snapshot.AgentStatus) {
	t.stateCounts[s]++
	wsCounts := t.workspaceStateCounts[ws]
	if wsCounts == nil {
		wsCounts = map[snapshot.AgentStatus]int{}
		t.workspaceStateCounts[ws] = wsCounts
	}
	wsCounts[s]++
}

// decrStateLocked records one fewer agent in state s under workspace ws. A
// missing key is logged and ignored rather than decremented below zero: it is
// a symptom of state grown inconsistent along an unobserved path (events carry
// no revision, 0.4), and the next ApplySnapshot re-baseline recomputes the
// counters from the authoritative picture. Empty buckets are dropped so the
// workspace map can't grow unbounded as workspaces come and go.
func (t *Tracker) decrStateLocked(ws string, s snapshot.AgentStatus) {
	wsCounts := t.workspaceStateCounts[ws]
	if wsCounts == nil || wsCounts[s] <= 0 {
		slog.Warn("decrementing untracked agent state count", "workspace_id", ws, "state", s)
		return
	}
	wsCounts[s]--
	if wsCounts[s] == 0 {
		delete(wsCounts, s)
		if len(wsCounts) == 0 {
			delete(t.workspaceStateCounts, ws)
		}
	}
	if t.stateCounts[s] > 0 {
		t.stateCounts[s]--
	}
}

// resetCountsLocked clears both counters. Used by ApplySnapshot, which
// reconstructs them from the seeded agent records rather than patching them
// incrementally — a re-baseline is the authoritative picture and must win
// wholesale.
func (t *Tracker) resetCountsLocked() {
	t.stateCounts = map[snapshot.AgentStatus]int{}
	t.workspaceStateCounts = map[string]map[snapshot.AgentStatus]int{}
}

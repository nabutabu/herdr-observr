// Package usage defines the adapter seam through which herdr-observr turns
// per-session cumulative usage totals from coding-assistant sources (opencode
// first) into OTel telemetry attributed to the same pane/agent resources used
// for lifecycle state (PLAN.md Usage & Cost Telemetry Plan, U1.1).
//
// Privacy constraint, same bar as the rest of the project: adapters read
// *cumulative totals only* — the source's own running counters for cost and
// tokens. Nothing here ever reads, transmits, or even represents prompt or
// response content from an assistant's storage; message rows are out of scope
// by contract.
package usage

import (
	"context"

	"github.com/nabutabu/herdr-observr/internal/snapshot"
)

// AgentSessionRef is the adapter-facing lookup key for one session in its
// source storage. It is the projection of snapshot.AgentSessionInfo an
// adapter actually needs: the Source ("herdr:opencode"), a typed Kind, and
// the Value — the session id when Kind is id, or a session directory path
// when Kind is path. The Agent field from AgentSessionInfo is deliberately
// absent: dispatch to the right adapter happens on the pane's agent type in
// the collector (U1.2), not from inside the ref.
type AgentSessionRef struct {
	Source string                       // e.g. "herdr:opencode"; identifies the assistant/backend.
	Kind   snapshot.AgentSessionRefKind // id | path, mirroring Herdr's AgentSessionRefKind.
	Value  string                       // session id (id) or session directory path (path).
}

// UsageTotals is a point-in-time cumulative snapshot read directly from the
// source (opencode's session row). Adapters return totals, never deltas —
// diffing against prior observations happens one layer up in the collector,
// keyed by SessionID (never by pane, per the U-finding that agent_session is
// frontmost-right-now, not a stable pane binding).
//
// The token columns mirror the five cumulative counters opencode maintains on
// its session row (tokens_input, tokens_output, tokens_reasoning,
// tokens_cache_read, tokens_cache_write); the "total" counter exported at
// U4.1 is derived at export time, not stored here. Cost is USD as reported by
// the source.
type UsageTotals struct {
	SessionID        string
	CostUSD          float64
	InputTokens      int64
	OutputTokens     int64
	ReasoningTokens  int64
	CacheReadTokens  int64
	CacheWriteTokens int64
}

// UsageDelta is one session's accrued usage between two polls: current
// cumulative totals minus the last-observed totals for that session (U1.2).
// It is the collector's output, produced by diffing against its session_id ->
// last-observed Total map, and it is the normalized usage form itself (U3.1):
// U4 exports these deltas directly, attaching observed-time and
// pane/workspace attribution at export time, not via a separate seam type.
//
// Fields are deltas by construction — never negative by design (a decrease is
// treated as a source reset and re-seeds the cursor instead of emitting).
// Attribution (which pane/workspace last pointed at the session) is a U3/U4
// concern and deliberately absent here.
type UsageDelta struct {
	SessionID        string
	CostUSD          float64
	InputTokens      int64
	OutputTokens     int64
	ReasoningTokens  int64
	CacheReadTokens  int64
	CacheWriteTokens int64
}

// UsageAdapter is the seam between herdr-observr and one assistant's storage.
// Adapters are stateless: the collector owns all session_id -> last-seen
// totals state, so an adapter carries nothing across PollUsage calls.
type UsageAdapter interface {
	// AgentType returns the pane agent type this adapter serves (e.g.
	// "opencode"). The collector dispatches each pane's agent_session to the
	// adapter whose AgentType matches the pane's agent.
	AgentType() string

	// PollUsage resolves ref according to its Kind and returns the source's
	// current cumulative totals for that session, as reported by the source
	// itself. The ref is the lookup key only; the returned SessionID is what
	// any long-lived state is keyed by.
	PollUsage(ctx context.Context, ref AgentSessionRef) (UsageTotals, error)
}

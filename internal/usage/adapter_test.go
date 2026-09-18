package usage

import (
	"context"
	"testing"

	"github.com/nabutabu/herdr-observr/internal/snapshot"
)

var _ UsageAdapter = (*fakeAdapter)(nil)

type fakeAdapter struct {
	agentType string
}

func (f *fakeAdapter) AgentType() string { return f.agentType }

func (f *fakeAdapter) PollUsage(_ context.Context, ref AgentSessionRef) (UsageTotals, error) {
	return UsageTotals{
		SessionID:        ref.Value,
		CostUSD:          0.42,
		InputTokens:      100,
		OutputTokens:     50,
		ReasoningTokens:  10,
		CacheReadTokens:  5,
		CacheWriteTokens: 2,
	}, nil
}

func TestAdapterRoundingTrip(t *testing.T) {
	adapter := &fakeAdapter{agentType: "opencode"}
	if got := adapter.AgentType(); got != "opencode" {
		t.Fatalf("AgentType() = %q, want %q", got, "opencode")
	}

	ref := AgentSessionRef{
		Source: "herdr:opencode",
		Kind:   snapshot.AgentSessionRefKindID,
		Value:  "ses_f4e33e375ffeyBBqhfPxBEhEmn",
	}

	totals, err := adapter.PollUsage(context.Background(), ref)
	if err != nil {
		t.Fatalf("PollUsage: %v", err)
	}
	if totals.SessionID != ref.Value {
		t.Errorf("SessionID = %q, want ref.Value %q", totals.SessionID, ref.Value)
	}
	want := UsageTotals{
		SessionID:        "ses_f4e33e375ffeyBBqhfPxBEhEmn",
		CostUSD:          0.42,
		InputTokens:      100,
		OutputTokens:     50,
		ReasoningTokens:  10,
		CacheReadTokens:  5,
		CacheWriteTokens: 2,
	}
	if totals != want {
		t.Errorf("totals = %+v, want %+v", totals, want)
	}
}

func TestPathKindRefIsViableLookupKey(t *testing.T) {
	ref := AgentSessionRef{
		Source: "herdr:opencode",
		Kind:   snapshot.AgentSessionRefKindPath,
		Value:  "/home/user/.local/share/opencode/session-1",
	}
	totals, err := (&fakeAdapter{}).PollUsage(context.Background(), ref)
	if err != nil {
		t.Fatalf("PollUsage: %v", err)
	}
	if totals.SessionID == "" {
		t.Error("adapter returned empty SessionID for a path-kind ref")
	}
}

package otel

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	apimetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

const (
	// exportInterval is how often the periodic reader collects and exports
	// metric data. Short enough that the agent-state gauges of 3.6/3.7 stay
	// current; the OTLP exporter itself retries individual export attempts so
	// a collector blip is absorbed rather than losing a sample outright.
	exportInterval = 10 * time.Second

	// MeterName is the instrumentation scope name for all herdr-observr meters.
	MeterName = "herdr-observr"

	// UpMetricName is the 3.1 proof/liveness metric: 1 while this process is
	// alive and exporting. Registered by App on startup.
	UpMetricName = "herdr.up"
)

// (3.3) herdr.agent.state.transitions — a counter incremented once per genuine
// agent state transition (2.3), tagged by agent type and previous/new state.
// Bounded cardinality by construction: agent.type is a small closed set and
// each state has five values; no pane/workspace id participates (those are
// high cardinality and gated behind config in 3.6).
const (
	// TransitionMetricName is the 3.3 counter metric name.
	TransitionMetricName = "herdr.agent.state.transitions"

	// Attribute keys on TransitionMetricName. The herdr.* prefix matches 3.8's
	// herdr.agent.* scheme and the existing herdr.machine.* resource keys;
	// "state" (not "new_state") is the forward-compatible name 3.8 shares.
	AgentTypeKey       = "herdr.agent.type"
	AgentPreviousState = "herdr.agent.previous_state"
	AgentStateKey      = "herdr.agent.state"
)

// (3.4) herdr.agent.state.duration — a histogram of closed agent state
// intervals, recorded once each time an agent leaves a state (2.3): the
// working/blocked/idle (and done/unknown) duration metric. Tagged by agent
// type and the state whose interval just closed. It shares AgentTypeKey and
// AgentStateKey with the 3.3 counter; bounded cardinality by construction.
//
// Deliberately separate from attention latency (3.5): "time in done" is a
// generic state duration like any other, while the done-but-unseen dwell time
// is its own alertable metric and gets its own histogram.
const (
	// DurationMetricName is the 3.4 histogram metric name.
	DurationMetricName = "herdr.agent.state.duration"
)

// (3.5) herdr.agent.attention_latency — a histogram of closed done-but-unseen
// intervals (2.4), recorded once each time an agent stops being done: the
// elapsed "waiting for a human" time. Deliberately separate from 3.4's generic
// state duration — time in done is one thing, done-and-unseen dwell is the
// alertable metric (e.g. "agent has been waiting for 20+ minutes").
//
// Tagged by agent.type only. Unlike 3.4 there is no state attribute: this
// metric's state is always `done` by construction, so a state dimension would
// be a constant. Bounded cardinality, no pane/workspace id.
const (
	// AttentionLatencyMetricName is the 3.5 histogram metric name.
	AttentionLatencyMetricName = "herdr.agent.attention_latency"
)

// (3.6) herdr.agent.{active,blocked,idle,done,unknown} — observable gauges of
// the live per-state agent counts (2.6). herdr.agent.active holds the working
// count; done/unknown are gauged too so all five states are visible. Each
// gauge observes a global datapoint plus one per workspace carrying
// WorkspaceIDKey. Note: the plan originally gated the per-workspace datapoints
// behind a config flag (default off) as a cardinality caution; the decision
// taken during implementation is to always emit them — bounded at
// 5 x N_workspaces series per machine, and revisit-able when Phase 4.2 config
// lands. Aggregate counts only; no agent/pane ids participate.
const (
	// ActiveAgentsMetricName is the 3.6 gauge for agents currently working.
	ActiveAgentsMetricName = "herdr.agent.active"

	// BlockedAgentsMetricName is the 3.6 gauge for blocked agents.
	BlockedAgentsMetricName = "herdr.agent.blocked"

	// IdleAgentsMetricName is the 3.6 gauge for idle agents.
	IdleAgentsMetricName = "herdr.agent.idle"

	// DoneAgentsMetricName is the 3.6 gauge for done agents.
	DoneAgentsMetricName = "herdr.agent.done"

	// UnknownAgentsMetricName is the 3.6 gauge for unknown-state agents.
	UnknownAgentsMetricName = "herdr.agent.unknown"

	// WorkspaceIDKey is the herdr.workspace.id attribute key. Shared with 3.7
	// (herdr.workspace.agent.concurrent) and 3.8 (per-transition events) so all
	// per-workspace datapoints use the one key.
	WorkspaceIDKey = "herdr.workspace.id"
)

// (3.7) herdr.workspace.agent.concurrent — an observable gauge of the agents
// concurrently working per workspace, summed over working + blocked (the two
// states where an agent is engaged in the work cycle: actively producing or
// waiting on a human). Distinct from the 3.6 gauges, which are per-state; this
// is the mission's "how much concurrent agent work is happening" per workspace.
//
// Per-workspace only — there is no global datapoint, because a concurrency
// count is meaningless without the workspace it belongs to. One datapoint per
// workspace carries WorkspaceIDKey, always emitted (explicit 0 when the sum is
// zero), mirroring the 3.6 gauges' always-emit decision so per-workspace
// series never go stale. Bounded at N_workspaces series per machine; aggregated
// counts only, no agent/pane ids participate.
const (
	// WorkspaceConcurrentMetricName is the 3.7 gauge metric name.
	WorkspaceConcurrentMetricName = "herdr.workspace.agent.concurrent"
)

// NewMeterProvider initializes the OTel metrics SDK with the OTLP/gRPC
// exporter and associates it with res.
//
// The exporter reads its whole configuration from the environment
// (OTEL_EXPORTER_OTLP_ENDPOINT defaulting to https://localhost:4317,
// OTEL_EXPORTER_OTLP_INSECURE, OTEL_EXPORTER_OTLP_HEADERS, ...), so no
// endpoint plumbing is needed here. WithInsecure pins the SDK to a plaintext
// local collector, which is the deployment this plugin targets; promoting TLS
// to a config flag is deferred to the Phase 4.2 plugin config.
//
// The gRPC connection is established lazily (grpc.NewClient): construction
// never blocks or fails because the collector is unreachable — individual
// exports fail and are dropped instead, so the process keeps tracking state
// with no exporter (Phase 5.4).
//
// The returned func flushes and shuts down the provider. Callers that receive
// an error (configuration-level failure only) should log and continue without
// telemetry rather than aborting the process.
func NewMeterProvider(ctx context.Context, res *resource.Resource) (apimetric.MeterProvider, func(context.Context) error, error) {
	exporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithInsecure())
	if err != nil {
		return nil, nil, fmt.Errorf("creating OTLP gRPC metrics exporter: %w", err)
	}

	reader := metric.NewPeriodicReader(exporter, metric.WithInterval(exportInterval))
	mp := newMeterProviderWithReader(reader, res)

	// Install as the global so any instrumentation that doesn't receive the
	// provider explicitly still exports through it.
	otel.SetMeterProvider(mp)

	return mp, mp.Shutdown, nil
}

// newMeterProviderWithReader builds a MeterProvider over an explicit Reader.
// Separated from NewMeterProvider so tests can drive collection with a manual
// reader instead of a live collector.
func newMeterProviderWithReader(reader metric.Reader, res *resource.Resource) *metric.MeterProvider {
	return metric.NewMeterProvider(metric.WithReader(reader), metric.WithResource(res))
}

// NewMeterProviderWithReader builds a MeterProvider over an explicit Reader,
// exported for cross-package tests that inspect collected metric data without
// a live collector (the app tests drive a manual reader the same way the
// package-internal meter tests do). Returns the concrete provider so callers
// can Shutdown/flush it. Not part of the runtime configuration; production
// code should use NewMeterProvider.
func NewMeterProviderWithReader(reader metric.Reader, res *resource.Resource) *metric.MeterProvider {
	return newMeterProviderWithReader(reader, res)
}

// NewTransitionCounter registers herdr.agent.state.transitions (3.3) on meter
// and returns it, or an error if registration failed. The returned counter is
// a no-op when the meter itself is a no-op (telemetry disabled).
func NewTransitionCounter(meter apimetric.Meter) (apimetric.Int64Counter, error) {
	return meter.Int64Counter(
		TransitionMetricName,
		apimetric.WithUnit("1"),
		apimetric.WithDescription("Count of genuine agent state transitions, tagged by agent type and previous/new state"),
	)
}

// NewDurationHistogram registers herdr.agent.state.duration (3.4) on meter and
// returns it, or an error if registration failed. The returned histogram is a
// no-op when the meter itself is a no-op (telemetry disabled). Durations are
// recorded in seconds (unit "s"), one sample per closed state interval.
func NewDurationHistogram(meter apimetric.Meter) (apimetric.Float64Histogram, error) {
	return meter.Float64Histogram(
		DurationMetricName,
		apimetric.WithUnit("s"),
		apimetric.WithDescription("Closed agent state interval durations, tagged by agent type and state"),
	)
}

// NewAttentionLatencyHistogram registers herdr.agent.attention_latency (3.5)
// on meter and returns it, or an error if registration failed. The returned
// histogram is a no-op when the meter itself is a no-op (telemetry disabled).
// These are closed done-but-unseen intervals, recorded in seconds (unit "s"),
// one sample each time an agent leaves done. Tagged by agent type only; the
// state is always done by construction, so no state attribute participates.
func NewAttentionLatencyHistogram(meter apimetric.Meter) (apimetric.Float64Histogram, error) {
	return meter.Float64Histogram(
		AttentionLatencyMetricName,
		apimetric.WithUnit("s"),
		apimetric.WithDescription("Closed done-but-unseen (attention latency) intervals, tagged by agent type"),
	)
}

// AgentCountGauges holds the five 3.6 state-count gauges. The state each gauge
// measures is fixed by its metric name (Active=herdr.agent.active,
// Blocked=..., Idle=..., Done=..., Unknown=...), so no state attribute is
// needed on datapoints. Created bare — without a collection callback — and
// registered app-side via meter.RegisterCallback (internal/app/telemetry.go),
// which maps tracker.AgentCounts onto them in one snapshot pass.
type AgentCountGauges struct {
	Active  apimetric.Int64ObservableGauge
	Blocked apimetric.Int64ObservableGauge
	Idle    apimetric.Int64ObservableGauge
	Done    apimetric.Int64ObservableGauge
	Unknown apimetric.Int64ObservableGauge
}

// NewAgentCountGauges registers the five 3.6 gauges on meter and returns them,
// or an error if any registration failed. The returned gauges are no-ops when
// the meter itself is a no-op (telemetry disabled). Counts are observed with
// unit "1" (number of agents), one datapoint per state running on the SDK's
// collection cadence — no per-event writes, so gauge freshness follows the
// periodic reader's exportInterval.
func NewAgentCountGauges(meter apimetric.Meter) (AgentCountGauges, error) {
	create := func(name, desc string) (apimetric.Int64ObservableGauge, error) {
		return meter.Int64ObservableGauge(name, apimetric.WithUnit("1"), apimetric.WithDescription(desc))
	}

	active, err := create(ActiveAgentsMetricName, "Number of agents currently working")
	if err != nil {
		return AgentCountGauges{}, err
	}
	blocked, err := create(BlockedAgentsMetricName, "Number of agents currently blocked")
	if err != nil {
		return AgentCountGauges{}, err
	}
	idle, err := create(IdleAgentsMetricName, "Number of agents currently idle")
	if err != nil {
		return AgentCountGauges{}, err
	}
	done, err := create(DoneAgentsMetricName, "Number of agents currently done (finished, awaiting human)")
	if err != nil {
		return AgentCountGauges{}, err
	}
	unknown, err := create(UnknownAgentsMetricName, "Number of agents in the transient/unknown state")
	if err != nil {
		return AgentCountGauges{}, err
	}
	return AgentCountGauges{Active: active, Blocked: blocked, Idle: idle, Done: done, Unknown: unknown}, nil
}

// NewWorkspaceConcurrentGauge registers herdr.workspace.agent.concurrent (3.7)
// on meter and returns it, or an error if registration failed. The returned
// gauge is a no-op when the meter itself is a no-op (telemetry disabled).
// Created bare — without a collection callback — and registered app-side via
// meter.RegisterCallback (internal/app/telemetry.go), which maps
// tracker.AgentCounts onto it in the same snapshot pass as the 3.6 gauges.
// Counts are observed with unit "1" (number of agents), one datapoint per
// workspace running on the SDK's collection cadence — no per-event writes.
func NewWorkspaceConcurrentGauge(meter apimetric.Meter) (apimetric.Int64ObservableGauge, error) {
	return meter.Int64ObservableGauge(
		WorkspaceConcurrentMetricName,
		apimetric.WithUnit("1"),
		apimetric.WithDescription("Number of agents concurrently working per workspace (working + blocked)"),
	)
}

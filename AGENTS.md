# AGENTS.md — herdr-observr

Guidance for any AI coding agent (or human) working in this repository. Read
this before making changes — it encodes decisions that were already argued
out and verified against a live Herdr instance, so re-litigating them from
first principles will waste time and likely produce a regression.

## What this project is

`herdr-observr` is a Go plugin for [Herdr](https://github.com/nabutabu/herdr)
(a terminal multiplexer / agent-management tool). It subscribes to Herdr's
live event stream and exports agent runtime *health* telemetry — where
agents are stuck, how long they wait for a human, how much concurrent work
is happening — over OTLP to standard observability backends (Prometheus,
Tempo, Grafana).

**Strict privacy constraint, non-negotiable:** herdr-observr never transmits
*what* an agent is doing — no pane content, no terminal output, no agent
transcripts. Only lifecycle/state metadata (ids, states, timestamps) leaves
the process. Any change that risks putting pane/terminal content on the wire
must be rejected or flagged, regardless of how useful it might seem for
debugging.

## Source of truth for planning

The full phased implementation plan (Phase 0–6, resolved architecture
decisions, spike findings, changelog of what changed and why) lives outside
this repo in the project's planning document — treat this file as the
implementation-facing companion to that plan, not a replacement. If you have
access to that plan, read it before starting Phase 2–3 work; it has far more
rationale than fits here. If it is not available to you, the summary below
is the minimum you need.

## Verify over trust

This codebase has already been burned once by trusting stale
docs/assumptions instead of a live Herdr instance — and, separately, by
trusting *this file* after the code had moved past it. The house rule:

- **Never** implement against remembered/assumed wire shapes. If a change
  touches the socket protocol (event payloads, `session.snapshot` shape,
  RPC behavior), it must be checked against a live Herdr instance (or, at
  minimum, against the already-verified findings recorded in code comments
  in `internal/events/event.go`, `internal/snapshot/snapshot.go`, and this
  file) before being trusted.
- **Never trust this file's "Phase status" section over the tree.** It is
  hand-maintained prose describing code, which means it can and does lag —
  it has already done so once (see the note at the top of that section).
  Before claiming a phase item is done or not done, `grep`/`view` the actual
  package and its `_test.go` file.
- **Consult the real Herdr source, not memory.** When deciding any object
  shape, event payload, RPC behavior, or structure received from the Herdr
  socket, always refer to the actual Herdr project at
  `https://github.com/herdrdev/herdr` (the upstream home of the project; the
  fork you can clone/fetch from is `nabutabu/herdr`). Do not trust model
  memory, other plugins, or older docs over that source. Where this file's
  confirmed findings (or the comments in `internal/events/event.go` /
  `internal/snapshot/snapshot.go`) conflict with or stray from that source,
  note the discrepancy rather than silently overriding.
- When re-fetching this repo itself (e.g. to diff before editing), use the
  tarball approach, not the GitHub tree API or raw-path guessing:
  ```sh
  curl -sL "https://codeload.github.com/nabutabu/herdr-observr/tar.gz/refs/heads/main" -o repo.tar.gz \
    && tar xzf repo.tar.gz
  ```
- Spikes are throwaway. If you write a `cmd/spike-*/main.go` or an
  `./experiment` binary to probe live socket behavior, do not merge it —
  extract the *finding* into a comment or this file, then delete the spike.

## Confirmed wire-protocol facts (do not re-derive, just use)

These were confirmed against a running Herdr v0.9.0 instance and are load-
bearing for the current design. Treat them as constraints, not suggestions:

1. **No respawn on startup-hook crash.** Herdr does not restart a crashed
   `[[startup]]` process within a boot — only on the next full server
   restart. `plugin.log.list` retains only the single latest entry per
   startup command (overwritten each boot), so it is not a durable crash
   record on its own.
2. **One connection, one purpose.** A connection that has issued
   `events.subscribe` cannot also serve RPCs — sending a second RPC on it
   resets the peer. Conversely, plain RPCs don't share a connection either.
   Rule: one long-lived connection for the subscription (owned by a
   dedicated reader goroutine), and every RPC (`ping`, `session.snapshot`,
   reconciliation, future `report_agent` calls) opens its own short-lived
   connection via `client.Call`. Never mix the two.
3. **Pushed events carry no sequence number or revision**, even though both
   exist server-side. `pane.agent_status_changed` on the wire is only
   `{agent, agent_status, pane_id, workspace_id}` (+ occasional nesting —
   see `internal/events/event.go`'s `eventPayload`/`workspaceRef`/`paneRef`
   handling). There is no event-level dedup available; idempotent state
   application is the only dedup mechanism (see the tracker's per-kind
   `Apply*` methods, which upsert rather than assuming ordering).
4. **The wire frame has no top-level `type` field.** Pushed events arrive as
   `{"event": "pane.agent_status_changed", "data": {...}}`. `event.go`'s
   `parseFrame`/`wireFrame` is the single source of truth for classifying
   and normalizing a frame in one decode pass — do not reintroduce a
   separate classify-then-normalize split (that was a real double-unmarshal
   bug that got fixed).
5. **`done` is only reachable through genuine agent detection.**
   `pane.report_agent --state` accepts `idle|working|blocked|unknown` only —
   verified against `herdr src/cli.rs parse_pane_agent_state` and a live CLI.
   You cannot push `done` through the RPC. **(Live-verified refinement, 2026-09:
   you *can* reach `done` without a full coding agent.)** Reporting `idle` on a
   pane that is unfocused/unseen lets Herdr's own detection engine convert
   `idle && !seen` → `done` and push a real `pane.agent_status_changed` with
   `agent_status: done` — so environment 0.4's gate ("needs a real agent") is
   really a gate on Herdr *detection*, and an unseen pane can be scripted into
   done via `--state idle`. Leaving `done` headlessly works too: any
   `report_agent` state change out of `done` fires the event-driven close path
   (`ApplyAgentStatusChanged`), which the tracker closed to sub-second accuracy
   live (2m50.806s vs. 170.999s wall). The *seen*-flip close path (leaving done
   by being seen) could **not** be triggered from the headless CLI: `send-keys`
   on an unfocused pane in a non-focused workspace does not flip `seen` — seen
   is flipped by the TUI client's focus/rendering, so live seen-flip
   verification needs the actual TUI client focused on the pane. (Unit
   fixtures still construct `done` synthetically for the tracker layer, which
   is fine.)
6. **Subscriptions can go silently dead.** After repeated
   subscribe/disconnect cycles, the stream can stop delivering pushed events
   with *no socket error at all* — no EOF, no error, just silence. A
   reconnect-on-error loop alone will never catch this. This is why
   `internal/tracker.Tracker.Run` exists as an independent periodic
   `session.snapshot` diff (see below) — it is the primary defense, not a
   backstop.
7. **No cross-machine machine id exists in Herdr.** Injected pane env vars
   are only `HERDR_ENV`, `HERDR_PANE_ID`, `HERDR_TAB_ID`,
   `HERDR_WORKSPACE_ID`, `HERDR_SOCKET_PATH`, `HERDR_BIN_PATH`. Herdr's own
   ids (`w1`, `w1:t1`, `w1:p1`, `term_...`) are explicitly documented as
   scoped to a single server, not unique across machines. `herdr.machine.id`
   must be a UUID generated by the plugin on first run and persisted to
   `HERDR_PLUGIN_STATE_DIR` — never derived from hostname or Herdr's ids.
   Hostname is only ever a secondary, display-only attribute
   (`herdr.machine.hostname`), read via the plugin's own OS call.
8. **`session.snapshot` has `tabs[]` and `layouts[]`** beyond
   workspaces/panes/agents. `layouts[]` is pure UI geometry and is
   intentionally not modeled anywhere. Tab-lifecycle events **do** exist on
   the subscription — `tab.created`, `tab.closed`, `tab.renamed` (all
   kind-scoped, no `pane_id`) — verified against upstream
   `src/api/schema/events.rs`, `src/app/creation.rs`, `src/app/api/tabs.rs`,
   and now fully wired: `internal/events/subscription.go` subscribes to all
   three, `internal/events/event.go` normalizes them
   (`KindTabCreated`/`KindTabClosed`/`KindTabRenamed`), and
   `internal/tracker/tracker.go` applies them
   (`ApplyTabCreated`/`ApplyTabClosed`/`ApplyTabRenamed`). Two follow-on wire
   facts anchored to that source:
   - Closing a tab emits **no `pane.closed` for its panes** — they are
      destroyed silently in `handle_tab_close`. `Tracker.ApplyTabClosed`
      therefore cascades the tab's panes/agents itself (mirroring
      `ApplyWorkspaceClosed`), including flushing any open attention-latency
      interval. Each cascaded entity enters its close grace window (2.7)
      instead of vanishing immediately.
   - `tab.closed` on the last tab is immediately followed by
      `workspace.closed`, whose cascade is idempotent against the tab's
      (the `closeAgentLocked`/`closePaneLocked` guards make the double
      cascade emit attention latency and decrement each agent exactly once).
9. **`pane.agent_status_changed` is structurally pane-scoped — there is no
   global form.** Herdr probes `pane_id` at subscribe time and rejects an
   unscoped subscription outright (`"invalid request: missing field
   pane_id"`). This means a pane created *after* the last subscribe is
   invisible to status-change events until the subscription is rebuilt to
   include it. `internal/app/scope.go` tracks exactly which panes the live
   subscription covers; `app.paneCreatedNeedsResubscribe` checks each
   incoming `pane.created` against that set and triggers a resubscribe
   (via the same `resubscribe` channel the drift-detector in
   `Tracker.Run` uses) the moment an uncovered pane shows up, rather than
   waiting up to a full reconcile interval.

## Architecture (current)

```
main.go                        # entrypoint: signal handling, delegates to internal/app
internal/app/app.go            # process lifecycle: ping → bootstrap snapshot → subscribe →
                                #   event loop → reconnect-on-error → reconciliation-driven
                                #   resubscribe → pane.created-triggered resubscribe
internal/app/scope.go          # App's live subscription state: the subscribed-pane set for
                                #   pane.agent_status_changed, seeded by SubscribeFromSnapshot
                                #   and replaced wholesale on every (re)subscribe
internal/app/app_test.go       # scope + resubscribe-trigger + event-routing tests
internal/client/
  client.go                    # Dial, SocketPath (HERDR_SOCKET_PATH), one-shot Call()
  rpc.go                       # Request/Response wire types, WriteFrame/ReadFrame (NDJSON framing)
internal/events/
  subscription.go              # SubscriptionType consts, BuildParams, SubscribeFromSnapshot
                                #   (fetches session.snapshot, opens a pane-scoped subscription,
                                #   returns the subscribed pane IDs it scoped the subscribe to)
  subscriber.go                # Subscriber: dedicated long-lived connection + reader goroutine,
                                #   Events() <-chan NormalizedEvent, Err() <-chan error
  event.go                     # Kind enum, NormalizedEvent, wireFrame, parseFrame
                                #   (single-pass classify+normalize)
internal/snapshot/
  snapshot.go                  # Response/Snapshot/Workspace/Pane/Tab/Agent structs, Fetch()
internal/otel/
  resource.go                  # BuildResource: env-driven resource (OTEL_SERVICE_NAME,
                                #   OTEL_RESOURCE_ATTRIBUTES, herdr-telemetry default)
  meter.go                     # NewMeterProvider: OTLP/gRPC exporter + periodic reader (3.1),
                                #   MeterName/UpMetricName consts (herdr.up proof gauge)
  log.go                       # NewLoggerProvider: OTLP/gRPC logs exporter + batch processor (3.8),
                                #   StateChangeEventName/Body + AgentIDKey consts
  trace.go                     # NewTracerProvider: OTLP/gRPC traces exporter + batch span processor
                                #   (3.9), TracerName const; short root herdr.agent.state_change spans
internal/tracker/
  tracker.go                   # Tracker: mutex-guarded workspaces/tabs/panes/agents maps.
                                #   Per-kind ApplyWorkspaceCreated/.../ApplyAgentStatusChanged/
                                #   ApplySeenFlip (steady-state writers), ApplySnapshot
                                #   (bootstrap/re-baseline), Diff/DiffReport (read-only
                                #   comparison, including tab membership and the done→idle
                                #   seen-flip distinction), Run (periodic reconcile loop),
                                #   close helpers + EvictExpired (2.7 grace-window cleanup)
  counts.go                    # AgentCounts, Tracker.Counts(): live per-state agent counts,
                                #   global and per-workspace, maintained incrementally
                                #   (incrStateLocked/decrStateLocked) — feeds Phase 3's
                                #   herdr.agent.* and herdr.workspace.agent.concurrent gauges
```

Ownership rules embedded in this structure — preserve them when extending:

- **`Subscriber`** exposes no write surface after construction. If you need
  to call an RPC, open a new connection via `client.Call`; never reuse the
  subscription connection.
- **`Tracker`** has exactly one writer path in steady state (the per-kind
  `Apply*` methods, called from `app.handleEvent`, itself called
  synchronously from `app.Run`'s event-loop `case`) and one re-baseline path
  (`ApplySnapshot`, called on initial bootstrap and after every reconnect),
  plus one housekeeping writer (`EvictExpired`, 2.7) fired from `app.Run`'s
  own ticker — still on the single event-loop goroutine.
  `Tracker.Run`'s reconciliation loop is **read-only** — it diffs
  (`Diff`/`DiffReport`) and calls `onReport()` with drifts and seen-flips;
  it never mutates tracker state directly. Applying a seen-flip
  (`ApplySeenFlip`) is `app.Run`'s job, invoked from the same `onReport`
  callback that decides whether to resubscribe. This separation is
  intentional: it keeps a single owner (`app.Run`) responsible for actually
  tearing down and recreating the subscription and for all tracker writes,
  so there's no risk of two goroutines racing.
- **`app.App.reconnect`** is the only path that closes and replaces the
  subscription, whatever triggers it: a hard error (`sub.Err()`), a drift
  signal from the reconciliation loop, or an uncovered pane surfacing via
  `paneCreatedNeedsResubscribe`. All three funnel through the same
  `resubscribe` channel and the same re-bootstrap: fresh `session.snapshot`
  → `ApplySnapshot` → `SubscribeFromSnapshot` → replace `a.scope`. State
  never trusts a partial gap silently, and coverage never silently goes
  stale.
- **`scope`** is read/written solely on the `app.Run` event-loop goroutine —
  no locking. It exists because the tracker isn't the right owner of "which
  panes does the *subscription* cover" (the tracker also knows about panes
  created after the last subscribe, which is exactly the gap `scope`
  detects).

## Phase status (do not trust this section blindly — cross-check the tree)

**This section itself has already gone stale once** — an earlier revision
claimed Phase 2 and the pane-coverage resubscribe fix were unimplemented
after they had, in fact, landed. Treat every line below as a claim to spot-
check against the actual package and its `_test.go` file, not as ground
truth.

Implemented and tested, as of the current `main` branch:

- **1.1** — one-shot NDJSON client (`internal/client`)
- **1.3** — `session.snapshot` fetch/parse (`internal/snapshot.Fetch`)
- **1.4/1.5** — scoped subscription on its own dedicated connection
  (`internal/events.Subscriber`, `BuildParams`), now including
  `tab.created`/`tab.closed`/`tab.renamed` alongside the original
  workspace/pane kinds (finding #8)
- **1.6** — single-pass classify+normalize (`internal/events.parseFrame`,
  `NormalizedEvent`), covering all nine `Kind` values including the three
  tab kinds
- **1.7** — reconnect/resubscribe with capped exponential backoff on socket
  error/EOF (`app.subscribeWithBackoff`, `app.reconnect`)
- **1.8** — periodic reconciliation against a fresh snapshot, independent of
  socket state, driving a forced resubscribe on drift, plus classification
  of the benign `done`→idle seen-flip as distinct from a genuine drift
  (`internal/tracker.Tracker.Run`/`reconcile`/`Diff`)
- **Pane-coverage resubscribe (the structural gap in finding #9)** —
  `internal/app/scope.go` plus `app.paneCreatedNeedsResubscribe`. A pane
  created after the last subscribe triggers an immediate resubscribe
  instead of waiting for the next reconcile tick.
- **2.1–2.3** — state machine, per-agent state struct, and
  duration-by-state accounting (`tracker.AgentState`,
  `ApplyAgentStatusChanged`)
- **2.4** — attention-latency tracking, both close paths: a real
  status-change event out of `done` (`ApplyAgentStatusChanged`) and the
  silent reconcile-detected seen-flip (`ApplySeenFlip`)
- **2.5** — idempotent state application (same-state event is a no-op;
  every `Apply*` upserts by id rather than assuming ordering)
- **2.6** — workspace/global agent concurrency counts, maintained
  incrementally on every transition (`internal/tracker/counts.go`,
  `Tracker.Counts()`), including cleanup on workspace/tab/pane close so the
  counters can't leak
- **2.7** — bounded in-memory retention / grace window. Closing a pane/tab/
  workspace now flushes final durations and attention latency, decrements
  counts *immediately*, but retains the entities with `ClosedAt` stamped for
  a 15s grace window (`tracker.DefaultGraceWindow`) instead of deleting in
  place — a late status event for a closing pane is absorbed rather than
  creating a phantom that leaks a state count. `app.Run`'s event loop owns
  eviction on its own 5s ticker (`app.evictInterval` → `Tracker.EvictExpired`),
  so retention stays bounded to ~[15,20)s and the single-writer rule holds.
  Closed entities are skipped by `Diff` (no false drift → no spurious
  resubscribe) and wiped by any `ApplySnapshot` re-baseline. The workspace
  cascade now flushes `done` agents' attention-latency intervals too,
  matching the pane/tab paths, and the `closeAgentLocked` idempotency guard
  keeps tab.closed-then-workspace.closed double cascades to a single flush
  and decrement. `DurationByState` is freed at close — each closed state
  interval is surfaced at exit time via the `Tracker.StateDurations()` channel
  (3.4), so the map is pure accounting and only identity + `ClosedAt` metadata
  is retained. `Tracker.Agents()` (added for live verification, mirroring
  `Tabs()`) snapshots per-agent state for live-verification spikes.
- **Phase 2 exit criteria — both parts live-verified 2026-09 against a
  running Herdr v0.9.0.** (1) `pane.report_agent` drove a pane through
  working→blocked→working; the tracker closed working=1m24.271s and
  blocked=39.998s against wall-clock segments of 84.3s/40.0s (sub-10ms
  accuracy; the final `idle` report became `done` via detection — finding #5).
  (2) An agent left `done` headlessly and the tracker emitted attention
  latency 2m50.806s against a 170.999s wall interval, with counts moving
  (`done`→`working`) and `DurationByState` accumulating the closed done
  interval. See finding #5 for the headless seen-flip limitation.
- **3.1** — OTel SDK with env-based config (`internal/otel`). `BuildResource`
  reads `OTEL_SERVICE_NAME` (default `herdr-telemetry`) and
  `OTEL_RESOURCE_ATTRIBUTES`; `NewMeterProvider` builds an OTLP/gRPC
  MeterProvider that reads `OTEL_EXPORTER_OTLP_ENDPOINT` from the env, and
  `app.Run` registers the `herdr.up` proof gauge. Reach-ability live-verified
  2026-09 against a throwaway OTel Collector (gRPC/4317): `herdr.up`=1 landed
  with `service.name=herdr-telemetry` and env attributes. Wire fact: the SDK's
  gRPC exporter dials lazily (`grpc.NewClient`) — construction never fails or
  blocks on an unreachable collector; exports fail and are dropped instead
  (this is what Phase 5.4 relies on). None of this requires a live Herdr to
  test (`go test ./internal/otel/...` uses a manual reader + an unreachable
  endpoint with a short `OTEL_EXPORTER_OTLP_TIMEOUT`).
- **3.2 (resource attributes)** — `BuildResource` (in `internal/otel/resource.go`)
  attaches `herdr.machine.id` (persisted UUID from `HERDR_PLUGIN_STATE_DIR`,
  finding #7 / 0.6) and `herdr.machine.hostname` (secondary display-only, from
  the plugin's own OS call) onto every exported resource datum. The
  per-resource `herdr.workspace.id`/`herdr.pane.id`/`herdr.agent.id`/
  `herdr.agent.type` attributes listed in the plan's 3.2 belong on metric
  events/datapoints (3.8), not the resource, and are not wired yet.
- **3.3** — `herdr.agent.state.transitions` counter
  (`internal/otel/meter.go`'s `TransitionMetricName`, the `Telemetry` type in
  `internal/app/telemetry.go` registers it via
  `registerTransitions`/`recordTransition`). The tracker surfaces genuine
  transitions through a new `AgentTransition` channel (`Tracker.Transitions()`,
  mirroring `AttentionLatency`): emitted by `ApplyAgentStatusChanged` on every
  real event-driven change and by `ApplySeenFlip` on the silent done→idle
  close; never on same-state re-applies, first-seen upserts, re-baseline
  seeding, or close-time flushes. Incremented with `herdr.agent.type`,
  `herdr.agent.previous_state`, `herdr.agent.state` attributes — bounded
  cardinality by construction (no pane/workspace id).
- **3.4** — `herdr.agent.state.duration` histogram
  (`internal/otel/meter.go`'s `DurationMetricName`, `NewDurationHistogram`,
  unit `s`; `Telemetry` registers it via `registerStateDurations`/
  `recordStateDuration`). The tracker surfaces each *closed* state interval at
  the moment it closes through a new `StateDuration` channel
  (`Tracker.StateDurations()`), recording one histogram sample tagged
  `herdr.agent.type` + `herdr.agent.state` (working/blocked/idle/done/unknown,
  bounded cardinality). Emission sites are the three duration-close paths:
  the real transition (`ApplyAgentStatusChanged`), the silent done→idle close
  (`ApplySeenFlip`), and the close-time flush (`closeAgentLocked`); never on
  first-seen upserts, same-state re-applies, re-baseline seeding, or
  late status-in-grace (absorbed). This is a *closed* duration metric by
  design — polling the tracker's live records (as an earlier note here and in
  `types.go` had anticipated) would have sampled open interval ages instead
  of completed durations, and `DurationByState` is freed at close anyway.
- **3.5** — `herdr.agent.attention_latency` histogram
  (`internal/otel/meter.go`'s `AttentionLatencyMetricName`,
  `NewAttentionLatencyHistogram`, unit `s`; `Telemetry` registers it via
  `registerAttentionLatency`/`recordAttentionLatency`). The tracker's
  `AttentionLatency()` channel (2.4) — emitted by `ApplyAgentStatusChanged`,
  `ApplySeenFlip`, and the close-time flush in `closeAgentLocked` — is now
  recorded as a *closed* done-but-unseen interval histogram, separate from
  3.4's generic state duration so it can be alerted on independently. Tagged
  `herdr.agent.type` only: the interval's state is `done` by construction, so
  no state attribute participates (unlike 3.4); bounded cardinality by
  construction. `app.Run` still keeps a `slog.Debug` on each close for local
  troubleshooting.
- **3.6/3.7** — state-count and concurrency gauges
  (`internal/otel/meter.go`'s `NewAgentCountGauges` + `ActiveAgentsMetricName`
  (herdr.agent.active) and siblings for blocked/idle/done/unknown, and
  `NewWorkspaceConcurrentGauge`/`WorkspaceConcurrentMetricName`
  (herdr.workspace.agent.concurrent); `Telemetry.registerCountGauges` wires
  them to `Tracker.Counts()` on a single per-collection callback). Each 3.6
  gauge observes a global datapoint plus one per workspace carrying
  `herdr.workspace.id`; 3.7 is per-workspace only (working+blocked sum, the
  mission's "concurrent agent work"). Both always emit explicit zeros, so
  per-workspace series never go stale — the config flag the plan's cardinality
  caution suggested was deliberately dropped in favor of always-emit,
  revisit-able when Phase 4.2 config lands.
- **3.8** — structured state-change *events* over the OTLP Logs signal
  (`internal/otel/log.go`'s `NewLoggerProvider` (otlploggrpc exporter, batch
  processor, ~1s cadence, lazy-dial like the metrics path) and
  `NewLoggerProviderWithProcessor` test seam; `Telemetry.registerLogger`/
  `recordTransitionEvent`). Each genuine transition (the same `Transitions()`
  channel while feeding 3.3) emits one `log.Record` with event name
  `herdr.agent.state_change` (shared with 3.9's span), `tr.ObservedAt` as the
  timestamp, and attributes `herdr.agent.id` (the PaneID — no standalone
  agent id exists on the wire, finding #3), `herdr.agent.type`,
  `herdr.workspace.id`, `herdr.agent.previous_state`, `herdr.agent.state`.
  **`herdr.state.source` is deliberately not emitted** — that field is
  verified-absent from pushed status events (finding #3), so it would be a
  permanent no-op; see the note in `log.go` for the hook if the wire ever
  carries one. Event records are transient logs, not long-lived series, so the
  high-cardinality ids are fine un-gated (no 3.6-style config flag). Requires
  a `logs:` pipeline in the Phase 6 collector.

**Not yet implemented** — confirmed by `grep`, not just absence from this
list:

- **1.2** — self-supervising *external process* wrapper with crash/respawn
  backoff for the `[[startup]]` hook itself. The current backoff in
  `app.subscribeWithBackoff` only covers subscribe/snapshot retries
  *within* a running process — it does not protect against the process
  itself crashing (finding #1). This is the top open item.
- **Phase 3 (OTel/OTLP export)** — 3.1, 3.2 (resource attributes), **3.3**,
  **3.4**, **3.5**, **3.6**/**3.7**, **3.8**, and **3.9** all landed (see
  above). 3.9 (short `herdr.agent.state_change` root spans over the OTLP
  Traces signal, started at the transition's observed time and ended
  immediately) shares the `Transitions()` feed and `StateChangeEventName`
  with 3.8, emitting through `internal/otel/trace.go`'s
  `NewTracerProvider` (lazy-dial OTLP/gRPC trace exporter + batch span
  processor) into `Telemetry.recordTransitionSpan`. One note: `sdk/trace`
  ships inside the already-required `go.opentelemetry.io/otel/sdk` module —
  only the `otlptracegrpc` exporter package was added to go.mod.
- **Phase 4 (plugin packaging)** — no `herdr-plugin.toml` in the repo.
- **Phases 5–6** — reliability hardening and the local demo stack.

## Conventions

- **Constructed types over package-level globals.** State lives in
  explicitly constructed structs (`Tracker`, `Subscriber`, `App`, `scope`)
  passed around or closed over — not package globals. This is deliberate:
  it keeps everything independently testable and avoids data races between
  the event-consumer goroutine and the reconciliation goroutine.
- **No double-decode.** If you touch wire parsing, keep classify and
  normalize as a single `json.Unmarshal` pass (`parseFrame`/`wireFrame`).
  Splitting them back into two passes was already tried and reverted.
- **Idempotent state application, not sequence-based dedup.** There is no
  sequence number on the wire (finding #3). The tracker's `Apply*` methods
  always upsert by id; don't add ordering assumptions.
- **Reconciliation is primary, not a backstop.** It remains the sole
  detector of a silently stalled subscription (no socket error) and of
  missing events in any category, including tab membership now that
  `tab.created`/`tab.closed` are subscribed (`Tracker.Diff` reports tab-id
  membership drift too). Don't demote `Tracker.Run` to an optional safety
  net in comments or logic — it is load-bearing. The one thing reconcile
  does *not* own is pane-coverage gaps for `pane.agent_status_changed` —
  that has its own faster path (`scope`/`paneCreatedNeedsResubscribe`,
  finding #9) precisely because waiting a full interval for a brand-new
  pane was judged too slow.
- **Single-writer discipline for the tracker.** Every mutating call goes
  through `app.Run`'s event loop or its `onReport` callback — never call a
  tracker `Apply*`/`ApplySeenFlip` method from another goroutine.
- **Spike before building** on anything touching the live socket protocol.
  Throwaway `cmd/spike-*/main.go` binaries are the expected pattern; delete
  them after extracting the finding.
- **Batch findings before restructuring the plan.** If you're doing
  exploratory/spike work, gather findings first and propose one coherent
  restructuring rather than editing the plan document incrementally as you
  go.

## Testing

```sh
go test ./...
```

Tests run against stub Unix-socket servers (see `startStubServer` helpers in
`internal/client/client_test.go` and `internal/events/subscriber_test.go`)
and do not require a live Herdr instance. When adding wire-protocol tests,
follow that pattern — a real `net.Listen("unix", ...)` in a temp dir —
rather than mocking at a higher abstraction level, since framing bugs
(NDJSON boundaries, ack-vs-event classification) only show up at that
layer.

Tracker-level tests bypass the wire entirely: they construct `NormalizedEvent`
values directly and feed them to the relevant `Apply*` method
(`internal/tracker/tracker_test.go`, `counts_test.go`). This is the right
layer for state-machine/duration/attention-latency/concurrency-count
assertions — it doesn't need a live agent to exercise `done`, only a
hand-built event (finding #5 only constrains *live* verification, not unit
tests).

Live-instance verification (anything under "Verify over trust" above)
happens separately, via spike binaries against `HERDR_SOCKET_PATH` pointed
at a real running Herdr — not as part of `go test ./...`.

## Environment

- Go 1.26+
- `HERDR_SOCKET_PATH` — required; path to Herdr's Unix socket
- `HERDR_PLUGIN_STATE_DIR` — where the persisted `herdr.machine.id` UUID
  will live once Phase 4 packaging lands (not yet wired up)
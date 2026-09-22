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

This codebase has already been burned twice by trusting stale docs instead
of the tree: once by trusting assumed wire shapes instead of a live Herdr
instance, and once by trusting *this file's* Phase-status section after the
code had moved past it (the section claimed `herdr-plugin.toml` didn't exist
and that Phases 5–6 were untouched, when the manifest, build script, and the
full local demo stack were all already merged). The house rule:

- **Never** implement against remembered/assumed wire shapes. If a change
  touches the socket protocol (event payloads, `session.snapshot` shape,
  RPC behavior), it must be checked against a live Herdr instance (or, at
  minimum, against the already-verified findings recorded in code comments
  in `internal/events/event.go`, `internal/snapshot/snapshot.go`, and this
  file) before being trusted.
- **Never trust this file's "Phase status" section over the tree.** It is
  hand-maintained prose describing code, which means it can and does lag.
  Before claiming a phase item is done or not done, `grep`/`view` the actual
  package and its `_test.go` file — and, for anything meant to run in
  production, also check whether it's actually *wired into* `main.go`/
  `internal/app`. A package can be fully implemented and tested and still be
  dead code if nothing calls it (see the Usage/cost telemetry note below).
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
   record on its own. **This makes the process's own startup retry budget
   load-bearing:** `app.subscribeWithBackoff`'s initial-connection path caps
   out at 5 attempts / ~31s of backoff before `App.Run` returns an error and
   the process exits. Until 1.2 (below) lands, a Herdr socket that isn't
   ready within that window kills telemetry for the rest of the boot with no
   recovery path — this is the exact silent-failure shape finding #6 warns
   about, just reachable today via a finite retry budget rather than a
   hypothetical.
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
   (`herdr.machine.hostname`), read via the plugin's own OS call. **Wired up:**
   `internal/machineid.LoadOrCreate` + `internal/otel.BuildResource` do this
   today (see Phase status, 3.2) — the "not yet wired" note that used to sit
   in the Environment section below was itself stale and has been removed.
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
10. **A pane's `agent_session` is "frontmost right now", not a stable
    pane↔session binding.** Confirmed by polling: the same pane's
    `agent_session.value` was observed flipping between two distinct
    opencode session ids across consecutive ~10s polls, then flipping back a
    poll later. Anything that attributes usage/cost to a session must key
    its cursor state by the session id itself, never by pane
    (`internal/usage.UsageCollector.cursors` does this correctly). For the
    same reason, `Tracker.Diff` deliberately excludes `AgentSession` from
    pane comparison — including it would turn ordinary session-frontmost
    churn into spurious drift and needless resubscribes.

## Architecture (current)

```
main.go                        # entrypoint: signal handling, delegates to internal/app
internal/app/app.go            # process lifecycle: ping → bootstrap snapshot → subscribe →
                                #   event loop → reconnect-on-error → reconciliation-driven
                                #   resubscribe → pane.created-triggered resubscribe
internal/app/scope.go          # App's live subscription state: the subscribed-pane set for
                                #   pane.agent_status_changed, seeded by SubscribeFromSnapshot
                                #   and replaced wholesale on every (re)subscribe
internal/app/telemetry.go      # Telemetry: owns the OTel meter/logger/tracer instruments and
                                #   their nil-safe record* methods (Phase 3)
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
  snapshot.go                  # Response/Snapshot/Workspace/Pane/Tab structs, Fetch()
internal/otel/
  resource.go                  # BuildResource: env-driven resource (OTEL_SERVICE_NAME,
                                #   OTEL_RESOURCE_ATTRIBUTES, herdr-telemetry default) plus
                                #   herdr.machine.id/herdr.machine.hostname (3.2)
  meter.go                     # NewMeterProvider: OTLP/gRPC exporter + periodic reader (3.1),
                                #   MeterName/UpMetricName consts (herdr.up proof gauge), plus
                                #   the 3.3–3.7 counter/histogram/gauge constructors
  log.go                       # NewLoggerProvider: OTLP/gRPC logs exporter + batch processor (3.8),
                                #   StateChangeEventName/Body + AgentIDKey consts
  trace.go                     # NewTracerProvider: OTLP/gRPC traces exporter + batch span processor
                                #   (3.9), TracerName const; short root herdr.agent.state_change spans
internal/tracker/
  tracker.go                   # Tracker: mutex-guarded workspaces/tabs/panes/agents maps, plus
                                #   the U3.2 session→attribution index (sessions, keyed by the
                                #   agent_session Value — never by pane). Per-kind
                                #   ApplyWorkspaceCreated/.../ApplyAgentStatusChanged/
                                #   ApplySeenFlip (steady-state writers), ApplySnapshot
                                #   (bootstrap/re-baseline — the *only* writer of the sessions
                                #   map, rebuilt wholesale so vanished sessions self-prune),
                                #   Diff/DiffReport (read-only comparison, including tab
                                #   membership and the done→idle seen-flip distinction),
                                #   Run (periodic reconcile loop), close helpers + EvictExpired
                                #   (2.7 grace-window cleanup) — the sessions map is excluded
                                #   from EvictExpired: it is pure last-known attribution, no
                                #   phantom/leak risk, rebuilt on every re-baseline.
  counts.go                    # AgentCounts, Tracker.Counts(): live per-state agent counts,
                                #   global and per-workspace, maintained incrementally
                                #   (incrStateLocked/decrStateLocked) — feeds Phase 3's
                                #   herdr.agent.* and herdr.workspace.agent.concurrent gauges
internal/machineid/
  machineid.go                 # LoadOrCreate: persisted herdr.machine.id UUID under
                                #   HERDR_PLUGIN_STATE_DIR (finding #7)
internal/usage/                # Usage/cost telemetry (U-plan). BUILT AND TESTED BUT NOT WIRED
  adapter.go                   #   into App.Run or main.go — see "Not yet implemented" below
                                #   before assuming any of this runs in the shipped binary.
  collector.go                 # UsageCollector: polls tracker.Panes() on an interval, dispatches
                                #   each pane's agent_session to the adapter registered for its
                                #   agent type, diffs cumulative totals into UsageDelta, keyed by
                                #   session id (never pane — finding #10). U4.1 resolves each
                                #   delta's pane/workspace/agent tags via tracker.Sessions()
                                #   (U3.2), not from the delta itself.
  adapters/opencode/opencode.go# OpencodeAdapter: reads opencode.db's session row (cost + 5 token
                                #   counters) read-only; never touches the message table
internal/version/version.go    # release version, synced by .github/workflows/release.yml
herdr-plugin.toml              # plugin manifest: [[build]] → scripts/build.sh, [[startup]] →
                                #   ./bin/herdr-observr (Phase 4 — see Phase status)
scripts/build.sh               # prefers a local Go toolchain; falls back to the latest prebuilt
                                #   release binary, so `herdr plugin install` works with or
                                #   without Go on the target machine
deploy/                        # local demo stack (Phase 6): otel-collector, Prometheus, Loki,
                                #   Tempo, Grafana (compose.yaml + provisioning + one dashboard)
```

Ownership rules embedded in this structure — preserve them when extending:

- **`Subscriber`** exposes no write surface after construction. If you need
  to call an RPC, open a new connection via `client.Call`; never reuse the
  subscription connection.
- **`Tracker`** is guarded by a single `sync.RWMutex` (`Tracker.mu`), and
  that mutex — not a single goroutine — is what actually makes concurrent
  `Apply*` calls safe. In steady state, almost every write does come from
  one place: `app.handleEvent`, called synchronously from `app.Run`'s
  event-loop `case`, plus `EvictExpired` fired from the same loop's own
  ticker. **One write path does not:** `Tracker.Run`'s periodic
  reconciliation goroutine (`go a.tr.Run(ctx, time.Minute, onReport)`) calls
  `onReport` — and, inside it, `a.tr.ApplySeenFlip(sf.PaneID)` — directly
  from *its own* goroutine, not from `app.Run`'s loop. This is safe (the
  mutex serializes it, and `ApplySeenFlip` is written to be a correct no-op
  if a real event raced it and already closed the interval), but it means
  the "single-writer, no other goroutine ever calls `Apply*`" framing that
  used to appear in this file and in `app.go`'s comments was never quite
  true — `Tracker.mu` is the actual invariant holding correctness, the
  event-loop-only framing is only true for `App`-level state (`scope`,
  `a.sub`). If you add unprotected state anywhere near `onReport`, remember
  it can run concurrently with the event loop.
  `Tracker.Run`'s reconciliation loop is otherwise **read-only** on tracker
  state — it diffs (`Diff`/`DiffReport`) and calls `onReport()` with drifts
  and seen-flips; it never mutates the maps directly itself, only through
  the one `ApplySeenFlip` call described above.
- **`app.App.reconnect`** is the only path that closes and replaces the
  subscription, whatever triggers it: a hard error (`sub.Err()`), a drift
  signal from the reconciliation loop, or an uncovered pane surfacing via
  `paneCreatedNeedsResubscribe`. All three funnel through the same
  `resubscribe` channel and the same re-bootstrap: fresh `session.snapshot`
  → `ApplySnapshot` → `SubscribeFromSnapshot` → replace `a.scope`. State
  never trusts a partial gap silently, and coverage never silently goes
  stale. `reconnect` and `paneCreatedNeedsResubscribe` genuinely are
  event-loop-only — they touch `a.sub`/`a.scope`, not `Tracker`.
- **`scope`** is read/written solely on the `app.Run` event-loop goroutine —
  no locking. It exists because the tracker isn't the right owner of "which
  panes does the *subscription* cover" (the tracker also knows about panes
  created after the last subscribe, which is exactly the gap `scope`
  detects).
- **`internal/usage`** follows the same single-owner-goroutine idiom as
  `Tracker`/`Subscriber` (register adapters before `Run`, then only the
  collector's own goroutine touches `cursors`/`adapters`) — but nothing in
  `internal/app` currently constructs a `UsageCollector`, registers the
  `opencode` adapter, or starts `Run`/consumes `Deltas()`. Treat this
  package as a correctly-built, fully-tested library with no caller yet,
  not as a running feature — `go.mod` even lists `modernc.org/sqlite` as a
  direct (non-indirect) dependency purely to compile it in, even though the
  shipped binary never calls it at runtime.

## Phase status (do not trust this section blindly — cross-check the tree)

**This section itself has already gone stale twice** — an earlier revision
claimed Phase 2 and the pane-coverage resubscribe fix were unimplemented
after they had, in fact, landed; the revision after that claimed
`herdr-plugin.toml` didn't exist and Phases 5–6 were both untouched, when
the manifest/build script and the full `deploy/` demo stack were already
merged. Treat every line below as a claim to spot-check against the actual
package and its `_test.go` file, not as ground truth — and for anything
described as "implemented," also check whether it's reachable from
`main.go` (see the Usage/cost telemetry item below for why that second
check matters).

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
  error/EOF (`app.subscribeWithBackoff`, `app.reconnect`) — note the same
  backoff, applied to the *initial* connection, is a finite ~31s budget
  before `App.Run` gives up entirely (finding #1)
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
  resubscribe) and wiped by any `ApplySnapshot` re-baseline.
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
  `app.Run` registers the `herdr.up` proof gauge. Wire fact: the SDK's
  gRPC exporter dials lazily (`grpc.NewClient`) — construction never fails or
  blocks on an unreachable collector; exports fail and are dropped instead
  (this is what Phase 5.4 relies on).
- **3.2 (resource attributes)** — `BuildResource` (in `internal/otel/resource.go`)
  attaches `herdr.machine.id` (persisted UUID from `HERDR_PLUGIN_STATE_DIR`
  via `internal/machineid.LoadOrCreate`, finding #7 / 0.6) and
  `herdr.machine.hostname` (secondary display-only, from the plugin's own OS
  call) onto every exported resource datum. The per-resource
  `herdr.workspace.id`/`herdr.pane.id`/`herdr.agent.id`/`herdr.agent.type`
  attributes listed in the plan's 3.2 belong on metric events/datapoints
  (3.8), not the resource, and are attached there instead.
- **3.3** — `herdr.agent.state.transitions` counter, tagged
  `herdr.agent.type` + previous/new state, bounded cardinality by
  construction (no pane/workspace id). Fed by `Tracker.Transitions()`,
  emitted by `ApplyAgentStatusChanged` and `ApplySeenFlip`; never on
  same-state re-applies, first-seen upserts, re-baseline seeding, or
  close-time flushes.
- **3.4** — `herdr.agent.state.duration` histogram (unit `s`), tagged
  `herdr.agent.type` + state, recorded once per *closed* interval — the
  real transition, the silent done→idle close, and the close-time flush —
  never for first-seen upserts, same-state re-applies, re-baseline seeding,
  or late-status-in-grace (absorbed).
- **3.5** — `herdr.agent.attention_latency` histogram (unit `s`), tagged
  `herdr.agent.type` only (state is `done` by construction), recorded once
  per closed done-but-unseen interval via the same three close sites as 3.4.
- **3.6/3.7** — state-count gauges (`herdr.agent.active/blocked/idle/done/
  unknown`) and `herdr.workspace.agent.concurrent`, wired to
  `Tracker.Counts()` on a single per-collection callback
  (`Telemetry.registerCountGauges`). Each 3.6 gauge emits a global datapoint
  plus one per workspace; 3.7 is per-workspace only (working+blocked sum).
  Both always emit explicit zeros so per-workspace series never go stale —
  the config flag the plan's cardinality caution suggested was deliberately
  dropped in favor of always-emit, revisit-able when Phase 4.2 config lands.
- **3.8** — structured state-change *events* over the OTLP Logs signal
  (`internal/otel/log.go`, `Telemetry.registerLogger`/`recordTransitionEvent`).
  Each genuine transition emits one `log.Record` (event name
  `herdr.agent.state_change`) with `herdr.agent.id` (the PaneID — no
  standalone agent id exists on the wire, finding #3), `herdr.agent.type`,
  `herdr.workspace.id`, previous/new state. `herdr.state.source` is
  deliberately not emitted (verified-absent from pushed events, finding #3).
- **3.9** — short root `herdr.agent.state_change` spans over the OTLP Traces
  signal (`internal/otel/trace.go`, `Telemetry.recordTransitionSpan`),
  started at the transition's observed time and ended immediately — never
  open-ended. Shares the `Transitions()` feed and event name with 3.8.
- **Phase 4 (plugin packaging)** — `herdr-plugin.toml` exists at the repo
  root with `[[build]]` (`scripts/build.sh`, prefers a local Go toolchain
  and falls back to the latest prebuilt release binary) and `[[startup]]`
  (`./bin/herdr-observr`). `HERDR_PLUGIN_STATE_DIR` is read by
  `internal/machineid` for the persisted machine UUID (3.2). Not yet
  confirmed: a live `herdr plugin link`/`enable`/`disable`/`uninstall` pass
  against a real Herdr install (4.4), and the manifest version-mismatch
  test (4.5).
- **Phase 6 (local demo stack)** — `deploy/compose.yaml` brings up an
  OTel Collector, Prometheus, Loki, Tempo, and Grafana (with provisioned
  datasources and one dashboard, `deploy/grafana/dashboards/herdr-observr.json`)
  wired to receive the metrics/logs/traces signals above. Not yet confirmed:
  an end-to-end walkthrough by someone unfamiliar with the project (the
  original 6.5/exit-criteria bar).

**Not yet implemented, or implemented but not wired in** — confirmed by
`grep`, not just absence from the list above:

- **1.2** — self-supervising *external process* wrapper with crash/respawn
  backoff for the `[[startup]]` hook itself. The current backoff in
  `app.subscribeWithBackoff` only covers subscribe/snapshot retries
  *within* a running process — it does not protect against the process
  itself crashing, or against the process giving up after its own ~31s
  initial-connection budget (finding #1). This is the top open item.
- **Usage/cost telemetry (the U-plan)** — `internal/usage` (collector +
  adapter interface) and `internal/usage/adapters/opencode` are fully
  implemented and unit-tested, but **nothing in `internal/app` or `main.go`
  constructs a `UsageCollector`, registers the `opencode` adapter, or
  consumes `Deltas()`.** No cost/token telemetry leaves the process today
  regardless of configuration. Wiring this up is small (construct the
  collector off `a.tr.Panes`, register `opencode.New()`, start
  `go c.Run(ctx)`, add a `case d := <-c.Deltas():` to `App.Run`'s select —
  same shape as the tracker's channels) and is the highest-value next step
  since the hard part (the adapter, the session-id-keyed diffing in finding
  #10) is already done and tested.
- **Phase 5 (reliability hardening)** — no dedicated stress test yet for
  the crash-loop/backoff ceiling (5.1, moot until 1.2 exists), the
  socket-unavailable-at-startup race (5.2 — `subscribeWithBackoff` retries,
  but only up to the finite initial budget in finding #1, not "forever
  until the socket appears"), or the protocol-version-mismatch check on
  connect (5.5). Malformed/unknown event handling (5.3) is effectively
  covered by `parseFrame`'s default case (log and skip) and is exercised in
  `event_test.go`, but there's no standalone Phase-5-labeled test for it.
  OTLP-endpoint-unreachable (5.4) is covered structurally by the lazy-dial
  exporters (see 3.1) but not stress-tested end-to-end.
- **Phase 4 exit criteria** — see the Phase 4 bullet above: the manifest
  and build script exist, but a live install/link/enable/disable pass and
  the version-mismatch test are unconfirmed.
- **Phase 6 exit criteria** — see the Phase 6 bullet above: the stack
  exists, but the "someone unfamiliar can follow the README in 15 minutes"
  bar is unconfirmed.

## Conventions

- **Constructed types over package-level globals.** State lives in
  explicitly constructed structs (`Tracker`, `Subscriber`, `App`, `scope`)
  passed around or closed over — not package globals. This is deliberate:
  it keeps everything independently testable.
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
- **`Tracker.mu` is the real single-writer guarantee, not "only one
  goroutine calls `Apply*`".** Almost every write does come from
  `app.Run`'s event loop, but `Tracker.Run`'s own reconciliation goroutine
  also calls `ApplySeenFlip` directly (see the Architecture section above).
  If you add a new tracker writer, either route it through the event loop
  to preserve the simpler mental model, or make sure it's safe to run
  concurrently with the event loop under `Tracker.mu` the way `ApplySeenFlip`
  is — don't assume the mutex is redundant.
- **Spike before building** on anything touching the live socket protocol.
  Throwaway `cmd/spike-*/main.go` binaries are the expected pattern; delete
  them after extracting the finding.
- **Batch findings before restructuring the plan.** If you're doing
  exploratory/spike work, gather findings first and propose one coherent
  restructuring rather than editing the plan document incrementally as you
  go.
- **Built ≠ running.** Before describing a package as "done," check that
  something in `internal/app`/`main.go` actually calls it. `internal/usage`
  is the cautionary example: complete, tested, and inert.

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

Usage-collector tests (`internal/usage/collector_test.go`,
`internal/usage/adapters/opencode/opencode_test.go`) follow the same
pattern one layer further down: a fixture SQLite file for the opencode
adapter, and a fake `panes`/adapter pair for the collector's diff logic.
These are real, useful tests of code that isn't reachable in production yet
— see the Usage/cost telemetry item above before assuming test coverage
implies the feature runs.

Live-instance verification (anything under "Verify over trust" above)
happens separately, via spike binaries against `HERDR_SOCKET_PATH` pointed
at a real running Herdr — not as part of `go test ./...`.

## Environment

- Go 1.26+
- `HERDR_SOCKET_PATH` — required; path to Herdr's Unix socket
- `HERDR_PLUGIN_STATE_DIR` — where the persisted `herdr.machine.id` UUID
  lives (`internal/machineid`, read by `internal/otel.BuildResource`, 3.2).
  Herdr injects this automatically when it starts the plugin itself; in
  standalone/dev runs, set it yourself or `herdr.machine.id` is omitted
  with a warning.
- `OTEL_EXPORTER_OTLP_ENDPOINT` — OTLP/gRPC collector endpoint for metrics,
  logs, and traces (defaults per the OTel SDK's own convention). Unreachable
  is not a startup failure — see finding #6/3.1's lazy-dial note.
- `OTEL_SERVICE_NAME` — defaults to `herdr-telemetry` (`otel.DefaultServiceName`).
- `OTEL_RESOURCE_ATTRIBUTES` — additional comma-separated resource attributes.
- `XDG_DATA_HOME` — honored by the (currently unwired) opencode usage
  adapter to locate `opencode.db`; falls back to
  `$HOME/.local/share/opencode/opencode.db`.
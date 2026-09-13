# AGENTS.md — herdr-scribe

Guidance for any AI coding agent (or human) working in this repository. Read
this before making changes — it encodes decisions that were already argued
out and verified against a live Herdr instance, so re-litigating them from
first principles will waste time and likely produce a regression.

## What this project is

`herdr-scribe` is a Go plugin for [Herdr](https://github.com/nabutabu/herdr)
(a terminal multiplexer / agent-management tool). It subscribes to Herdr's
live event stream and exports agent runtime *health* telemetry — where
agents are stuck, how long they wait for a human, how much concurrent work
is happening — over OTLP to standard observability backends (Prometheus,
Tempo, Grafana).

**Strict privacy constraint, non-negotiable:** herdr-scribe never transmits
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
access to that plan, read it before starting Phase 0–2 work; it has far more
rationale than fits here. If it is not available to you, the summary below
is the minimum you need.

## Verify over trust

This codebase has already been burned once by trusting stale
docs/assumptions instead of a live Herdr instance. The house rule:

- **Never** implement against remembered/assumed wire shapes. If a change
  touches the socket protocol (event payloads, `session.snapshot` shape,
  RPC behavior), it must be checked against a live Herdr instance (or, at
  minimum, against the already-verified findings recorded in code comments
in `internal/events/event.go`, `internal/snapshot/snapshot.go`, and this
   file) before being trusted.
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
  curl -sL "https://codeload.github.com/nabutabu/herdr-scribe/tar.gz/refs/heads/main" -o repo.tar.gz \
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
   application is the only dedup mechanism (see `tracker.ApplyEvent`, which
   upserts rather than assuming ordering).
4. **The wire frame has no top-level `type` field.** Pushed events arrive as
   `{"event": "pane.agent_status_changed", "data": {...}}`. `event.go`'s
   `parseFrame` is the single source of truth for classifying and
   normalizing a frame in one decode pass — do not reintroduce a separate
   classify-then-normalize split (that was a real double-unmarshal bug that
   got fixed).
5. **`done` is only reachable through genuine agent detection.**
   `pane.report_agent --state` accepts `idle|working|blocked|unknown` only.
   You cannot synthetically drive an agent into `done` — any test or exit
   criterion touching `done`/attention-latency needs a real coding agent
   running under Herdr's own detection, not a scripted RPC sequence.
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
    intentionally not modeled anywhere. **Correction (verified against the
    upstream herdr v0.9.0 source, `src/api/schema/events.rs`,
    `src/app/creation.rs`, `src/app/api/tabs.rs`): tab-lifecycle events DO
    exist on the subscription** — `tab.created`, `tab.closed`, `tab.renamed`
    (all kind-scoped, no pane_id) — and the tracker subscribes to them
    (`internal/events/subscription.go`). The earlier claim that "no
    tab-lifecycle events exist" was an over-conclusion drawn from a
    subscription that never requested the tab kinds; trust the source, and
    re-spike against a live instance before depending on it again. Two
    follow-on wire facts anchored to that source:
    - Closing a tab emits **no `pane.closed` for its panes** — they are
      destroyed silently in `handle_tab_close`. `Tracker.ApplyTabClosed`
      therefore cascades the tab's panes/agents itself (mirroring
      `ApplyWorkspaceClosed`).
    - `tab.closed` on the last tab is immediately followed by
      `workspace.closed`, whose cascade is idempotent against the tab's.

## Architecture (current)

```
main.go                        # entrypoint: signal handling, delegates to internal/app
internal/app/app.go            # process lifecycle: ping → bootstrap snapshot → subscribe →
                                #   event loop → reconnect-on-error → reconciliation-driven resubscribe
internal/app/scope.go           # App's live subscription state: the subscribed-pane set for
                                #   pane.agent_status_changed, seeded by SubscribeFromSnapshot
internal/client/
  client.go                    # Dial, SocketPath (HERDR_SOCKET_PATH), one-shot Call()
  rpc.go                       # Request/Response wire types, WriteFrame/ReadFrame (NDJSON framing)
internal/events/
  subscription.go              # SubscriptionType consts, BuildParams, SubscribeFromSnapshot
                                #   (fetches session.snapshot, opens a pane-scoped subscription,
                                #   returns the subscribed pane IDs it scoped the subscribe to)
  subscriber.go                # Subscriber: dedicated long-lived connection + reader goroutine,
                                #   Events() <-chan NormalizedEvent, Err() <-chan error
  event.go                     # Kind enum, NormalizedEvent, parseFrame (single-pass classify+normalize)
internal/snapshot/
  snapshot.go                  # Response/Snapshot/Workspace/Pane/Tab/Agent structs, Fetch()
internal/tracker/
  tracker.go                   # Tracker: mutex-guarded workspaces/tabs/panes maps.
                                #   ApplyEvent (steady-state writer), ApplySnapshot (bootstrap/re-baseline),
                                #   Diff/DiffReport (read-only comparison), Run (periodic reconcile loop)
```

Ownership rules embedded in this structure — preserve them when extending:

- **`Subscriber`** exposes no write surface after construction. If you need
  to call an RPC, open a new connection via `client.Call`; never reuse the
  subscription connection.
- **`Tracker`** has exactly one writer path in steady state (`ApplyEvent`,
  called from `app.Run`'s event-loop `case`) and one re-baseline path
  (`ApplySnapshot`, called on initial bootstrap and after every reconnect).
  `Tracker.Run`'s reconciliation loop is **read-only** — it diffs and calls
  `onDrift()`, it never mutates tracker state directly. This separation is
  intentional: it keeps a single owner (`app.Run`) responsible for actually
  tearing down and recreating the subscription, so there's no risk of two
  goroutines racing to swap `a.sub`.
- **`app.App.reconnect`** is the only path that closes and replaces the
  subscription, whether triggered by a hard error (`sub.Err()`) or a drift
  signal from the reconciliation loop (`resubscribe` channel). Both trigger
  the same re-bootstrap: fresh `session.snapshot` → `ApplySnapshot` →
  `SubscribeFromSnapshot`, so state never trusts a partial gap silently.

## Phase status (do not trust README.md's "Implemented so far" list blindly — cross-check the tree)

As of the current `main` branch, the following are implemented and tested:

- **1.1** — one-shot NDJSON client (`internal/client`)
- **1.3** — `session.snapshot` fetch/parse (`internal/snapshot.Fetch`)
- **1.4/1.5** — scoped subscription on its own dedicated connection
  (`internal/events.Subscriber`, `BuildParams`)
- **1.6** — single-pass classify+normalize (`internal/events.parseFrame`,
  `NormalizedEvent`)
- **1.7** — reconnect/resubscribe with capped exponential backoff on socket
  error/EOF (`app.subscribeWithBackoff`, `app.reconnect`)
- **1.8** — periodic reconciliation against a fresh snapshot, independent of
  socket state, driving a forced resubscribe on drift
  (`internal/tracker.Tracker.Run`/`reconcile`/`Diff`)

**Not yet implemented:**

- **1.2** — self-supervising *external process* wrapper with crash/respawn
  backoff for the `[[startup]]` hook itself. The current backoff in
  `app.subscribeWithBackoff` only covers subscribe/snapshot retries *within*
  a running process — it does not protect against the process itself
  crashing (finding #1 above). This is the recommended next piece of work.
- Herdr plugin packaging (`herdr-plugin.toml`, Phase 4) — no manifest exists
  in the repo yet.
- Phase 2 (state-duration/attention-latency tracker on top of
  `internal/tracker`), Phase 3 (OTel export), Phases 5–6.

Before claiming a phase item is "done," check the actual tree and tests —
the plan document and README can lag the code (as they currently do for
1.3/1.6/1.7/1.8, all of which were implemented after the last README
update).

## Conventions

- **Constructed types over package-level globals.** State lives in
  explicitly constructed structs (`Tracker`, `Subscriber`, `App`) passed
  around or closed over — not package globals. This is deliberate: it keeps
  everything independently testable and avoids data races between the
  event-consumer goroutine and the reconciliation goroutine.
- **No double-decode.** If you touch wire parsing, keep classify and
  normalize as a single `json.Unmarshal` pass (`parseFrame`). Splitting them
  back into two passes was already tried and reverted.
- **Idempotent state application, not sequence-based dedup.** There is no
  sequence number on the wire (finding #3). `Tracker.ApplyEvent` always
  upserts by id; don't add ordering assumptions.
- **Reconciliation is primary, not a backstop.** It remains the sole
  detector of a silently stalled subscription (no socket error) and of
  missing events in any category — tab membership drift was questionable
  while tab events were assumed absent, but now that `tab.created`/`tab.closed`
  are subscribed, `Tracker.Diff` reports tab-id membership drift too. Don't
  demote `Tracker.Run` to an optional safety net in comments or logic — it is
  load-bearing.
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
follow that pattern — a real `net.Listen("unix", ...)` in a temp dir — rather
than mocking at a higher abstraction level, since framing bugs (NDJSON
boundaries, ack-vs-event classification) only show up at that layer.

Live-instance verification (anything under "Verify over trust" above)
happens separately, via spike binaries against `HERDR_SOCKET_PATH` pointed
at a real running Herdr — not as part of `go test ./...`.

## Environment

- Go 1.26+
- `HERDR_SOCKET_PATH` — required; path to Herdr's Unix socket
- `HERDR_PLUGIN_STATE_DIR` — where the persisted `herdr.machine.id` UUID
  will live once Phase 4 packaging lands (not yet wired up)
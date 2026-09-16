# herdr-observr

Agent runtime telemetry for Herdr exported over OpenTelemetry, without ever
transmitting what the agents are actually doing.

## Status

Implemented so far:

- **1.1 — Minimal socket connection + `ping`.** A reusable, one-shot
  newline-delimited JSON RPC client over the Herdr Unix socket, exercised by a
  tiny `ping` demo binary.
- **1.5 — Subscription on its own dedicated connection.** A `Subscriber` that
  opens a fresh connection exclusively for `events.subscribe`, owns it with a
  dedicated reader task, and logs every pushed event verbatim. The connection
  is write-once by design: RPCs must open their own short-lived connection
  (`client.Call`) and can never reuse this one.
- **1.6 — Normalize pushed events into an internal type.** A stateless
  `Normalize` maps each raw event into a `NormalizedEvent` (workspace
  created/closed, pane created/closed, agent detected, agent status changed).
  Only the confirmed wire fields are modeled — no sequence number, revision,
  or display fields. `agent_status_changed` carries no previous state on the
  wire, so it is supplied by a caller-provided lookup; unrecognized status
  values degrade to `unknown`, and malformed/unknown events return an error
  for the caller to log and skip.

## Requirements

- Go 1.26+
- A running Herdr instance exposing its Unix socket

## Build

```sh
go build -o herdr-observr .
```

## Run

The binary sends `ping` over Herdr's Unix socket and confirms `pong`:

```sh
./herdr-observr
```

The socket path is read from `HERDR_SOCKET_PATH` and NEEDS to be set in order to run correctly

```sh
HERDR_SOCKET_PATH=/path/to/your/herdr.sock ./herdr-observr
```

## Test

```sh
go test ./...
```

Tests run against a stub Unix socket server and do not require a live Herdr
instance.

## Layout

```
main.go                      # ping + events.subscribe demo binary
internal/client/             # low-level NDJSON RPC client over the Unix socket
  client.go                  # one-shot Call helper, Dial, socket path resolution
  rpc.go                     # wire types, frame encode/decode
  client_test.go             # framing + end-to-end tests against a stub server
internal/events/             # scoped event subscription payload + long-lived subscriber
  subscription.go            # event names, BuildParams subscription payload
  subscription_test.go       # BuildParams tests
  subscriber.go              # dedicated-connection Subscriber with verbatim event logging
  subscriber_test.go         # subscriber tests against a stub server
  event.go                   # NormalizedEvent + stateless Normalize mapping
  event_test.go              # normalizer tests
PLAN.md                      # full multi-phase implementation plan
```

## License

MIT — see [`LICENSE`](LICENSE).
# herdr-observr

Agent runtime telemetry for Herdr exported over OpenTelemetry.

A Herdr plugin (`herdr-plugin.toml`): a telemetry daemon subscribed to Herdr's
live event stream that tracks agent runtime *health* — where agents are stuck,
how long they wait for a human, how much concurrent work is happening — and
exports it over OTLP to standard observability backends (Prometheus, Tempo,
Grafana). Strict privacy constraint, non-negotiable: only lifecycle/state
metadata (ids, states, timestamps) leaves the process, never pane/terminal
content or agent transcripts.

## Requirements

- Go 1.26+
- A running Herdr instance exposing its Unix socket

## Build

```sh
make build   # → ./bin/herdr-observr
```

## Install

As a Herdr plugin (requires **herdr ≥ 0.7.5**):

```sh
herdr plugin install nabutabu/herdr-observr
```

Herdr clones the repo, runs the manifest's `[[build]]` step, and registers the
startup hook. That step **prefers a local Go toolchain** (an exact build of the
source) and **falls back to downloading the latest prebuilt release binary**, so
it works **with or without Go**. The daemon then starts automatically with each
Herdr boot. Manage it with `herdr plugin list`, `herdr plugin disable`, and
`herdr plugin uninstall nabutabu.herdr-observr`.

**Local development:** build the binary and link your checkout in place:

```sh
make plugin-link     # or: herdr plugin link . && herdr plugin enable
```

### Just the binary

If you'd rather have `herdr-observr` on your `PATH` (e.g. to run
`herdr-observr version`), prebuilt binaries are published on every release:

```sh
# Homebrew (the repo is its own tap)
brew tap nabutabu/herdr-observr https://github.com/nabutabu/herdr-observr
brew install nabutabu/herdr-observr/herdr-observr

# or the install script (Linux/macOS, no Homebrew)
curl -fsSL https://raw.githubusercontent.com/nabutabu/herdr-observr/main/install.sh | sh
```

The binary on its own doesn't register the plugin with Herdr — use
`herdr plugin install` (above) for that. Every merge to `main` cuts a new
release with cross-compiled binaries.

## Run

The binary runs the telemetry daemon; with no arguments it starts the event
loop and keeps exporting until signalled. `herdr-observr version` prints the
release version.

```sh
HERDR_SOCKET_PATH=/path/to/your/herdr.sock ./bin/herdr-observr
```

The socket path is read from `HERDR_SOCKET_PATH` and NEEDS to be set in order to
run correctly (Herdr injects it when the plugin starts the daemon itself).

## Configuration

herdr-observr reads a `.env` config file from the plugin's config directory
(`HERDR_PLUGIN_CONFIG_DIR`) — Herdr creates the directory and your user
editable config lives there. To find it:

```sh
herdr plugin config-dir nabutabu.herdr-observr
```

Copy [`example.env`](example.env) into that directory as `.env` and fill in
what you need. Keys set in the file win over the process environment, which
falls back to the OTel SDK defaults:

| Key | Default | Purpose |
| --- | --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `OTEL_EXPORTER_OTLP_ENDPOINT` env / SDK default | Collector base URL (metrics, logs, traces) |
| `OTEL_EXPORTER_OTLP_INSECURE` | `true` (plaintext) | Set `false` to force TLS — the only supported TLS switch |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | SDK default | Per-export timeout |
| `OTEL_EXPORTER_OTLP_HEADERS` | SDK default | Extra gRPC metadata (auth/proxy) |
| `OTEL_SERVICE_NAME` | `herdr-telemetry` | `service.name` resource attribute |
| `OTEL_RESOURCE_ATTRIBUTES` | env / empty | Extra resource attributes |
| `HERDR_OBSRVR_MACHINE_ID` | persisted UUID | Fixed cross-machine identity override |
| `HERDR_OBSRVR_EMIT_PER_WORKSPACE_GAUGES` | `true` | Set `false` to drop per-workspace gauge series (cardinality opt-out) |

Malformed lines and unknown keys are skipped with a warning — a bad config file
never stops the daemon (the process degrades to env defaults). Endpoint and the
default `OTEL_*` env vars still work exactly as before.

`HERDR_PLUGIN_STATE_DIR` is where the daemon persists its generated machine
UUID — the `machine-id` file read at startup into the `herdr.machine.id` OTLP
resource attribute that rides on every exported datapoint. It is the dashboard's
per-machine grouping/filter key: deliberately a plugin-persisted UUID, never the
hostname (display-only) or Herdr's own ids (scoped to a single server), and
stable across restarts by design. When Herdr starts the daemon itself it
injects this variable for you, pointing at a per-plugin directory under Herdr's
own state dir (e.g. `~/.local/state/herdr/plugins/nabutabu.herdr-observr`, or
under `$XDG_STATE_HOME/herdr/...`), and creates it before launching — so
plugin-installed runs need no extra setup at all. In standalone runs
(`./bin/herdr-observr`), set it to a persistent directory of your own; if it's
unset, the daemon logs a warning and omits `herdr.machine.id`, leaving your
dashboard with no series grouped by machine.

## Release

Every merge to `main` cuts a new release (`.github/workflows/release.yml`):

1. Reads the version from `internal/version/version.go`. A hand edit to that
   file is respected as-is (a deliberate major/minor bump); otherwise the patch
   number is auto-incremented.
2. Syncs that version into both `internal/version/version.go` and
   `herdr-plugin.toml`, commits `[skip ci]`, and pushes.
3. Tags `v<x.y.z>` and runs GoReleaser, which cross-compiles
   linux/darwin × amd64/arm64 binaries, attaches the archives + `checksums.txt`
   to a GitHub Release, and pushes the updated Homebrew formula into `Formula/`.

## Test

```sh
go test ./...
# or the CI command: make test (with -race)
```

Tests run against a stub Unix socket server and do not require a live Herdr
instance. See `AGENTS.md` for the implementation status and wire-protocol
findings.

## Layout

```
main.go                      # entrypoint: version subcommand, delegates to internal/app
internal/app/                # process lifecycle: bootstrap snapshot → subscribe → event
                             #   loop → reconnect/resubscribe (reconciliation-driven and
                             #   pane-coverage-driven) → teardown
internal/client/             # one-shot NDJSON RPC client over the Unix socket
internal/events/             # scoped event subscription, dedicated-connection subscriber,
                             #   single-pass classify+normalize (nine Kind values)
internal/snapshot/           # session.snapshot fetch/parse
internal/tracker/            # state machine, duration/attention/concurrency accounting,
                             #   grace-window retention, periodic reconciliation (read-only)
internal/otel/               # OTLP/gRPC metrics, logs, traces providers + resource builder
internal/config/             # 4.2: .env parsing (HERDR_PLUGIN_CONFIG_DIR) and typed config
internal/machineid/          # persisted herdr.machine.id UUID
internal/version/            # release version (synced by .github/workflows/release.yml)
deploy/                      # local demo stack: collector, Prometheus, Tempo, Loki, Grafana
example.env                  # commented sample plugin config ($HERDR_PLUGIN_CONFIG_DIR/.env)
herdr-plugin.toml            # the Herdr plugin manifest ([[build]] + [[startup]])
PLAN.md                      # full multi-phase implementation plan
```

## License

MIT — see [`LICENSE`](LICENSE).
## Summary

<!-- What does this PR do? Link to the issue it resolves, if any. -->

## Changes

<!-- High-level list of changes, one bullet per logical unit. -->

## How tested

<!-- How did you verify this change? e.g. `go test -race ./...`, a live check
against a running Herdr instance, a manual collector take, etc. -->

## Checklist

- [ ] All tests pass locally (`make test` or `go test -race ./...`)
- [ ] `go.mod` is tidy (`make tidy`)
- [ ] No pane content, terminal output, or agent transcripts included in this diff
- [ ] No new high-cardinality attributes added to OTLP metrics/events (or cardinality is bounded)
- [ ] Metric/event attribute names follow the existing conventions in `internal/otel/`
- [ ] Wire-protocol changes were checked against a live Herdr instance and documented (see `AGENTS.md`)
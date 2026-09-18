# Contributing to herdr-observr

Thanks for considering a contribution. This project is small but production-minded:
please read `AGENTS.md` before writing code — it encodes wire-protocol findings
and architecture decisions that took live verification against a running Herdr
instance to settle. Re-litigating them wastes everyone's time.

## Prerequisites

- Go 1.26+
- `make` (optional, but used in the examples below)
- A working Herdr install for local live testing (see `AGENTS.md`)

## Getting started

```sh
make build        # build ./bin/herdr-observr
make test         # go test -race ./...  (what CI runs)
make test-short   # quick iteration loop, skips slow tests
make vet          # go vet ./...
make tidy         # keep go.mod/go.sum tidy
make plugin-link  # link this checkout as a local herdr plugin (dev)
```

The full test suite runs against stub Unix-socket servers and does not require a
live Herdr instance. Live socket-behavior verification happens separately via
spike binaries — see `AGENTS.md`'s "Verify over trust" section.

## How contributions land

1. **Branch off `main`** and make your changes.
2. **Keep commits conventional** (`feat:`, `fix:`, `chore:`, `docs:`, `test:`, ...).
3. **Open a pull request.** `main` is protected: every PR must pass the required
   CI check (`test (ubuntu-latest)`) and is merged only through the merge queue.
   Code from external contributors should get a review before merging.
4. **Release is separate from merge.** Merging to `main` never cuts a release.
   Releases are kicked off deliberately from the workflow_dispatch "Release"
   action (see `.github/workflows/release.yml`).

## Wire protocol and privacy

Anything that touches the Herdr socket protocol must be checked against a live
Herdr instance before being trusted. Relatedly, herdr-observr has a strict,
non-negotiable constraint: it never transmits *what* an agent is doing — no pane
content, no terminal output, no transcripts. Only lifecycle metadata (ids,
states, timestamps) leaves the process. Keep that in mind for every change;
full details are in `AGENTS.md`.
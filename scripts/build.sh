#!/bin/sh
#
# Build herdr-observr for `herdr plugin install`. herdr runs this as the
# manifest's [[build]] step after cloning the repo, in the plugin root, with no
# plugin context and no guaranteed Go toolchain.
#
# Prefer a local Go toolchain — it builds the exact cloned source. When Go is
# absent, fall back to downloading the latest prebuilt release binary via
# install.sh, so installing the plugin works without Go. Either way the result
# is ./bin/herdr-observr, which the manifest's startup hook invokes.

set -eu

mkdir -p bin

if command -v go >/dev/null 2>&1; then
	echo "herdr-observr: building from source (go build)…" >&2
	exec go build -o bin/herdr-observr .
fi

echo "herdr-observr: no Go toolchain found — downloading the latest prebuilt binary…" >&2
INSTALL_DIR="$(pwd)/bin" sh install.sh
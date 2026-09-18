// Package version exposes herdr-observr's release version. Keep this file tiny —
// it is the single source of truth a future release pipeline bumps, so the
// one-line diff stays trivial to review.
package version

// Version is the herdr-observr release version, printed by
// `herdr-observr version`. Edit the major or minor by hand to cut a larger
// release; the release workflow auto-bumps the patch on every push to main.
const Version = "0.0.7"
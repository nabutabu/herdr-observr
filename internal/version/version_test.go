// Tests for the tiny version package. The Version constant is the single source
// of truth a release pipeline bumps, so we fail loudly if its shape ever drifts
// away from semver.

package version

import (
	"strconv"
	"strings"
	"testing"
)

func TestVersionNotEmpty(t *testing.T) {
	if Version == "" {
		t.Fatal("Version is empty")
	}
}

func TestVersionIsSemver(t *testing.T) {
	parts := strings.Split(Version, ".")
	if len(parts) != 3 {
		t.Fatalf("Version %q is not in x.y.z form (got %d parts)", Version, len(parts))
	}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			t.Fatalf("Version %q part %d (%q) is not an integer: %v", Version, i, p, err)
		}
		if n < 0 {
			t.Fatalf("Version %q part %d (%q) is negative", Version, i, p)
		}
	}
}
package machineid

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

// readFile returns the raw bytes of the machine-id file, or an error if it is
// absent or unreadable.
func readFile(t *testing.T, stateDir string) []byte {
	t.Helper()
	buf, err := os.ReadFile(filepath.Join(stateDir, fileName))
	if err != nil {
		t.Fatalf("reading machine-id file: %v", err)
	}
	return buf
}

func TestLoadOrCreateGeneratesAndPersists(t *testing.T) {
	dir := t.TempDir()

	id, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}
	if _, err := uuid.Parse(id); err != nil {
		t.Fatalf("returned id %q is not a UUID: %v", id, err)
	}

	buf := readFile(t, dir)
	if got := string(buf); got != id {
		t.Errorf("file content = %q, want %q", got, id)
	}

	fi, err := os.Stat(filepath.Join(dir, fileName))
	if err != nil {
		t.Fatalf("stat machine-id file: %v", err)
	}
	if fi.Mode().Perm() != filePerm {
		t.Errorf("file mode = %o, want %o", fi.Mode().Perm(), filePerm)
	}
}

func TestLoadOrCreateStableAcrossCalls(t *testing.T) {
	dir := t.TempDir()

	first, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatalf("first LoadOrCreate: %v", err)
	}
	second, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatalf("second LoadOrCreate: %v", err)
	}
	if second != first {
		t.Errorf("id changed across calls: first %q, second %q", first, second)
	}
	if got := string(readFile(t, dir)); got != first {
		t.Errorf("file content changed: %q, want %q", got, first)
	}
}

func TestLoadOrCreateCreatesMissingStateDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "state")

	id, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatalf("LoadOrCreate with missing dir: %v", err)
	}
	if _, err := uuid.Parse(id); err != nil {
		t.Fatalf("returned id %q is not a UUID: %v", id, err)
	}
	if _, err := os.Stat(filepath.Join(dir, fileName)); err != nil {
		t.Errorf("machine-id file not present after create: %v", err)
	}
}

func TestLoadOrCreateRegeneratesGarbage(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, fileName), []byte("not-a-uuid"), filePerm); err != nil {
		t.Fatalf("seeding garbage file: %v", err)
	}

	id, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatalf("LoadOrCreate must regenerate rather than fail: %v", err)
	}
	if _, err := uuid.Parse(id); err != nil {
		t.Fatalf("regenerated id %q is not a UUID: %v", id, err)
	}
	if got := string(readFile(t, dir)); got != id {
		t.Errorf("file content = %q, want regenerated %q", got, id)
	}
}

func TestLoadOrCreateRegeneratesTruncatedUUID(t *testing.T) {
	dir := t.TempDir()
	truncated := "550e8400-e29b-41d4"
	if err := os.WriteFile(filepath.Join(dir, fileName), []byte(truncated), filePerm); err != nil {
		t.Fatalf("seeding truncated UUID: %v", err)
	}

	id, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatalf("LoadOrCreate must regenerate rather than fail: %v", err)
	}
	if _, err := uuid.Parse(id); err != nil {
		t.Fatalf("regenerated id %q is not a UUID: %v", id, err)
	}
	if got := string(readFile(t, dir)); got != id {
		t.Errorf("file content = %q, want regenerated %q", got, id)
	}
}

func TestLoadOrCreateAcceptsWhitespacePaddedUUID(t *testing.T) {
	dir := t.TempDir()
	seed := uuid.New().String()
	// A hand-edited file may carry an extra newline; the reader must trim it.
	if err := os.WriteFile(filepath.Join(dir, fileName), []byte("\n  "+seed+"  \r\n"), filePerm); err != nil {
		t.Fatalf("seeding padded UUID: %v", err)
	}

	id, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}
	if id != seed {
		t.Errorf("id = %q, want seeded %q", id, seed)
	}
}

func TestLoadOrCreateStaticFromValidFile(t *testing.T) {
	dir := t.TempDir()
	seed := uuid.New().String()
	seedBytes := []byte(seed)
	path := filepath.Join(dir, fileName)
	if err := os.WriteFile(path, seedBytes, filePerm); err != nil {
		t.Fatalf("seeding valid UUID: %v", err)
	}

	id, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}
	if id != seed {
		t.Errorf("id = %q, want seeded %q", id, seed)
	}
	// The happy load path must not rewrite the file (checked by comparing
	// contents rather than mtime, which can collide on fast writes).
	if got := string(readFile(t, dir)); got != seed {
		t.Errorf("file was rewritten: %q, want unchanged %q", got, seed)
	}
}

func TestLoadOrCreateEmptyStateDir(t *testing.T) {
	if _, err := LoadOrCreate(""); err == nil {
		t.Fatal("LoadOrCreate(\"\") = nil error, want error")
	}
}

func TestLoadOrCreateBlockedStateDir(t *testing.T) {
	// A file occupying the state-dir path makes MkdirAll fall over even when
	// running as root (not a permission-denied test, which root would bypass).
	dir := t.TempDir()
	block := filepath.Join(dir, "block")
	if err := os.WriteFile(block, []byte("x"), 0600); err != nil {
		t.Fatalf("creating blocking file: %v", err)
	}

	if _, err := LoadOrCreate(filepath.Join(block, "state")); err == nil {
		t.Fatal("LoadOrCreate with blocked dir = nil error, want error")
	}
}

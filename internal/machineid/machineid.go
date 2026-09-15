package machineid

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/google/uuid"
)

const (
	// fileName is the name of the persisted machine-id file inside the plugin
	// state dir.
	fileName = "machine-id"

	// filePerm keeps the id readable only by the owning process. The value is a
	// non-secret grouping label, but there is no reason to share it.
	filePerm = 0600

	// dirPerm is applied only when the state dir does not yet exist.
	dirPerm = 0700
)

// generate writes a fresh UUIDv4 into <stateDir>/machine-id. The directory is
// created if absent. writeError returns the generated id alongside the write
// error so callers can decide how to degrade.
func generate(stateDir string) (string, error) {
	u := uuid.New()
	if err := os.MkdirAll(stateDir, dirPerm); err != nil {
		return "", fmt.Errorf("creating machine id state dir %s: %w", stateDir, err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, fileName), []byte(u.String()), filePerm); err != nil {
		return "", fmt.Errorf("writing machine id to %s: %w", filepath.Join(stateDir, fileName), err)
	}
	return u.String(), nil
}

// LoadOrCreate returns the persisted herdr.machine.id, generating and
// persisting a fresh UUIDv4 on first run and regenerating (with a warning)
// when the file is unreadable or holds an invalid UUID, so a truncated write
// cannot poison cross-machine grouping.
//
// stateDir is the value of HERDR_PLUGIN_STATE_DIR; an empty value returns an
// error without touching the filesystem. Only one process per machine writes
// this file, so no locking is needed. Callers treat any error as
// warn-and-omit rather than failing the process.
func LoadOrCreate(stateDir string) (string, error) {
	if stateDir == "" {
		return "", errors.New("machine id: state dir is empty")
	}

	path := filepath.Join(stateDir, fileName)

	buf, err := os.ReadFile(path)
	switch {
	case err == nil:
		if id, perr := uuid.ParseBytes(bytes.TrimSpace(buf)); perr == nil {
			return id.String(), nil
		}
		slog.Warn("machine id file holds an invalid UUID; regenerating", "path", path)
	case errors.Is(err, fs.ErrNotExist):
		// Expected on first run; a missing file is the create path, not an
		// error. Read errors other than not-exist (permissions, io) return
		// below.
	default:
		return "", fmt.Errorf("reading machine id %s: %w", path, err)
	}

	return generate(stateDir)
}

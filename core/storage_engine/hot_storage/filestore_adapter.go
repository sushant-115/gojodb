// Package hotstorage provides adapters for the hot storage tier.
// The FileStoreAdapter is backed by the local filesystem and serves as the
// default hot-tier implementation.
package hotstorage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// HotStorageAdapter is the hot-tier storage adapter.  It stores individual
// logical units as flat files under a root directory.
type HotStorageAdapter struct {
	rootDir string
}

// NewHotStorageAdapter creates an adapter that uses rootDir as the storage root.
// The directory is created with 0755 permissions if it does not already exist.
func NewHotStorageAdapter(rootDir string) (*HotStorageAdapter, error) {
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, fmt.Errorf("hotstorage: create root dir %q: %w", rootDir, err)
	}
	return &HotStorageAdapter{rootDir: rootDir}, nil
}

// Write persists data for the given logical unit ID.  The file is written
// atomically using a temp-file + rename pattern so partial writes are never
// visible.
func (a *HotStorageAdapter) Write(unitID string, data []byte) (locationPointer string, err error) {
	dir := filepath.Join(a.rootDir, namespace(unitID))
	if err = os.MkdirAll(dir, 0755); err != nil {
		return "", fmt.Errorf("hotstorage: mkdir %q: %w", dir, err)
	}

	dest := filepath.Join(dir, sanitize(unitID))
	tmp := dest + ".tmp"

	f, err := os.Create(tmp) // #nosec G304 — operator-controlled path
	if err != nil {
		return "", fmt.Errorf("hotstorage: create tmp: %w", err)
	}
	if _, err = f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return "", fmt.Errorf("hotstorage: write: %w", err)
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return "", fmt.Errorf("hotstorage: fsync: %w", err)
	}
	_ = f.Close()
	if err = os.Rename(tmp, dest); err != nil {
		_ = os.Remove(tmp)
		return "", fmt.Errorf("hotstorage: rename: %w", err)
	}
	return dest, nil
}

// Read loads and returns the data for the given logical unit ID.
func (a *HotStorageAdapter) Read(unitID string) ([]byte, error) {
	path := filepath.Join(a.rootDir, namespace(unitID), sanitize(unitID))
	f, err := os.Open(path) // #nosec G304
	if err != nil {
		return nil, fmt.Errorf("hotstorage: open %q: %w", unitID, err)
	}
	defer f.Close()
	return io.ReadAll(f)
}

// Delete removes the unit from hot storage.
func (a *HotStorageAdapter) Delete(unitID string) error {
	path := filepath.Join(a.rootDir, namespace(unitID), sanitize(unitID))
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// Exists reports whether the given unit ID has data in hot storage.
func (a *HotStorageAdapter) Exists(unitID string) bool {
	path := filepath.Join(a.rootDir, namespace(unitID), sanitize(unitID))
	_, err := os.Stat(path)
	return err == nil
}

// GetAdapterType returns a human-readable identifier.
func (a *HotStorageAdapter) GetAdapterType() string { return "filestore" }

// namespace extracts a one-level shard prefix from unitID (first 2 chars) to
// avoid a flat directory with millions of files.
func namespace(id string) string {
	if len(id) < 2 {
		return "_"
	}
	return id[:2]
}

// sanitize escapes the unit ID for use as a filename.
func sanitize(id string) string {
	// Replace path separators; other characters are assumed safe because unit
	// IDs are internally generated.
	safe := make([]byte, len(id))
	for i := 0; i < len(id); i++ {
		c := id[i]
		if c == '/' || c == '\\' || c == ':' {
			safe[i] = '_'
		} else {
			safe[i] = c
		}
	}
	return string(safe)
}

// Package restore implements snapshot-based crash recovery for GojoDB.
//
// A restore operation:
//  1. Reads the backup Manifest to discover snapshot files and snapshot LSNs.
//  2. Streams each snapshot chunk-by-chunk into the matching IndexManager via
//     ApplySnapshot.
//  3. Verifies the final LSN reported by each manager matches (or exceeds) the
//     snapshot LSN recorded in the manifest.
//
// WAL replay on top of the snapshot is handled automatically when the node
// server restarts: the TransactionManager's ARIES recovery re-applies any WAL
// records whose LSN is greater than the snapshot LSN.
package restore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.uber.org/zap"
)

// Restorer is implemented by any index manager that can apply a snapshot.
type Restorer interface {
	// Name identifies this manager.
	Name() string
	// ApplySnapshot applies streamed snapshot chunks.
	ApplySnapshot(ctx context.Context, snapshotID string, chunkChan <-chan []byte) error
	// GetLatestLSN returns the manager's LSN after the snapshot has been applied.
	GetLatestLSN() uint64
}

// Source is a readable backup source.
// Implementations may be local-filesystem, S3, GCS, etc.
type Source interface {
	// Read opens the file at path and returns a ReadCloser.
	Read(ctx context.Context, path string) (io.ReadCloser, error)
}

// LocalSource reads backups from a local directory.
type LocalSource struct {
	BaseDir string
}

// Read implements Source for local filesystem.
func (ls *LocalSource) Read(ctx context.Context, path string) (io.ReadCloser, error) {
	fullPath := filepath.Join(ls.BaseDir, path)
	return os.Open(fullPath) // #nosec G304 – path is constructed internally
}

// Manifest mirrors the backup.Manifest type to avoid circular imports.
type Manifest struct {
	BackupID    string            `json:"backup_id"`
	NodeID      string            `json:"node_id"`
	SnapshotLSN map[string]uint64 `json:"snapshot_lsn"`
	Files       []string          `json:"files"`
}

// Manager orchestrates restores.
type Manager struct {
	logger    *zap.Logger
	restorers map[string]Restorer // keyed by manager Name()
	source    Source
}

// NewManager creates a restore Manager.
func NewManager(logger *zap.Logger, restorers []Restorer, source Source) *Manager {
	m := &Manager{
		logger:    logger.Named("restore"),
		restorers: make(map[string]Restorer, len(restorers)),
		source:    source,
	}
	for _, r := range restorers {
		m.restorers[r.Name()] = r
	}
	return m
}

// Restore applies the backup identified by backupID to all registered restorers.
func (m *Manager) Restore(ctx context.Context, backupID string) error {
	m.logger.Info("starting restore", zap.String("backupID", backupID))

	// 1. Load manifest.
	manifestPath := filepath.Join(backupID, "manifest.json")
	manifestRC, err := m.source.Read(ctx, manifestPath)
	if err != nil {
		return fmt.Errorf("restore: open manifest: %w", err)
	}
	defer manifestRC.Close()

	var manifest Manifest
	if err = json.NewDecoder(manifestRC).Decode(&manifest); err != nil {
		return fmt.Errorf("restore: decode manifest: %w", err)
	}

	m.logger.Info("manifest loaded", zap.String("backupID", manifest.BackupID), zap.String("nodeID", manifest.NodeID))

	// 2. Apply each snapshot.
	for managerName, snapshotLSN := range manifest.SnapshotLSN {
		restorer, ok := m.restorers[managerName]
		if !ok {
			m.logger.Warn("no restorer registered for manager, skipping", zap.String("manager", managerName))
			continue
		}

		snapPath := filepath.Join(backupID, "snapshots", managerName+".snap")
		snapRC, err := m.source.Read(ctx, snapPath)
		if err != nil {
			return fmt.Errorf("restore: open snapshot for %s: %w", managerName, err)
		}

		chunkCh := make(chan []byte, 64)
		errCh := make(chan error, 1)

		go func(rc io.ReadCloser) {
			defer close(chunkCh)
			defer rc.Close()
			buf := make([]byte, 32*1024)
			for {
				n, readErr := rc.Read(buf)
				if n > 0 {
					chunk := make([]byte, n)
					copy(chunk, buf[:n])
					chunkCh <- chunk
				}
				if readErr == io.EOF {
					return
				}
				if readErr != nil {
					errCh <- readErr
					return
				}
			}
		}(snapRC)

		snapshotID := fmt.Sprintf("restore-%s-%s", backupID, managerName)
		if err = restorer.ApplySnapshot(ctx, snapshotID, chunkCh); err != nil {
			return fmt.Errorf("restore: ApplySnapshot for %s: %w", managerName, err)
		}
		// Drain errCh (non-blocking since the goroutine writes at most one value).
		select {
		case streamErr := <-errCh:
			if streamErr != nil {
				return fmt.Errorf("restore: stream error for %s: %w", managerName, streamErr)
			}
		default:
		}

		appliedLSN := restorer.GetLatestLSN()
		m.logger.Info("snapshot applied",
			zap.String("manager", managerName),
			zap.Uint64("snapshotLSN", snapshotLSN),
			zap.Uint64("appliedLSN", appliedLSN))

		if appliedLSN < snapshotLSN {
			return fmt.Errorf("restore: manager %s LSN %d is behind snapshot LSN %d after restore",
				managerName, appliedLSN, snapshotLSN)
		}
	}

	m.logger.Info("restore completed", zap.String("backupID", backupID))
	return nil
}

// VerifyManifest reads a manifest and returns a summary without applying anything.
func VerifyManifest(ctx context.Context, source Source, backupID string) (*Manifest, error) {
	manifestPath := filepath.Join(backupID, "manifest.json")
	rc, err := source.Read(ctx, manifestPath)
	if err != nil {
		return nil, fmt.Errorf("verify: open manifest: %w", err)
	}
	defer rc.Close()

	manifestBytes, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("verify: read manifest: %w", err)
	}

	var manifest Manifest
	if err = json.NewDecoder(bytes.NewReader(manifestBytes)).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("verify: decode manifest: %w", err)
	}
	return &manifest, nil
}

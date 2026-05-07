// Package backup provides full-snapshot and incremental (WAL-delta) backup
// utilities for GojoDB storage nodes.
//
// A Backup consists of:
//  1. A snapshot of every registered Snapshotter (serialised via CreateSnapshot).
//  2. The WAL tail from the snapshot LSN up to the moment the backup completes.
//
// Backups can be written to local disk or to any Sink implementation
// (S3, GCS, …).  The produced layout is:
//
//	<backupID>/
//	  manifest.json
//	  snapshots/<manager-name>.snap
//	  wal/<segment-filename>
package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
)

// Snapshotter is implemented by any index manager that can produce a snapshot.
type Snapshotter interface {
	// Name uniquely identifies this manager (used as the snapshot filename stem).
	Name() string
	// CreateSnapshot streams the full snapshot to w and returns the LSN that was
	// current when the snapshot was taken.
	CreateSnapshot(ctx context.Context, snapshotID string, w io.Writer) (lsn uint64, err error)
	// GetLatestLSN returns the manager's current LSN without creating a snapshot.
	GetLatestLSN() uint64
}

// WALArchiver is implemented by the WAL LogManager to expose the segment files
// needed to complete a backup.
type WALArchiver interface {
	// GetSegmentFilesForBackup returns the absolute paths of all WAL segment
	// files that contain records at or after fromLSN.
	GetSegmentFilesForBackup(fromLSN uint64) ([]string, error)
}

// Sink is a write-only storage backend (local directory, S3, GCS, …).
type Sink interface {
	Write(ctx context.Context, path string, r io.Reader) error
}

// Source is a read-only storage backend used by VerifyBackup.
type Source interface {
	Read(ctx context.Context, path string) (io.ReadCloser, error)
}

// LocalSink writes backup artifacts to a local directory tree.
type LocalSink struct {
	BaseDir string
}

// Write implements Sink for a local filesystem.
func (s *LocalSink) Write(ctx context.Context, path string, r io.Reader) error {
	fullPath := filepath.Join(s.BaseDir, path)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0750); err != nil {
		return err
	}
	f, err := os.Create(fullPath) // #nosec G304 — path is constructed internally
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, r)
	return err
}

// Read implements Source for a local filesystem.
func (s *LocalSink) Read(_ context.Context, path string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(s.BaseDir, path)) // #nosec G304
}

// Manifest records everything needed to replay a backup.
type Manifest struct {
	BackupID    string            `json:"backup_id"`
	NodeID      string            `json:"node_id"`
	Timestamp   time.Time         `json:"timestamp"`
	SnapshotLSN map[string]uint64 `json:"snapshot_lsn"` // manager name → snapshot LSN
	Files       []string          `json:"files"`
	Checksums   map[string]string `json:"checksums"` // relative path → hex SHA-256
}

// Manager creates and verifies backups.
type Manager struct {
	logger       *zap.Logger
	nodeID       string
	snapshotters []Snapshotter
	walArchiver  WALArchiver
	sink         Sink
}

// NewManager creates a backup Manager.
// walArchiver may be nil; in that case the WAL tail is not included.
func NewManager(
	logger *zap.Logger,
	nodeID string,
	snapshotters []Snapshotter,
	walArchiver WALArchiver,
	sink Sink,
) *Manager {
	return &Manager{
		logger:       logger.Named("backup"),
		nodeID:       nodeID,
		snapshotters: snapshotters,
		walArchiver:  walArchiver,
		sink:         sink,
	}
}

// Backup creates a full snapshot backup and returns the backupID.
//
// The layout written to the Sink is:
//
//	<backupID>/manifest.json
//	<backupID>/snapshots/<name>.snap   (one per Snapshotter)
//	<backupID>/wal/<segment-file>      (WAL segments from minSnapshotLSN onward)
func (m *Manager) Backup(ctx context.Context) (string, error) {
	ts := time.Now().UTC()
	backupID := fmt.Sprintf("backup-%s-%d", m.nodeID, ts.UnixNano())
	m.logger.Info("starting backup", zap.String("backupID", backupID))

	manifest := Manifest{
		BackupID:    backupID,
		NodeID:      m.nodeID,
		Timestamp:   ts,
		SnapshotLSN: make(map[string]uint64),
		Checksums:   make(map[string]string),
	}

	// Track the minimum snapshot LSN across all managers so we know which WAL
	// segments are needed for a complete recovery.
	minSnapshotLSN := uint64(0)

	// 1. Snapshot each registered manager.
	for _, s := range m.snapshotters {
		snapshotID := fmt.Sprintf("%s-%s", backupID, s.Name())
		snapPath := filepath.Join(backupID, "snapshots", s.Name()+".snap")

		pr, pw := io.Pipe()
		hw := &hashingWriter{w: pw, h: sha256.New()}

		var (
			snapLSN uint64
			snapErr error
		)
		go func(snap Snapshotter) {
			snapLSN, snapErr = snap.CreateSnapshot(ctx, snapshotID, hw)
			pw.CloseWithError(snapErr)
		}(s)

		if err := m.sink.Write(ctx, snapPath, pr); err != nil {
			return "", fmt.Errorf("backup: write snapshot for %s: %w", s.Name(), err)
		}
		if snapErr != nil {
			return "", fmt.Errorf("backup: create snapshot for %s: %w", s.Name(), snapErr)
		}

		manifest.SnapshotLSN[s.Name()] = snapLSN
		manifest.Files = append(manifest.Files, snapPath)
		manifest.Checksums[snapPath] = hex.EncodeToString(hw.h.Sum(nil))
		m.logger.Info("snapshot complete",
			zap.String("manager", s.Name()),
			zap.Uint64("lsn", snapLSN),
			zap.String("checksum", manifest.Checksums[snapPath]))

		if minSnapshotLSN == 0 || snapLSN < minSnapshotLSN {
			minSnapshotLSN = snapLSN
		}
	}

	// 2. Copy WAL segments starting from minSnapshotLSN.
	if m.walArchiver != nil {
		walFiles, err := m.walArchiver.GetSegmentFilesForBackup(minSnapshotLSN)
		if err != nil {
			return "", fmt.Errorf("backup: list WAL segments: %w", err)
		}
		for _, walFile := range walFiles {
			destPath := filepath.Join(backupID, "wal", filepath.Base(walFile))
			f, err := os.Open(walFile) // #nosec G304 — path comes from LogManager
			if err != nil {
				return "", fmt.Errorf("backup: open WAL segment %s: %w", walFile, err)
			}
			hr := &hashingReader{r: f, h: sha256.New()}
			writeErr := m.sink.Write(ctx, destPath, hr)
			f.Close()
			if writeErr != nil {
				return "", fmt.Errorf("backup: write WAL segment %s: %w", walFile, writeErr)
			}
			manifest.Files = append(manifest.Files, destPath)
			manifest.Checksums[destPath] = hex.EncodeToString(hr.h.Sum(nil))
		}
		m.logger.Info("WAL segments archived", zap.Int("count", len(walFiles)))
	}

	// 3. Write manifest.
	manifestJSON, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", fmt.Errorf("backup: marshal manifest: %w", err)
	}
	manifestPath := filepath.Join(backupID, "manifest.json")
	if err = m.sink.Write(ctx, manifestPath, bytes.NewReader(manifestJSON)); err != nil {
		return "", fmt.Errorf("backup: write manifest: %w", err)
	}

	m.logger.Info("backup complete",
		zap.String("backupID", backupID),
		zap.Int("files", len(manifest.Files)))
	return backupID, nil
}

// VerifyBackup reads a backup manifest and re-checksums every file to ensure
// the backup is intact.  It returns the manifest on success.
func VerifyBackup(ctx context.Context, source Source, backupID string) (*Manifest, error) {
	manifestPath := filepath.Join(backupID, "manifest.json")
	rc, err := source.Read(ctx, manifestPath)
	if err != nil {
		return nil, fmt.Errorf("verify: open manifest: %w", err)
	}
	defer rc.Close()

	var manifest Manifest
	if err = json.NewDecoder(rc).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("verify: decode manifest: %w", err)
	}

	for _, filePath := range manifest.Files {
		expected, ok := manifest.Checksums[filePath]
		if !ok {
			continue // no checksum recorded — skip
		}
		frc, err := source.Read(ctx, filePath)
		if err != nil {
			return nil, fmt.Errorf("verify: open %s: %w", filePath, err)
		}
		h := sha256.New()
		if _, err = io.Copy(h, frc); err != nil {
			frc.Close()
			return nil, fmt.Errorf("verify: hash %s: %w", filePath, err)
		}
		frc.Close()
		actual := hex.EncodeToString(h.Sum(nil))
		if actual != expected {
			return nil, fmt.Errorf("verify: checksum mismatch for %s: want %s got %s",
				filePath, expected, actual)
		}
	}
	return &manifest, nil
}

// hashingWriter wraps an io.Writer and computes a running SHA-256 hash.
type hashingWriter struct {
	w io.Writer
	h interface {
		Write([]byte) (int, error)
		Sum([]byte) []byte
	}
}

func (hw *hashingWriter) Write(p []byte) (int, error) {
	n, err := hw.w.Write(p)
	if n > 0 {
		_, _ = hw.h.Write(p[:n])
	}
	return n, err
}

// hashingReader wraps an io.Reader and computes a running SHA-256 hash.
type hashingReader struct {
	r io.Reader
	h interface {
		Write([]byte) (int, error)
		Sum([]byte) []byte
	}
}

func (hr *hashingReader) Read(p []byte) (int, error) {
	n, err := hr.r.Read(p)
	if n > 0 {
		_, _ = hr.h.Write(p[:n])
	}
	return n, err
}

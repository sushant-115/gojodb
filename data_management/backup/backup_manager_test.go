package backup

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// --- Test helpers ---

type mockSnapshotter struct {
	name    string
	lsn     uint64
	payload []byte
	err     error
}

func (m *mockSnapshotter) Name() string { return m.name }
func (m *mockSnapshotter) GetLatestLSN() uint64 { return m.lsn }
func (m *mockSnapshotter) CreateSnapshot(_ context.Context, _ string, w io.Writer) (uint64, error) {
	if m.err != nil {
		return 0, m.err
	}
	_, err := w.Write(m.payload)
	return m.lsn, err
}

type mockWALArchiver struct {
	mu    sync.Mutex
	files []string
}

func (m *mockWALArchiver) GetSegmentFilesForBackup(_ uint64) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.files...), nil
}

func newLogger() *zap.Logger {
	l, _ := zap.NewDevelopment()
	return l
}

func newMgr(t *testing.T, snapshotters []Snapshotter, archiver WALArchiver, sink Sink) *Manager {
	t.Helper()
	return NewManager(newLogger(), "test-node", snapshotters, archiver, sink)
}

// --- Tests ---

// TestBackup_EmptySnapshotters verifies that Backup succeeds with no snapshotters and
// writes a valid manifest with an empty files list.
func TestBackup_EmptySnapshotters(t *testing.T) {
	sink := &LocalSink{BaseDir: t.TempDir()}
	mgr := newMgr(t, nil, nil, sink)

	backupID, err := mgr.Backup(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, backupID)

	manifest, err := VerifyBackup(context.Background(), sink, backupID)
	require.NoError(t, err)
	require.Equal(t, "test-node", manifest.NodeID)
	require.Empty(t, manifest.Files)
}

// TestBackup_SingleSnapshotter verifies that a single snapshotter's data is
// written to the expected path and its checksum is recorded in the manifest.
func TestBackup_SingleSnapshotter(t *testing.T) {
	snap := &mockSnapshotter{name: "btree", lsn: 42, payload: []byte("snapshot_data")}
	sink := &LocalSink{BaseDir: t.TempDir()}
	mgr := newMgr(t, []Snapshotter{snap}, nil, sink)

	backupID, err := mgr.Backup(context.Background())
	require.NoError(t, err)

	// Snapshot file should exist.
	snapPath := filepath.Join(sink.BaseDir, backupID, "snapshots", "btree.snap")
	data, err := os.ReadFile(snapPath)
	require.NoError(t, err)
	require.Equal(t, "snapshot_data", string(data))

	// Manifest should record LSN and checksum.
	manifest, err := VerifyBackup(context.Background(), sink, backupID)
	require.NoError(t, err)
	require.Equal(t, uint64(42), manifest.SnapshotLSN["btree"])
	require.NotEmpty(t, manifest.Checksums)
}

// TestBackup_MultipleSnapshotters verifies that all snapshotters produce separate
// snapshot files and all are listed in the manifest.
func TestBackup_MultipleSnapshotters(t *testing.T) {
	snaps := []Snapshotter{
		&mockSnapshotter{name: "btree", lsn: 10, payload: []byte("btree_snap")},
		&mockSnapshotter{name: "inverted", lsn: 20, payload: []byte("inverted_snap")},
		&mockSnapshotter{name: "spatial", lsn: 5, payload: []byte("spatial_snap")},
	}
	sink := &LocalSink{BaseDir: t.TempDir()}
	mgr := newMgr(t, snaps, nil, sink)

	backupID, err := mgr.Backup(context.Background())
	require.NoError(t, err)

	manifest, err := VerifyBackup(context.Background(), sink, backupID)
	require.NoError(t, err)
	require.Len(t, manifest.SnapshotLSN, 3)
	require.Equal(t, uint64(10), manifest.SnapshotLSN["btree"])
	require.Equal(t, uint64(20), manifest.SnapshotLSN["inverted"])
	require.Equal(t, uint64(5), manifest.SnapshotLSN["spatial"])

	// 3 snapshot files + 1 manifest = 4 entries in manifest.Files (no WAL).
	require.Len(t, manifest.Files, 3)
}

// TestBackup_WALSegmentsIncluded verifies that WAL segment files provided by the
// WALArchiver are copied into the backup and their checksums recorded.
func TestBackup_WALSegmentsIncluded(t *testing.T) {
	// Create a temporary WAL segment file.
	tmpDir := t.TempDir()
	walFile := filepath.Join(tmpDir, "wal-00000000000000000001.log")
	require.NoError(t, os.WriteFile(walFile, []byte("wal_contents"), 0600))

	archiver := &mockWALArchiver{files: []string{walFile}}
	snap := &mockSnapshotter{name: "btree", lsn: 1, payload: []byte("snap")}
	sink := &LocalSink{BaseDir: t.TempDir()}
	mgr := newMgr(t, []Snapshotter{snap}, archiver, sink)

	backupID, err := mgr.Backup(context.Background())
	require.NoError(t, err)

	// WAL file should be present.
	walDest := filepath.Join(sink.BaseDir, backupID, "wal", "wal-00000000000000000001.log")
	data, err := os.ReadFile(walDest)
	require.NoError(t, err)
	require.Equal(t, "wal_contents", string(data))

	manifest, err := VerifyBackup(context.Background(), sink, backupID)
	require.NoError(t, err)
	require.Len(t, manifest.Files, 2) // 1 snapshot + 1 WAL
}

// TestBackup_SnapshotterError verifies that a Backup call fails and returns an
// error when one of the snapshotters returns an error.
func TestBackup_SnapshotterError(t *testing.T) {
	snap := &mockSnapshotter{name: "btree", err: io.ErrUnexpectedEOF}
	sink := &LocalSink{BaseDir: t.TempDir()}
	mgr := newMgr(t, []Snapshotter{snap}, nil, sink)

	_, err := mgr.Backup(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "btree")
}

// TestBackup_ManifestContents verifies that the manifest.json written to the sink
// is valid JSON and contains the expected fields.
func TestBackup_ManifestContents(t *testing.T) {
	snap := &mockSnapshotter{name: "idx", lsn: 99, payload: []byte("data")}
	sink := &LocalSink{BaseDir: t.TempDir()}
	mgr := newMgr(t, []Snapshotter{snap}, nil, sink)

	backupID, err := mgr.Backup(context.Background())
	require.NoError(t, err)

	raw, err := os.ReadFile(filepath.Join(sink.BaseDir, backupID, "manifest.json"))
	require.NoError(t, err)

	var m Manifest
	require.NoError(t, json.Unmarshal(raw, &m))
	require.Equal(t, backupID, m.BackupID)
	require.Equal(t, "test-node", m.NodeID)
	require.False(t, m.Timestamp.IsZero())
}

// TestVerifyBackup_ChecksumMismatch verifies that VerifyBackup returns an error when
// a file has been tampered with (checksum mismatch).
func TestVerifyBackup_ChecksumMismatch(t *testing.T) {
	snap := &mockSnapshotter{name: "btree", lsn: 1, payload: []byte("original")}
	sink := &LocalSink{BaseDir: t.TempDir()}
	mgr := newMgr(t, []Snapshotter{snap}, nil, sink)

	backupID, err := mgr.Backup(context.Background())
	require.NoError(t, err)

	// Corrupt the snapshot file.
	snapPath := filepath.Join(sink.BaseDir, backupID, "snapshots", "btree.snap")
	require.NoError(t, os.WriteFile(snapPath, []byte("tampered"), 0600))

	_, err = VerifyBackup(context.Background(), sink, backupID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "checksum mismatch")
}

// TestVerifyBackup_MissingFile verifies that VerifyBackup returns an error when a
// file listed in the manifest is absent.
func TestVerifyBackup_MissingFile(t *testing.T) {
	snap := &mockSnapshotter{name: "btree", lsn: 1, payload: []byte("data")}
	sink := &LocalSink{BaseDir: t.TempDir()}
	mgr := newMgr(t, []Snapshotter{snap}, nil, sink)

	backupID, err := mgr.Backup(context.Background())
	require.NoError(t, err)

	// Remove the snapshot file.
	require.NoError(t, os.Remove(filepath.Join(sink.BaseDir, backupID, "snapshots", "btree.snap")))

	_, err = VerifyBackup(context.Background(), sink, backupID)
	require.Error(t, err)
}

// TestBackup_ContextCancellation verifies that Backup respects a cancelled context.
func TestBackup_ContextCancellation(t *testing.T) {
	// A snapshotter that blocks until it sees the context cancelled.
	snap := &blockingSnapshotter{}
	sink := &LocalSink{BaseDir: t.TempDir()}
	mgr := newMgr(t, []Snapshotter{snap}, nil, sink)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := mgr.Backup(ctx)
	require.Error(t, err)
}

// blockingSnapshotter returns a context error to simulate cancellation.
type blockingSnapshotter struct{}

func (b *blockingSnapshotter) Name() string { return "blocker" }
func (b *blockingSnapshotter) GetLatestLSN() uint64 { return 0 }
func (b *blockingSnapshotter) CreateSnapshot(ctx context.Context, _ string, _ io.Writer) (uint64, error) {
	return 0, ctx.Err()
}

// TestBackup_BackupIDUnique verifies that successive Backup calls produce distinct IDs.
func TestBackup_BackupIDUnique(t *testing.T) {
	sink := &LocalSink{BaseDir: t.TempDir()}
	mgr := newMgr(t, nil, nil, sink)

	id1, err := mgr.Backup(context.Background())
	require.NoError(t, err)
	id2, err := mgr.Backup(context.Background())
	require.NoError(t, err)
	require.NotEqual(t, id1, id2)
}

// TestBackup_NodeIDEmbedded verifies that the node ID appears in the backup ID and manifest.
func TestBackup_NodeIDEmbedded(t *testing.T) {
	sink := &LocalSink{BaseDir: t.TempDir()}
	mgr := NewManager(newLogger(), "node-xyz", nil, nil, sink)

	backupID, err := mgr.Backup(context.Background())
	require.NoError(t, err)
	require.True(t, strings.Contains(backupID, "node-xyz"), "backupID should contain nodeID")

	manifest, err := VerifyBackup(context.Background(), sink, backupID)
	require.NoError(t, err)
	require.Equal(t, "node-xyz", manifest.NodeID)
}

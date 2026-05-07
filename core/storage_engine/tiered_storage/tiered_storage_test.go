package tiered_storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	coldstorage "github.com/sushant-115/gojodb/core/storage_engine/cold_storage"
	hotstorage "github.com/sushant-115/gojodb/core/storage_engine/hot_storage"
	"go.uber.org/zap"
)

// --- Helpers ---

// inMemoryColdBackend is a simple in-memory CloudObjectStore for testing.
type inMemoryColdBackend struct {
	data map[string][]byte
}

func newInMemoryColdBackend() *inMemoryColdBackend {
	return &inMemoryColdBackend{data: make(map[string][]byte)}
}

func (b *inMemoryColdBackend) Write(_ context.Context, key string, data []byte) (string, error) {
	cp := make([]byte, len(data))
	copy(cp, data)
	b.data[key] = cp
	return "mem://" + key, nil
}

func (b *inMemoryColdBackend) Read(_ context.Context, key string) ([]byte, error) {
	d, ok := b.data[key]
	if !ok {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	cp := make([]byte, len(d))
	copy(cp, d)
	return cp, nil
}

func (b *inMemoryColdBackend) Delete(_ context.Context, key string) error {
	delete(b.data, key)
	return nil
}

func (b *inMemoryColdBackend) AdapterType() string { return "mem" }

func setupTSM(t *testing.T, cfg Config) (*TieredStorageManager, *inMemoryColdBackend) {
	t.Helper()
	logger, _ := zap.NewDevelopment()

	hot, err := hotstorage.NewHotStorageAdapter(t.TempDir())
	require.NoError(t, err)

	backend := newInMemoryColdBackend()
	cold := coldstorage.NewColdStorageAdapter(backend)

	tsm, err := NewTieredStorageManager(hot, cold, cfg, logger)
	require.NoError(t, err)
	return tsm, backend
}

func registerUnit(tsm *TieredStorageManager, unitID string, content []byte) TieredDataMetadata {
	_, _ = tsm.hotAdapter.Write(unitID, content)
	meta := TieredDataMetadata{
		LogicalUnitID:    unitID,
		CurrentTier:      HotTier,
		Size:             int64(len(content)),
		CreationTime:     time.Now().Add(-24 * time.Hour),
		LastModifiedTime: time.Now().Add(-24 * time.Hour),
	}
	tsm.RegisterUnit(meta)
	return meta
}

// --- Tests ---

// TestTSM_MigrateUnit_HotToCold verifies that a unit in hot storage is moved to
// cold storage and that the metadata cache reflects the new tier.
func TestTSM_MigrateUnit_HotToCold(t *testing.T) {
	tsm, backend := setupTSM(t, Config{})

	registerUnit(tsm, "unit-001", []byte("hello tiering"))

	err := tsm.MigrateUnit(context.Background(), "unit-001", ColdTierS3)
	require.NoError(t, err)

	meta, ok := tsm.GetMetadata("unit-001")
	require.True(t, ok)
	require.Equal(t, ColdTierS3, meta.CurrentTier)
	require.NotEmpty(t, meta.Checksum)

	// Data should be in cold backend.
	_, ok = backend.data["unit-001"]
	require.True(t, ok, "data should exist in cold backend")

	// Data should no longer be in hot storage.
	require.False(t, tsm.hotAdapter.Exists("unit-001"), "data should be removed from hot storage")
}

// TestTSM_MigrateUnit_WithEncryption verifies that data migrated to cold storage is
// encrypted and that the raw cold storage bytes differ from the plaintext.
func TestTSM_MigrateUnit_WithEncryption(t *testing.T) {
	key := make([]byte, 32) // AES-256 zero key (acceptable for tests)
	tsm, backend := setupTSM(t, Config{EncryptionKey: key})

	payload := []byte("sensitive data for cold storage")
	registerUnit(tsm, "enc-unit-001", payload)

	require.NoError(t, tsm.MigrateUnit(context.Background(), "enc-unit-001", ColdTierS3))

	// Raw bytes in cold backend should NOT equal the plaintext.
	rawCold := backend.data["enc-unit-001"]
	require.NotEqual(t, payload, rawCold, "cold data should be encrypted")

	// Metadata should record the encryption key ID.
	meta, _ := tsm.GetMetadata("enc-unit-001")
	require.Equal(t, "aes-gcm-v1", meta.EncryptionKeyID)
}

// TestTSM_RecallDataUnit verifies that a previously tiered-out unit can be recalled
// back into hot storage with the correct data.
func TestTSM_RecallDataUnit(t *testing.T) {
	tsm, _ := setupTSM(t, Config{})

	payload := []byte("data to be recalled")
	registerUnit(tsm, "recall-001", payload)

	require.NoError(t, tsm.MigrateUnit(context.Background(), "recall-001", ColdTierS3))

	recalled, err := tsm.RecallDataUnit(context.Background(), "recall-001")
	require.NoError(t, err)
	require.Equal(t, payload, recalled)

	meta, _ := tsm.GetMetadata("recall-001")
	require.Equal(t, HotTier, meta.CurrentTier)
	require.True(t, tsm.hotAdapter.Exists("recall-001"), "data should be back in hot storage")
}

// TestTSM_RecallWithEncryption verifies that encrypted cold data is correctly
// decrypted when recalled to hot storage.
func TestTSM_RecallWithEncryption(t *testing.T) {
	key := make([]byte, 32)
	tsm, _ := setupTSM(t, Config{EncryptionKey: key})

	payload := []byte("encrypted payload round-trip")
	registerUnit(tsm, "enc-recall-001", payload)

	require.NoError(t, tsm.MigrateUnit(context.Background(), "enc-recall-001", ColdTierS3))

	recalled, err := tsm.RecallDataUnit(context.Background(), "enc-recall-001")
	require.NoError(t, err)
	require.Equal(t, payload, recalled, "recalled data should match original")
}

// TestTSM_ChecksumVerification verifies that if cold storage returns corrupted data,
// RecallDataUnit returns a checksum-mismatch error.
func TestTSM_ChecksumVerification(t *testing.T) {
	tsm, backend := setupTSM(t, Config{})

	registerUnit(tsm, "corrupt-001", []byte("original content"))
	require.NoError(t, tsm.MigrateUnit(context.Background(), "corrupt-001", ColdTierS3))

	// Corrupt the cold data.
	backend.data["corrupt-001"] = []byte("corrupted!!!")

	_, err := tsm.RecallDataUnit(context.Background(), "corrupt-001")
	require.Error(t, err)
	require.Contains(t, err.Error(), "checksum mismatch")
}

// TestTSM_ShouldMigrate_Age verifies that shouldMigrate returns true when the
// unit is older than the policy's age threshold.
func TestTSM_ShouldMigrate_Age(t *testing.T) {
	tsm, _ := setupTSM(t, Config{})

	policy := DLMPolicy{
		PolicyID:    "p1",
		TriggerType: Age,
		TriggerValue: "1h",
		SourceTier:  HotTier,
		TargetTier:  ColdTierS3,
		IsEnabled:   true,
	}

	oldUnit := TieredDataMetadata{
		LastModifiedTime: time.Now().Add(-2 * time.Hour),
	}
	require.True(t, tsm.shouldMigrate(oldUnit, policy), "unit older than threshold should migrate")

	newUnit := TieredDataMetadata{
		LastModifiedTime: time.Now().Add(-30 * time.Minute),
	}
	require.False(t, tsm.shouldMigrate(newUnit, policy), "unit younger than threshold should not migrate")
}

// TestTSM_ShouldMigrate_AccessFrequency verifies that rarely accessed units
// (low score) are selected for migration.
func TestTSM_ShouldMigrate_AccessFrequency(t *testing.T) {
	tsm, _ := setupTSM(t, Config{})

	policy := DLMPolicy{
		TriggerType:  AccessFrequency,
		TriggerValue: "0.2",
	}

	rareUnit := TieredDataMetadata{AccessFrequencyScore: 0.1}
	require.True(t, tsm.shouldMigrate(rareUnit, policy))

	frequentUnit := TieredDataMetadata{AccessFrequencyScore: 0.5}
	require.False(t, tsm.shouldMigrate(frequentUnit, policy))
}

// TestTSM_UpdateAccessStats_EWMA verifies that repeated accesses increase the
// EWMA score and that the score stays bounded between 0 and 1.
func TestTSM_UpdateAccessStats_EWMA(t *testing.T) {
	tsm, _ := setupTSM(t, Config{})
	registerUnit(tsm, "access-unit", []byte("data"))

	meta0, _ := tsm.GetMetadata("access-unit")
	require.Equal(t, 0.0, meta0.AccessFrequencyScore)

	now := time.Now()
	tsm.UpdateAccessStats("access-unit", now)

	meta1, _ := tsm.GetMetadata("access-unit")
	require.Greater(t, meta1.AccessFrequencyScore, 0.0)
	require.LessOrEqual(t, meta1.AccessFrequencyScore, 1.0)

	// More accesses should increase the score further.
	for i := 0; i < 10; i++ {
		tsm.UpdateAccessStats("access-unit", now)
	}
	meta2, _ := tsm.GetMetadata("access-unit")
	require.Greater(t, meta2.AccessFrequencyScore, meta1.AccessFrequencyScore)
}

// TestTSM_MigrateUnit_NotRegistered verifies that migrating an unknown unit
// returns an error.
func TestTSM_MigrateUnit_NotRegistered(t *testing.T) {
	tsm, _ := setupTSM(t, Config{})
	err := tsm.MigrateUnit(context.Background(), "ghost-unit", ColdTierS3)
	require.Error(t, err)
}

// TestTSM_RecallAlreadyHot verifies that recalling a unit that is already in
// the hot tier reads it directly without hitting cold storage.
func TestTSM_RecallAlreadyHot(t *testing.T) {
	tsm, _ := setupTSM(t, Config{})
	payload := []byte("still hot")
	registerUnit(tsm, "hot-unit", payload)

	recalled, err := tsm.RecallDataUnit(context.Background(), "hot-unit")
	require.NoError(t, err)
	require.Equal(t, payload, recalled)
}

// TestTSM_StartStop verifies that Start and Stop do not race or deadlock.
func TestTSM_StartStop(t *testing.T) {
	tsm, _ := setupTSM(t, Config{EvalInterval: 50 * time.Millisecond})
	require.NoError(t, tsm.Start())
	time.Sleep(60 * time.Millisecond) // let the loop tick once
	require.NoError(t, tsm.Stop())
}

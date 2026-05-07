package tiered_storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/sushant-115/gojodb/core/security/encryption"
	coldstorage "github.com/sushant-115/gojodb/core/storage_engine/cold_storage"
	hotstorage "github.com/sushant-115/gojodb/core/storage_engine/hot_storage"
)

const (
	// ewmaAlpha is the smoothing factor for the Exponentially Weighted Moving
	// Average used to track access frequency.  Higher → faster reaction to recent
	// activity.
	ewmaAlpha = 0.3

	defaultEvalInterval = 1 * time.Hour
	defaultMaxRetries   = 3
	defaultRetryBase    = 500 * time.Millisecond
)

// Config holds optional parameters for the TieredStorageManager.
type Config struct {
	// EncryptionKey is a 16-, 24-, or 32-byte AES key.  When non-nil, all data
	// written to cold storage is encrypted with AES-256-GCM.
	EncryptionKey []byte
	// EvalInterval controls how often the DLM policy loop runs.
	EvalInterval time.Duration
	// MaxRetries is the number of times a failed write to cold storage is
	// retried before the error is propagated.
	MaxRetries int
}

func (c *Config) evalInterval() time.Duration {
	if c == nil || c.EvalInterval == 0 {
		return defaultEvalInterval
	}
	return c.EvalInterval
}

func (c *Config) maxRetries() int {
	if c == nil || c.MaxRetries == 0 {
		return defaultMaxRetries
	}
	return c.MaxRetries
}

// TieredStorageManager manages data movement between hot and cold storage tiers.
type TieredStorageManager struct {
	hotAdapter  *hotstorage.HotStorageAdapter
	coldAdapter *coldstorage.ColdStorageAdapter
	crypto      *encryption.CryptoUtils // nil when encryption is disabled

	logger *zap.Logger
	mu     sync.RWMutex
	cfg    Config

	dlmPolicies               []DLMPolicy
	localTieringMetadataCache map[string]TieredDataMetadata // key: LogicalUnitID

	stopChan chan struct{}
	wg       sync.WaitGroup
}

// NewTieredStorageManager creates a new TieredStorageManager.
func NewTieredStorageManager(
	hotAdapter *hotstorage.HotStorageAdapter,
	coldAdapter *coldstorage.ColdStorageAdapter,
	cfg Config,
	logger *zap.Logger,
) (*TieredStorageManager, error) {
	tsm := &TieredStorageManager{
		hotAdapter:                hotAdapter,
		coldAdapter:               coldAdapter,
		logger:                    logger.Named("tiered_storage"),
		cfg:                       cfg,
		dlmPolicies:               make([]DLMPolicy, 0),
		localTieringMetadataCache: make(map[string]TieredDataMetadata),
		stopChan:                  make(chan struct{}),
	}

	if len(cfg.EncryptionKey) > 0 {
		cu, err := encryption.NewCryptoUtils(cfg.EncryptionKey)
		if err != nil {
			return nil, fmt.Errorf("tiered_storage: init encryption: %w", err)
		}
		tsm.crypto = cu
		logger.Info("encryption enabled for cold storage tier")
	}

	return tsm, nil
}

// Start initiates background DLM policy evaluation.
func (tsm *TieredStorageManager) Start() error {
	tsm.logger.Info("starting TieredStorageManager")
	tsm.wg.Add(1)
	go tsm.dlmPolicyEvaluationLoop()
	return nil
}

// Stop gracefully shuts down background goroutines.
func (tsm *TieredStorageManager) Stop() error {
	tsm.logger.Info("stopping TieredStorageManager")
	close(tsm.stopChan)
	tsm.wg.Wait()
	tsm.logger.Info("TieredStorageManager stopped")
	return nil
}

// Close releases any resources held by the manager.
func (tsm *TieredStorageManager) Close() error { return nil }

// LoadDLMPolicies replaces the current policy set.
func (tsm *TieredStorageManager) LoadDLMPolicies(policies []DLMPolicy) {
	tsm.mu.Lock()
	defer tsm.mu.Unlock()
	tsm.dlmPolicies = policies
	tsm.logger.Info("DLM policies loaded", zap.Int("count", len(policies)))
}

// RegisterUnit registers a hot-tier data unit for tiering management.
// Call this whenever new data is written to hot storage.
func (tsm *TieredStorageManager) RegisterUnit(meta TieredDataMetadata) {
	tsm.mu.Lock()
	defer tsm.mu.Unlock()
	if meta.CreationTime.IsZero() {
		meta.CreationTime = time.Now()
	}
	if meta.LastModifiedTime.IsZero() {
		meta.LastModifiedTime = meta.CreationTime
	}
	meta.CurrentTier = HotTier
	tsm.localTieringMetadataCache[meta.LogicalUnitID] = meta
}

// UpdateAccessStats records an access event for the given unit.  The access
// frequency score is updated using an EWMA so recent activity has more weight.
func (tsm *TieredStorageManager) UpdateAccessStats(logicalUnitID string, accessTime time.Time) {
	tsm.mu.Lock()
	defer tsm.mu.Unlock()

	meta, exists := tsm.localTieringMetadataCache[logicalUnitID]
	if !exists {
		return
	}
	// EWMA: newScore = alpha + (1-alpha)*oldScore
	// A single access contributes 1.0; frequency scores decay toward 0 over time.
	meta.AccessFrequencyScore = ewmaAlpha + (1-ewmaAlpha)*meta.AccessFrequencyScore
	meta.LastAccessTime = accessTime
	tsm.localTieringMetadataCache[logicalUnitID] = meta
}

// MigrateUnit synchronously migrates a single unit to targetTier.
// Exposed for on-demand tiering (e.g. from an admin API).
func (tsm *TieredStorageManager) MigrateUnit(ctx context.Context, unitID string, targetTier StorageTierType) error {
	tsm.mu.RLock()
	meta, ok := tsm.localTieringMetadataCache[unitID]
	tsm.mu.RUnlock()
	if !ok {
		return fmt.Errorf("tiered_storage: unit %q not found in metadata cache", unitID)
	}
	return tsm.migrateDataUnit(ctx, meta, targetTier)
}

// RecallDataUnit reads a tiered-out data unit from cold storage and writes it
// back to hot storage.  Returns the raw (decrypted) data.
func (tsm *TieredStorageManager) RecallDataUnit(ctx context.Context, logicalUnitID string) ([]byte, error) {
	tsm.mu.RLock()
	meta, ok := tsm.localTieringMetadataCache[logicalUnitID]
	tsm.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("tiered_storage: unit %q not found in metadata cache", logicalUnitID)
	}
	if meta.CurrentTier == HotTier {
		return tsm.hotAdapter.Read(logicalUnitID)
	}

	// 1. Read from cold storage.
	rawData, err := tsm.coldAdapter.Read(ctx, meta.LocationPointer)
	if err != nil {
		return nil, fmt.Errorf("tiered_storage: recall read cold for %s: %w", logicalUnitID, err)
	}

	// 2. Decrypt if the unit was encrypted.
	plaintext := rawData
	if meta.EncryptionKeyID != "" && tsm.crypto != nil {
		plaintext, err = tsm.crypto.Decrypt(rawData)
		if err != nil {
			return nil, fmt.Errorf("tiered_storage: recall decrypt %s: %w", logicalUnitID, err)
		}
	}

	// 3. Verify checksum.
	if meta.Checksum != "" {
		actual := checksum(plaintext)
		if actual != meta.Checksum {
			return nil, fmt.Errorf("tiered_storage: recall checksum mismatch for %s: want %s got %s",
				logicalUnitID, meta.Checksum, actual)
		}
	}

	// 4. Write back to hot storage.
	if _, err = tsm.hotAdapter.Write(logicalUnitID, plaintext); err != nil {
		return nil, fmt.Errorf("tiered_storage: recall write hot for %s: %w", logicalUnitID, err)
	}

	// 5. Update metadata.
	tsm.mu.Lock()
	meta.CurrentTier = HotTier
	meta.LocationPointer = logicalUnitID
	meta.LastAccessTime = time.Now()
	tsm.localTieringMetadataCache[logicalUnitID] = meta
	tsm.mu.Unlock()

	tsm.logger.Info("unit recalled to hot tier",
		zap.String("unitID", logicalUnitID),
		zap.Int("bytes", len(plaintext)))
	return plaintext, nil
}

// GetMetadata returns the tiering metadata for a unit (for inspection / testing).
func (tsm *TieredStorageManager) GetMetadata(unitID string) (TieredDataMetadata, bool) {
	tsm.mu.RLock()
	defer tsm.mu.RUnlock()
	m, ok := tsm.localTieringMetadataCache[unitID]
	return m, ok
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

func (tsm *TieredStorageManager) dlmPolicyEvaluationLoop() {
	defer tsm.wg.Done()
	ticker := time.NewTicker(tsm.cfg.evalInterval())
	defer ticker.Stop()

	for {
		select {
		case <-tsm.stopChan:
			tsm.logger.Info("DLM evaluation loop stopping")
			return
		case <-ticker.C:
			tsm.logger.Info("evaluating DLM policies")
			tsm.evaluateAndExecutePolicies()
		}
	}
}

func (tsm *TieredStorageManager) evaluateAndExecutePolicies() {
	tsm.mu.RLock()
	policies := make([]DLMPolicy, len(tsm.dlmPolicies))
	copy(policies, tsm.dlmPolicies)
	tsm.mu.RUnlock()

	// Lower priority number = higher priority; evaluate in ascending order.
	sort.Slice(policies, func(i, j int) bool {
		return policies[i].Priority < policies[j].Priority
	})

	for _, policy := range policies {
		if !policy.IsEnabled {
			continue
		}
		candidates, err := tsm.findCandidateDataUnits(policy)
		if err != nil {
			tsm.logger.Error("findCandidateDataUnits failed",
				zap.String("policy", policy.PolicyID), zap.Error(err))
			continue
		}
		for _, unit := range candidates {
			if unit.IsPinned {
				continue
			}
			if !tsm.shouldMigrate(unit, policy) {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			if err := tsm.migrateDataUnit(ctx, unit, policy.TargetTier); err != nil {
				tsm.logger.Error("migration failed",
					zap.String("unitID", unit.LogicalUnitID),
					zap.String("policy", policy.PolicyID),
					zap.Error(err))
			} else {
				tsm.logger.Info("migration complete",
					zap.String("unitID", unit.LogicalUnitID),
					zap.String("targetTier", string(policy.TargetTier)))
			}
			cancel()
		}
	}
}

func (tsm *TieredStorageManager) findCandidateDataUnits(policy DLMPolicy) ([]TieredDataMetadata, error) {
	tsm.mu.RLock()
	defer tsm.mu.RUnlock()

	var candidates []TieredDataMetadata
	for _, meta := range tsm.localTieringMetadataCache {
		if meta.CurrentTier != policy.SourceTier {
			continue
		}
		if policy.TargetEntityType == WALSegments && meta.CustomTags["type"] != "wal" {
			continue
		}
		if policy.TargetEntityType == IndexData && meta.CustomTags["type"] != "index" {
			continue
		}
		candidates = append(candidates, meta)
	}
	return candidates, nil
}

func (tsm *TieredStorageManager) shouldMigrate(unit TieredDataMetadata, policy DLMPolicy) bool {
	switch policy.TriggerType {
	case Age:
		d, err := time.ParseDuration(policy.TriggerValue)
		if err != nil {
			tsm.logger.Error("invalid age trigger value",
				zap.String("policy", policy.PolicyID),
				zap.String("value", policy.TriggerValue))
			return false
		}
		ref := unit.LastModifiedTime
		if ref.IsZero() {
			ref = unit.CreationTime
		}
		return time.Since(ref) >= d

	case AccessFrequency:
		// TriggerValue is a float threshold; migrate if score is BELOW it
		// (rarely accessed data should move to cold).
		var threshold float64
		if _, err := fmt.Sscanf(policy.TriggerValue, "%f", &threshold); err != nil {
			return false
		}
		return unit.AccessFrequencyScore < threshold

	case Utilization:
		// Utilization-based policies are evaluated at the tier level.
		// Without a tier-level utilization API, we use unit size as a proxy:
		// if the unit is larger than TriggerValue bytes, consider it for archival.
		var minBytes int64
		if _, err := fmt.Sscanf(policy.TriggerValue, "%d", &minBytes); err != nil {
			return false
		}
		return unit.Size >= minBytes
	}
	return false
}

// migrateDataUnit moves a unit from its current tier to targetTier.
func (tsm *TieredStorageManager) migrateDataUnit(ctx context.Context, unit TieredDataMetadata, targetTier StorageTierType) error {
	tsm.logger.Info("migrating unit",
		zap.String("unitID", unit.LogicalUnitID),
		zap.String("from", string(unit.CurrentTier)),
		zap.String("to", string(targetTier)))

	// 1. Read from source tier.
	data, err := tsm.readFromTier(ctx, unit)
	if err != nil {
		return fmt.Errorf("tiered_storage: read source for %s: %w", unit.LogicalUnitID, err)
	}

	// 2. Compute plaintext checksum for later verification.
	plainChecksum := checksum(data)

	// 3. Optionally encrypt.
	toWrite := data
	encKeyID := ""
	if tsm.crypto != nil {
		toWrite, err = tsm.crypto.Encrypt(data)
		if err != nil {
			return fmt.Errorf("tiered_storage: encrypt %s: %w", unit.LogicalUnitID, err)
		}
		encKeyID = "aes-gcm-v1"
	}

	// 4. Write to target tier with retry.
	objectKey := unit.LogicalUnitID
	var uri string
	if err = tsm.writeWithRetry(ctx, objectKey, toWrite, &uri); err != nil {
		return fmt.Errorf("tiered_storage: write cold for %s: %w", unit.LogicalUnitID, err)
	}

	// 5. Verify: read back, decrypt, compare checksum.
	if err = tsm.verifyWrite(ctx, objectKey, plainChecksum); err != nil {
		// Best-effort cleanup of the orphaned object.
		_ = tsm.coldAdapter.Delete(ctx, objectKey)
		return fmt.Errorf("tiered_storage: verify write for %s: %w", unit.LogicalUnitID, err)
	}

	// 6. Update metadata.
	tsm.mu.Lock()
	updated := unit
	updated.CurrentTier = targetTier
	updated.LocationPointer = objectKey
	updated.Checksum = plainChecksum
	updated.EncryptionKeyID = encKeyID
	updated.LastModifiedTime = time.Now()
	tsm.localTieringMetadataCache[unit.LogicalUnitID] = updated
	tsm.mu.Unlock()

	// 7. Remove from hot tier if we just moved it out.
	if unit.CurrentTier == HotTier {
		if delErr := tsm.hotAdapter.Delete(unit.LogicalUnitID); delErr != nil {
			tsm.logger.Warn("failed to delete tiered-out unit from hot storage",
				zap.String("unitID", unit.LogicalUnitID), zap.Error(delErr))
		}
	}

	return nil
}

func (tsm *TieredStorageManager) readFromTier(ctx context.Context, unit TieredDataMetadata) ([]byte, error) {
	switch unit.CurrentTier {
	case HotTier:
		return tsm.hotAdapter.Read(unit.LogicalUnitID)
	default:
		return tsm.coldAdapter.Read(ctx, unit.LocationPointer)
	}
}

func (tsm *TieredStorageManager) writeWithRetry(ctx context.Context, objectKey string, data []byte, uriOut *string) error {
	maxRetries := tsm.cfg.maxRetries()
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(math.Pow(2, float64(attempt-1))) * defaultRetryBase
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		uri, err := tsm.coldAdapter.Write(ctx, objectKey, data)
		if err == nil {
			if uriOut != nil {
				*uriOut = uri
			}
			return nil
		}
		lastErr = err
		tsm.logger.Warn("cold storage write failed, retrying",
			zap.String("objectKey", objectKey),
			zap.Int("attempt", attempt+1),
			zap.Error(err))
	}
	return fmt.Errorf("all %d write attempts failed: %w", maxRetries, lastErr)
}

func (tsm *TieredStorageManager) verifyWrite(ctx context.Context, objectKey, expected string) error {
	raw, err := tsm.coldAdapter.Read(ctx, objectKey)
	if err != nil {
		return fmt.Errorf("verify read failed: %w", err)
	}

	// Decrypt before comparing if encryption is enabled.
	plaintext := raw
	if tsm.crypto != nil {
		plaintext, err = tsm.crypto.Decrypt(raw)
		if err != nil {
			return fmt.Errorf("verify decrypt failed: %w", err)
		}
	}

	actual := checksum(plaintext)
	if actual != expected {
		return fmt.Errorf("checksum mismatch: want %s got %s", expected, actual)
	}
	return nil
}

// checksum returns the hex-encoded SHA-256 digest of data.
func checksum(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

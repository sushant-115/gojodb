package tiered_storage

import (
	"fmt"
	"strings"
	"sync"
	"time"

	coldstorage "github.com/sushant-115/gojodb/core/storage_engine/cold_storage" // Assuming PageManager for hot tier interaction
	hotstorage "github.com/sushant-115/gojodb/core/storage_engine/hot_storage"
	pagemanager "github.com/sushant-115/gojodb/core/write_engine/page_manager"
	"github.com/sushant-115/gojodb/core/write_engine/wal" // For WAL segment tiering
	"go.uber.org/zap"
	// FSM client/interface will be needed to update metadata
	// "github.com/sushant-115/gojodb/core/replication/raft_consensus/fsm/client" or similar
)

// TieredStorageManager manages data movement between different storage tiers.
type TieredStorageManager struct {
	hotStorageAdapter  *hotstorage.HotStorageAdapter
	coldStorageAdapter *coldstorage.ColdStorageAdapter
	// warmStorageAdapter WarmStorageAdapter // To be defined and added if a warm tier is implemented

	// pageManager is used to interact with data in the hot tier (e.g., reading pages to tier out)
	pageManager *pagemanager.PageManager // This might need to be an interface

	// logManager is used for WAL segment tiering
	walLogManager *wal.LogManager // This is for identifying WALs to archive

	logger *zap.Logger
	mu     sync.RWMutex

	// dlmPolicies are fetched from the Controller/FSM or a config source.
	dlmPolicies []DLMPolicy

	// localTieringMetadataCache is a cache of TieredDataMetadata for frequently accessed items.
	// The authoritative source is the Raft FSM.
	localTieringMetadataCache map[string]TieredDataMetadata // Key: LogicalUnitID

	// fsmClient is an interface to interact with the Raft FSM for metadata updates.
	// fsmClient fsm.ClientAPI // Placeholder for FSM interaction

	stopChan chan struct{}
	wg       sync.WaitGroup
}

// NewTieredStorageManager creates a new TieredStorageManager.
// The pageManager and walLogManager are passed for interaction with hot data and WALs.
func NewTieredStorageManager(
	hotAdapter *hotstorage.HotStorageAdapter,
	coldAdapter *coldstorage.ColdStorageAdapter,
	pageMgr *pagemanager.PageManager, // Or an interface
	walMgr *wal.LogManager,
	logger *zap.Logger,
	// fsmClient fsm.ClientAPI, // Pass FSM client
) *TieredStorageManager {
	return &TieredStorageManager{
		hotStorageAdapter:         hotAdapter,
		coldStorageAdapter:        coldAdapter,
		pageManager:               pageMgr,
		walLogManager:             walMgr,
		logger:                    logger.Named("tiered_storage_manager"),
		dlmPolicies:               make([]DLMPolicy, 0),
		localTieringMetadataCache: make(map[string]TieredDataMetadata),
		// fsmClient:               fsmClient,
		stopChan: make(chan struct{}),
	}
}

// Start initiates background processes for DLM policy evaluation.
func (tsm *TieredStorageManager) Start() error {
	tsm.logger.Info("Starting TieredStorageManager background processes...")
	tsm.wg.Add(1)
	go tsm.dlmPolicyEvaluationLoop()

	// TODO: Start other background tasks like periodic cache refresh from FSM, etc.
	return nil
}

// Stop gracefully shuts down the TieredStorageManager.
func (tsm *TieredStorageManager) Stop() error {
	tsm.logger.Info("Stopping TieredStorageManager...")
	close(tsm.stopChan)
	tsm.wg.Wait()
	tsm.logger.Info("TieredStorageManager stopped.")
	return nil
}

// LoadDLMPolicies loads (or refreshes) DLM policies, e.g., from the controller/FSM.
func (tsm *TieredStorageManager) LoadDLMPolicies(policies []DLMPolicy) {
	tsm.mu.Lock()
	defer tsm.mu.Unlock()
	tsm.dlmPolicies = policies
	tsm.logger.Info("DLM policies loaded/refreshed", zap.Int("count", len(policies)))
}

func (tsm *TieredStorageManager) dlmPolicyEvaluationLoop() {
	defer tsm.wg.Done()
	// TODO: Make evaluation interval configurable
	ticker := time.NewTicker(1 * time.Hour) // Evaluate policies periodically
	defer ticker.Stop()

	for {
		select {
		case <-tsm.stopChan:
			tsm.logger.Info("DLM policy evaluation loop stopping.")
			return
		case <-ticker.C:
			tsm.logger.Info("Evaluating DLM policies...")
			tsm.evaluateAndExecutePolicies()
		}
	}
}

func (tsm *TieredStorageManager) evaluateAndExecutePolicies() {
	tsm.mu.RLock()
	policies := make([]DLMPolicy, len(tsm.dlmPolicies))
	copy(policies, tsm.dlmPolicies)
	tsm.mu.RUnlock()

	// Sort policies by priority if not already sorted
	// sort.SliceStable(policies, func(i, j int) bool {
	// 	return policies[i].Priority < policies[j].Priority
	// })

	for _, policy := range policies {
		if !policy.IsEnabled {
			continue
		}
		tsm.logger.Debug("Evaluating policy", zap.String("policyID", policy.PolicyID), zap.String("policyName", policy.PolicyName))

		// 1. Identify candidate data units based on TargetEntityType and TargetEntityMatcher.
		//    This requires querying metadata (from local cache or FSM).
		//    Example: For UserTableData, iterate over pages/segments in policy.SourceTier.
		//    Example: For WALSegments, ask tsm.walLogManager for archivable segments.

		candidates, err := tsm.findCandidateDataUnits(policy)
		if err != nil {
			tsm.logger.Error("Failed to find candidate data units for policy", zap.String("policyID", policy.PolicyID), zap.Error(err))
			continue
		}

		for _, unitMetadata := range candidates {
			if unitMetadata.IsPinned {
				tsm.logger.Debug("Skipping pinned data unit", zap.String("unitID", unitMetadata.LogicalUnitID))
				continue
			}
			if tsm.shouldMigrate(unitMetadata, policy) {
				tsm.logger.Info("Data unit meets migration criteria",
					zap.String("unitID", unitMetadata.LogicalUnitID),
					zap.String("policyID", policy.PolicyID),
					zap.String("sourceTier", string(unitMetadata.CurrentTier)),
					zap.String("targetTier", string(policy.TargetTier)))

				err := tsm.migrateDataUnit(unitMetadata, policy.TargetTier)
				if err != nil {
					tsm.logger.Error("Failed to migrate data unit",
						zap.String("unitID", unitMetadata.LogicalUnitID),
						zap.String("policyID", policy.PolicyID),
						zap.Error(err))
					// TODO: Implement retry logic or mark as "failed_migration" in metadata
				} else {
					tsm.logger.Info("Successfully migrated data unit",
						zap.String("unitID", unitMetadata.LogicalUnitID),
						zap.String("policyID", policy.PolicyID),
						zap.String("newTier", string(policy.TargetTier)))
				}
			}
		}
	}
}

// findCandidateDataUnits fetches metadata for units that *could* be migrated by this policy.
func (tsm *TieredStorageManager) findCandidateDataUnits(policy DLMPolicy) ([]TieredDataMetadata, error) {
	// This is a complex part. It needs to:
	// 1. Determine the scope (e.g., all pages of a certain table, all WALs).
	// 2. Query the FSM (or a local replica of relevant metadata) for units in policy.SourceTier matching policy.TargetEntityType.
	//    - For UserTableData, this might mean iterating through page metadata associated with specific shards/tables.
	//    - For WALSegments, it means querying tsm.walLogManager.GetArchivableSegments()
	//      (this method needs to be added to LogManager).
	// This is a placeholder, actual implementation needs FSM interaction.
	tsm.logger.Warn("findCandidateDataUnits is a placeholder and needs full FSM integration.", zap.Any("policy", policy))

	var candidates []TieredDataMetadata
	if policy.TargetEntityType == WALSegments && policy.SourceTier == HotTier {
		// Example for WAL segments:
		// archivableSegments, err := tsm.walLogManager.GetArchivableSegments(minAgeForArchival) // minAge from policy.TriggerValue
		// if err != nil { return nil, err }
		// for _, seg := range archivableSegments {
		//    candidates = append(candidates, TieredDataMetadata{
		//        LogicalUnitID: seg.Name, ShardID: "GLOBAL_WAL", /*or shard-specific if WALs are per-shard*/
		//        CurrentTier: HotTier, Size: seg.Size, CreationTime: seg.CreationTime,
		//    })
		// }
	} else if policy.TargetEntityType == UserTableData {
		// Example for user data (pages/segments):
		// Iterate through localTieringMetadataCache or query FSM
		// This needs to be efficient. FSM might need to support queries like:
		// "Get all units in shard X, table Y, currently in HotTier, older than Z days."
		tsm.mu.RLock()
		for _, meta := range tsm.localTieringMetadataCache {
			if meta.CurrentTier == policy.SourceTier { // Simplified check
				// Further matching based on policy.TargetEntityMatcher would go here
				candidates = append(candidates, meta)
			}
		}
		tsm.mu.RUnlock()
	}

	return candidates, nil
}

// shouldMigrate checks if a specific data unit meets the policy's trigger conditions.
func (tsm *TieredStorageManager) shouldMigrate(unit TieredDataMetadata, policy DLMPolicy) bool {
	switch policy.TriggerType {
	case Age:
		ageDuration, err := time.ParseDuration(policy.TriggerValue) // e.g., "90d" -> 90 * 24 * time.Hour
		if err != nil {
			tsm.logger.Error("Invalid age duration in policy", zap.String("policyID", policy.PolicyID), zap.String("value", policy.TriggerValue), zap.Error(err))
			return false
		}
		// Use LastModifiedTime or CreationTime depending on data type and policy intent
		referenceTime := unit.LastModifiedTime
		if referenceTime.IsZero() {
			referenceTime = unit.CreationTime
		}
		if time.Since(referenceTime) >= ageDuration {
			return true
		}
	case AccessFrequency:
		// Requires access frequency tracking. Parse policy.TriggerValue (e.g., "<5_reads_last_30d")
		// Compare against unit.AccessFrequencyScore or similar metric.
		tsm.logger.Warn("AccessFrequency trigger not yet fully implemented.", zap.String("unitID", unit.LogicalUnitID))
		return false // Placeholder
	case Utilization:
		// This trigger type usually applies to the source tier as a whole, not individual units.
		// The evaluation loop might check source tier utilization first before checking individual units.
		// Or, policy applies if source_tier_util > X AND unit_age > Y.
		tsm.logger.Warn("Utilization trigger not yet fully implemented.", zap.String("unitID", unit.LogicalUnitID))
		return false // Placeholder
	default:
		tsm.logger.Warn("Unknown trigger type in policy", zap.String("policyID", policy.PolicyID), zap.String("trigger", string(policy.TriggerType)))
		return false
	}
	return false
}

// migrateDataUnit performs the actual data movement and metadata update.
func (tsm *TieredStorageManager) migrateDataUnit(unit TieredDataMetadata, targetTier StorageTierType) error {
	tsm.logger.Info("Attempting to migrate data unit",
		zap.String("unitID", unit.LogicalUnitID),
		zap.String("fromTier", string(unit.CurrentTier)),
		zap.String("toTier", string(targetTier)))

	var data []byte
	// var err error

	// 1. Read data from the source tier
	if unit.CurrentTier == HotTier {
		if unit.LogicalUnitID[:4] == "page" { // Assuming LogicalUnitID for pages is "page-<PageID>"
			// pageID, _ := strconv.Atoi(unit.LogicalUnitID[5:])
			// pageData, err := tsm.pageManager.ReadPageForTiering(pagemanager.PageID(pageID)) // Needs method on PageManager
			// if err != nil { return fmt.Errorf("failed to read page %s from hot tier: %w", unit.LogicalUnitID, err) }
			// data = pageData.GetData()
			return fmt.Errorf("reading page data from hot tier not fully implemented for tiering")
		} else if unit.LogicalUnitID[:3] == "wal" { // Assuming "wal-<SegmentName>"
			// walSegmentName := unit.LogicalUnitID[4:]
			// data, err = tsm.walLogManager.ReadWALSegmentForArchival(walSegmentName) // Needs method on LogManager
			// if err != nil { return fmt.Errorf("failed to read WAL segment %s: %w", walSegmentName, err) }
			return fmt.Errorf("reading WAL segment data not fully implemented for tiering")
		} else {
			return fmt.Errorf("unknown data unit type in hot tier: %s", unit.LogicalUnitID)
		}
	} else {
		// Reading from another cold/warm tier (e.g., warm to cold)
		// This would involve using the appropriate adapter for unit.CurrentTier
		// data, err = tsm.getAdapterForTier(unit.CurrentTier).Read(unit.LocationPointer)
		// if err != nil { return fmt.Errorf("failed to read data unit %s from %s: %w", unit.LogicalUnitID, unit.CurrentTier, err) }
		return fmt.Errorf("reading from non-hot tier (%s) not implemented yet", unit.CurrentTier)
	}
	_ = data // Use data

	// 2. TODO: Encrypt data if targetTier requires/policy dictates, using crypto_utils.

	// 3. Write data to the target tier
	var newLocationPointer string
	targetAdapter := tsm.getAdapterForTier(targetTier)
	if targetAdapter == nil {
		return fmt.Errorf("no adapter found for target tier: %s", targetTier)
	}
	// newLocationPointer, err = targetAdapter.Write(unit.LogicalUnitID, data) // Write method on adapter
	// if err != nil { return fmt.Errorf("failed to write unit %s to %s: %w", unit.LogicalUnitID, targetTier, err) }
	_ = newLocationPointer
	return fmt.Errorf("writing to target tier not fully implemented")

	// 4. TODO: Verify write (e.g., read back and checksum, or use adapter's verification)

	// 5. Update metadata in FSM (this must be atomic)
	// updatedMetadata := unit
	// updatedMetadata.CurrentTier = targetTier
	// updatedMetadata.LocationPointer = newLocationPointer
	// updatedMetadata.LastModifiedTime = time.Now() // Or keep original if policy dictates

	// cmd := fsm.Command{Type: fsm.CommandUpdateTieringMetadata, TieringMetadata: updatedMetadata}
	// if err := tsm.fsmClient.Apply(cmd); err != nil {
	//    // IMPORTANT: Handle failure here. If FSM update fails, we might have orphaned data in targetTier.
	//    // Need a cleanup mechanism or retry for FSM update.
	//    tsm.logger.Error("CRITICAL: Failed to update FSM tiering metadata after successful write to target tier.",
	//        zap.String("unitID", unit.LogicalUnitID), zap.String("targetTier", string(targetTier)), zap.Error(err))
	//    // TODO: Attempt to delete from targetTier if FSM update is conclusively failed.
	//    return fmt.Errorf("failed to commit tiering metadata update to FSM: %w", err)
	// }

	// 6. Update local cache
	// tsm.mu.Lock()
	// tsm.localTieringMetadataCache[unit.LogicalUnitID] = updatedMetadata
	// tsm.mu.Unlock()

	// 7. If original tier was hot, inform PageManager/BufferPoolManager that the page can be evicted
	// if unit.CurrentTier == HotTier && unit.LogicalUnitID[:4] == "page" {
	//    pageID, _ := strconv.Atoi(unit.LogicalUnitID[5:])
	//    tsm.pageManager.MarkPageTieredOut(pagemanager.PageID(pageID), targetTier, newLocationPointer)
	// }

	// return nil
}

// RecallDataUnit fetches a data unit from a colder tier into the hot tier.
// This is called when a tiered-out data unit is accessed.
func (tsm *TieredStorageManager) RecallDataUnit(logicalUnitID string) ([]byte, error) {
	// 1. Get metadata from FSM (or cache)
	// meta, err := tsm.getTieringMetadata(logicalUnitID)
	// if err != nil { return nil, err}
	// if meta.CurrentTier == HotTier { return nil, fmt.Errorf("unit %s is already in hot tier", logicalUnitID) }

	// 2. Read from meta.CurrentTier using meta.LocationPointer and appropriate adapter
	// sourceAdapter := tsm.getAdapterForTier(meta.CurrentTier)
	// if sourceAdapter == nil { return nil, fmt.Errorf("no adapter for source tier %s of unit %s", meta.CurrentTier, logicalUnitID) }
	// data, err := sourceAdapter.Read(meta.LocationPointer)
	// if err != nil { return nil, fmt.Errorf("failed to read unit %s from %s: %w", logicalUnitID, meta.CurrentTier, err) }

	// 3. TODO: Decrypt if necessary

	// 4. Write to hot tier (e.g., load into BufferPoolManager)
	//    This depends on the type of logicalUnitID.
	// if logicalUnitID[:4] == "page" {
	//    pageID, _ := strconv.Atoi(logicalUnitID[5:])
	//    err := tsm.pageManager.LoadRecalledPage(pagemanager.PageID(pageID), data)
	//    if err != nil { return nil, fmt.Errorf("failed to load recalled page %s into buffer pool: %w", logicalUnitID, err) }
	// } else {
	//    return nil, fmt.Errorf("recall for unit type of %s not implemented", logicalUnitID)
	// }

	// 5. Update FSM metadata: unit is now also in HotTier (or primarily in HotTier)
	//    The policy might be to keep it in both temporarily, or move it.
	//    For simplicity, let's assume it's now considered Hot.
	// newMeta := meta
	// newMeta.CurrentTier = HotTier
	// newMeta.LocationPointer = "" // Pointer for hot tier is implicit or handled by PageManager
	// newMeta.LastAccessTime = time.Now()

	// cmd := fsm.Command{Type: fsm.CommandUpdateTieringMetadata, TieringMetadata: newMeta}
	// if err := tsm.fsmClient.Apply(cmd); err != nil {
	//    return data, fmt.Errorf("failed to update FSM tiering metadata after recall (data recalled successfully): %w", err)
	// }
	// tsm.mu.Lock()
	// tsm.localTieringMetadataCache[logicalUnitID] = newMeta
	// tsm.mu.Unlock()

	// return data, nil
	return nil, fmt.Errorf("RecallDataUnit not fully implemented")
}

// getAdapterForTier returns the appropriate storage adapter for a given tier.
func (tsm *TieredStorageManager) getAdapterForTier(tier StorageTierType) *coldstorage.ColdStorageAdapter { // Return general interface
	// This logic will expand as more tier types and adapters are added.
	// For now, assumes all non-hot tiers use the single coldStorageAdapter.
	switch tier {
	case HotTier:
		// Hot tier interaction is usually via PageManager or direct HotStorageAdapter for some ops
		tsm.logger.Warn("Attempted to get generic adapter for HotTier; direct interaction preferred.", zap.String("tier", string(tier)))
		return nil // Or return tsm.hotStorageAdapter if it fits a common interface
	case ColdTierS3, ColdTierS3GDA, ColdTierS3GF, ColdTierS3GIR, ColdTierS3GS, ColdTierGCSArchive, ColdTierGCSColdline, ColdTierGCSNearline, WarmTierCloud:
		// All these could potentially map to tsm.coldStorageAdapter if it's configured for S3/GCS.
		// This needs to be more sophisticated, possibly a map of TierType -> AdapterInstance.
		// For simplicity now, assuming coldStorageAdapter handles all these.
		if tsm.coldStorageAdapter.GetAdapterType() == "s3" && (strings.HasPrefix(string(tier), "cold_s3") || strings.HasPrefix(string(tier), "warm_s3")) {
			return tsm.coldStorageAdapter
		}
		if tsm.coldStorageAdapter.GetAdapterType() == "gcs" && (strings.HasPrefix(string(tier), "cold_gcs") || strings.HasPrefix(string(tier), "warm_gcs")) {
			return tsm.coldStorageAdapter
		}
		tsm.logger.Error("No specific cold adapter configured for tier, returning default cold adapter", zap.String("tier", string(tier)), zap.String("adapterType", tsm.coldStorageAdapter.GetAdapterType()))
		return tsm.coldStorageAdapter // Fallback, might be wrong
	// case WarmTierLocal:
	// return tsm.warmStorageAdapter // When implemented
	default:
		tsm.logger.Error("Unknown or unsupported storage tier", zap.String("tier", string(tier)))
		return nil
	}
}

// getTieringMetadata retrieves metadata for a logical unit, preferring cache, then FSM.
func (tsm *TieredStorageManager) getTieringMetadata(logicalUnitID string) (TieredDataMetadata, error) {
	tsm.mu.RLock()
	meta, found := tsm.localTieringMetadataCache[logicalUnitID]
	tsm.mu.RUnlock()
	if found {
		return meta, nil
	}

	// TODO: Fetch from FSM
	// meta, err := tsm.fsmClient.GetTieringMetadata(logicalUnitID)
	// if err != nil { return TieredDataMetadata{}, err }
	// if meta != nil {
	//    tsm.mu.Lock()
	//    tsm.localTieringMetadataCache[logicalUnitID] = *meta
	//    tsm.mu.Unlock()
	//    return *meta, nil
	// }
	return TieredDataMetadata{}, fmt.Errorf("metadata for unit %s not found", logicalUnitID)
}

// UpdateAccessStats is called by PageManager or other components when a data unit is accessed.
func (tsm *TieredStorageManager) UpdateAccessStats(logicalUnitID string, accessTime time.Time) {
	// This should update the LastAccessTime and AccessFrequencyScore in the FSM.
	// It might batch updates to FSM to avoid overhead.
	tsm.logger.Debug("Access detected, stats update pending for FSM", zap.String("unitID", logicalUnitID), zap.Time("accessTime", accessTime))

	// For now, update local cache; a background job could sync this to FSM periodically or FSM updated directly.
	tsm.mu.Lock()
	defer tsm.mu.Unlock()
	if meta, exists := tsm.localTieringMetadataCache[logicalUnitID]; exists {
		meta.LastAccessTime = accessTime
		// TODO: Update AccessFrequencyScore based on an algorithm (e.g., EWMA, counter)
		tsm.localTieringMetadataCache[logicalUnitID] = meta
	} else {
		// If not in cache, it implies it might be newly created in hot tier or needs fetch from FSM.
		// For now, log it. A robust system would fetch from FSM, update, and propose back.
		tsm.logger.Debug("Access to unit not in local tiering cache", zap.String("unitID", logicalUnitID))
	}
	// cmd := fsm.Command{Type: fsm.CommandUpdateTieringAccessStats, LogicalUnitID: logicalUnitID, AccessTime: accessTime}
	// err := tsm.fsmClient.Apply(cmd)
	// if err != nil {
	//  tsm.logger.Error("Failed to update access stats in FSM", zap.String("unitID", logicalUnitID), zap.Error(err))
	// }
}

// Close any resources held by TieredStorageManager
func (tsm *TieredStorageManager) Close() error {
	// This method might be needed if the manager holds closable resources,
	// like persistent connections to a metadata store or specific adapters.
	// For now, Stop() handles goroutine cleanup.
	tsm.logger.Info("TieredStorageManager closed.")
	return nil
}

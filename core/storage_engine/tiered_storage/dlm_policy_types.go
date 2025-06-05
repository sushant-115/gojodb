package tiered_storage

import "time"

// StorageTierType defines the type of storage tier.
type StorageTierType string

const (
	HotTier             StorageTierType = "hot"
	WarmTierLocal       StorageTierType = "warm_local_hdd"   // Example: Local HDDs
	WarmTierCloud       StorageTierType = "warm_cloud_s3_ia" // Example: S3 Infrequent Access
	ColdTierS3          StorageTierType = "cold_s3_standard"
	ColdTierS3GS        StorageTierType = "cold_s3_glacier_standard"
	ColdTierS3GIR       StorageTierType = "cold_s3_glacier_ir"       // S3 Glacier Instant Retrieval
	ColdTierS3GF        StorageTierType = "cold_s3_glacier_flexible" // S3 Glacier Flexible Retrieval (formerly S3 Glacier)
	ColdTierS3GDA       StorageTierType = "cold_s3_glacier_deep"     // S3 Glacier Deep Archive
	ColdTierGCSNearline StorageTierType = "cold_gcs_nearline"
	ColdTierGCSColdline StorageTierType = "cold_gcs_coldline"
	ColdTierGCSArchive  StorageTierType = "cold_gcs_archive"
	// Add other cloud provider tiers as needed
)

// DLMTargetEntityType specifies what the DLM policy applies to.
type DLMTargetEntityType string

const (
	UserTableData DLMTargetEntityType = "user_table_data" // Represents logical user data (e.g., pages or segments)
	WALSegments   DLMTargetEntityType = "wal_segments"
	IndexData     DLMTargetEntityType = "index_data" // Could be further refined (e.g., non-leaf B-tree pages)
)

// DLMTriggerType defines what triggers a policy evaluation/migration.
type DLMTriggerType string

const (
	Age             DLMTriggerType = "age"              // Based on data age
	AccessFrequency DLMTriggerType = "access_frequency" // Based on how often data is accessed
	Utilization     DLMTriggerType = "utilization"      // Based on source tier storage utilization
	// TODO: Add more triggers like last access time, size, etc.
)

// DLMPolicy defines a Data Lifecycle Management policy.
// This structure would be stored and managed by the Controller/FSM.
type DLMPolicy struct {
	PolicyID            string              `json:"policy_id"`             // Unique identifier for the policy
	PolicyName          string              `json:"policy_name"`           // Human-readable name
	TargetEntityType    DLMTargetEntityType `json:"target_entity_type"`    // e.g., "UserTableData", "WALSegments"
	TargetEntityMatcher string              `json:"target_entity_matcher"` // Optional: e.g., table name pattern, shard ID pattern. "*" for all.
	SourceTier          StorageTierType     `json:"source_tier"`           // Tier data is moving from
	TargetTier          StorageTierType     `json:"target_tier"`           // Tier data is moving to
	TriggerType         DLMTriggerType      `json:"trigger_type"`          // How the policy is triggered
	TriggerValue        string              `json:"trigger_value"`         // Value for the trigger (e.g., "90d" for age, ">80%" for utilization)
	MigrationSchedule   string              `json:"migration_schedule"`    // Cron-like expression for scheduled evaluation, or "continuous"
	IsEnabled           bool                `json:"is_enabled"`            // Whether the policy is active
	Priority            int                 `json:"priority"`              // For resolving policy conflicts (lower value = higher priority)
	CreatedAt           time.Time           `json:"created_at"`
	UpdatedAt           time.Time           `json:"updated_at"`
}

// TieredDataMetadata holds metadata for a logical unit of data that might be tiered.
// This would be stored in the Raft FSM, mapping LogicalUnitID to its location.
type TieredDataMetadata struct {
	LogicalUnitID        string            `json:"logical_unit_id"`    // e.g., PageID, WALSegmentName, DataSegmentID
	ShardID              string            `json:"shard_id"`           // Shard this unit belongs to
	CurrentTier          StorageTierType   `json:"current_tier"`       // Current storage tier
	LocationPointer      string            `json:"location_pointer"`   // Path if local, ObjectKey if cloud
	Size                 int64             `json:"size"`               // Size in bytes
	LastAccessTime       time.Time         `json:"last_access_time"`   // For LRU-style policies
	LastModifiedTime     time.Time         `json:"last_modified_time"` // For age-based policies
	CreationTime         time.Time         `json:"creation_time"`
	AccessFrequencyScore float64           `json:"access_frequency_score"` // Calculated score for access frequency
	Checksum             string            `json:"checksum"`               // Checksum of the data unit
	EncryptionKeyID      string            `json:"encryption_key_id"`      // Reference to encryption key if encrypted
	IsPinned             bool              `json:"is_pinned"`              // If true, prevents automatic tiering-out
	CustomTags           map[string]string `json:"custom_tags"`            // User-defined tags for finer control
}

// Package hotstorage provides adapters for the hot storage tier.
// EFSAdapter treats an NFS/EFS mount point as a hot-storage backend; it is
// structurally identical to FileStoreAdapter but is used when the backing
// filesystem is a network-attached elastic file system.
package hotstorage

import "fmt"

// EFSAdapter wraps a FileStoreAdapter pointed at an EFS/NFS mount.
type EFSAdapter struct {
	inner *HotStorageAdapter
}

// NewEFSAdapter creates an EFSAdapter rooted at mountPoint.
func NewEFSAdapter(mountPoint string) (*EFSAdapter, error) {
	inner, err := NewHotStorageAdapter(mountPoint)
	if err != nil {
		return nil, fmt.Errorf("efs: %w", err)
	}
	return &EFSAdapter{inner: inner}, nil
}

func (e *EFSAdapter) Write(unitID string, data []byte) (string, error) {
	return e.inner.Write(unitID, data)
}

func (e *EFSAdapter) Read(unitID string) ([]byte, error) {
	return e.inner.Read(unitID)
}

func (e *EFSAdapter) Delete(unitID string) error {
	return e.inner.Delete(unitID)
}

func (e *EFSAdapter) Exists(unitID string) bool {
	return e.inner.Exists(unitID)
}

func (e *EFSAdapter) GetAdapterType() string { return "efs" }

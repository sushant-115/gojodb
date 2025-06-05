package coldstorage

type ColdStorageAdapter struct {
}

func (c *ColdStorageAdapter) GetAdapterType() string {
	return "s3"
}

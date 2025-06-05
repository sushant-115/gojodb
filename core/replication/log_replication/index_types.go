package logreplication

// IndexType is a string alias for identifying different index types.
type IndexType string

const (
	BTreeIndexType       IndexType = "btree"
	InvertedIndexType    IndexType = "inverted_index"
	SpatialIndexType     IndexType = "spatial_index"
	PlaceholderIndexType IndexType = "placeholder"
)

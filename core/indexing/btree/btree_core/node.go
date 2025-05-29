package btree_core

// import (
// 	"bytes"
// 	"encoding/binary"
// 	"fmt"
// 	"hash/crc32"
// 	"io"
// 	"log"
// )

// // --- BTree Node Serialization/Deserialization ---
// const ( /* ... (checksumSize etc. same as before) ... */
// 	nodeHeaderSizeOffset = 0
// 	isLeafOffset         = 2
// 	numKeysOffset        = 3
// )

// // --- BTree Node Serialization/Deserialization ---

// // Header fields within a Node's page data (offsets relative to start of page data)
// const (
// 	nodeHeaderFlagsOffset   = 0 // For isLeaf, other flags (1 byte)
// 	nodeHeaderNumKeysOffset = 1 // Number of keys (2 bytes, uint16)
// 	// Key/Value/Child data follows
// 	// Checksum is at the very end of the page
// )

// // Node represents an in-memory B-tree node.
// type Node[K any, V any] struct {
// 	pageID       PageID
// 	isLeaf       bool
// 	keys         []K
// 	values       []V
// 	childPageIDs []PageID
// 	tree         *BTree[K, V] // Reference to the parent BTree for BPM/DiskManager access
// }

// func (n *Node[K, V]) GetPageID() PageID {
// 	return n.pageID
// }

// // serialize converts the Node into a byte slice and writes it to the provided Page's data buffer.
// // It also calculates and writes the checksum.
// func (n *Node[K, V]) serialize(page *Page, keySerializer func(K) ([]byte, error), valueSerializer func(V) ([]byte, error)) error {
// 	if n.tree == nil || n.tree.bpm == nil {
// 		return ErrBTreeNotInitializedProperly
// 	}
// 	pageSize := n.tree.bpm.pageSize
// 	buffer := new(bytes.Buffer)

// 	// Write Node Header:
// 	// Flags (1 byte): bit 0 for isLeaf, other bits for future flags
// 	var flags byte
// 	if n.isLeaf {
// 		flags |= (1 << 0) // Set 0th bit if it's a leaf
// 	}
// 	if err := binary.Write(buffer, binary.LittleEndian, flags); err != nil {
// 		return fmt.Errorf("%w: writing flags: %v", ErrSerialization, err)
// 	}

// 	// Number of keys (uint16)
// 	numKeys := uint16(len(n.keys))
// 	if err := binary.Write(buffer, binary.LittleEndian, numKeys); err != nil {
// 		return fmt.Errorf("%w: writing numKeys: %v", ErrSerialization, err)
// 	}

// 	// Serialize Keys
// 	for _, k := range n.keys {
// 		keyData, err := keySerializer(k)
// 		if err != nil {
// 			return fmt.Errorf("%w: serializing key: %v", ErrSerialization, err)
// 		}
// 		if err := binary.Write(buffer, binary.LittleEndian, uint16(len(keyData))); err != nil { // Length of key data
// 			return err
// 		}
// 		if _, err := buffer.Write(keyData); err != nil { // Key data
// 			return err
// 		}
// 	}

// 	// Serialize Values
// 	for _, v := range n.values {
// 		valData, err := valueSerializer(v)
// 		if err != nil {
// 			return fmt.Errorf("%w: serializing value: %v", ErrSerialization, err)
// 		}
// 		if err := binary.Write(buffer, binary.LittleEndian, uint16(len(valData))); err != nil { // Length of value data
// 			return err
// 		}
// 		if _, err := buffer.Write(valData); err != nil { // Value data
// 			return err
// 		}
// 	}

// 	// Serialize Child Page IDs (if not a leaf)
// 	if !n.isLeaf {
// 		numChildren := uint16(len(n.childPageIDs))
// 		if err := binary.Write(buffer, binary.LittleEndian, numChildren); err != nil {
// 			return fmt.Errorf("%w: writing numChildren: %v", ErrSerialization, err)
// 		}
// 		for _, childID := range n.childPageIDs {
// 			if err := binary.Write(buffer, binary.LittleEndian, childID); err != nil {
// 				return fmt.Errorf("%w: writing childPageID: %v", ErrSerialization, err)
// 			}
// 		}
// 	}

// 	serializedData := buffer.Bytes()

// 	// Check if serialized data fits within the page (excluding checksum space)
// 	if len(serializedData)+checksumSize > pageSize {
// 		return fmt.Errorf("%w: node data (%d bytes) + checksum (%d) exceeds page size (%d) for page %d",
// 			ErrSerialization, len(serializedData), checksumSize, pageSize, n.pageID)
// 	}

// 	// Copy serialized data into the page's buffer
// 	pageData := page.GetData()
// 	copy(pageData, serializedData)

// 	// Pad remaining space with zeros (important for consistent checksum calculation)
// 	for i := len(serializedData); i < pageSize-checksumSize; i++ {
// 		pageData[i] = 0
// 	}

// 	// Calculate and write checksum
// 	// The checksum is calculated over the entire page data *excluding* the checksum itself.
// 	checksum := crc32.ChecksumIEEE(pageData[:pageSize-checksumSize])
// 	binary.LittleEndian.PutUint32(pageData[pageSize-checksumSize:], checksum)

// 	// Logging for debugging serialization (can be extensive)
// 	log.Printf("SER: PageID %d, numKeys %d, isLeaf %v, serializedLen: %d, calculatedChecksum: 0x%x",
// 		n.pageID, len(n.keys), n.isLeaf, len(serializedData), checksum)
// 	// log.Printf("SER: Page %d data (first 64 bytes): %x", n.pageID, page.GetData()[:64])
// 	// log.Printf("SER: Page %d data (last %d bytes, includes checksum): %x", n.pageID, checksumSize+32, page.GetData()[pageSize-checksumSize-32:])

// 	// Mark the page as dirty, so it will be flushed to disk by the BufferPoolManager
// 	page.SetDirty(true)
// 	return nil
// }

// // deserialize reads node data from the provided Page's data buffer and reconstructs the Node.
// // It also verifies the checksum.
// func (n *Node[K, V]) deserialize(page *Page, keyDeserializer func([]byte) (K, error), valueDeserializer func([]byte) (V, error)) error {
// 	if n.tree == nil || n.tree.bpm == nil {
// 		n.keys = make([]K, 0) // Initialize slices even on error for safety
// 		n.values = make([]V, 0)
// 		n.childPageIDs = make([]PageID, 0)
// 		return ErrBTreeNotInitializedProperly
// 	}
// 	pageSize := n.tree.bpm.pageSize
// 	pageData := page.GetData()

// 	// --- CRITICAL CHECKSUM VERIFICATION ---
// 	// Extract stored checksum from the end of the page
// 	storedChecksumBytes := pageData[pageSize-checksumSize:]
// 	storedChecksum := binary.LittleEndian.Uint32(storedChecksumBytes)

// 	// Calculate checksum from the rest of the page data
// 	calculatedChecksum := crc32.ChecksumIEEE(pageData[:pageSize-checksumSize])

// 	log.Printf("DESER: PageID %d. Stored Checksum: 0x%x, Calculated Checksum: 0x%x",
// 		page.GetPageID(), storedChecksum, calculatedChecksum)
// 	// log.Printf("DESER: Page %d data (first 64 bytes for checksum): %x", page.GetPageID(), pageData[:64])
// 	// log.Printf("DESER: Page %d data (last %d bytes, includes checksum): %x", page.GetPageID(), checksumSize+32, pageData[pageSize-checksumSize-32:])

// 	if storedChecksum != calculatedChecksum {
// 		log.Printf("ERROR: CHECKSUM MISMATCH DETECTED for PageID %d: Stored=0x%x, Calculated=0x%x. Raw page data (first 64 bytes for debug): %x",
// 			page.GetPageID(), storedChecksum, calculatedChecksum, pageData[:64])
// 		// Initialize node slices to empty to prevent using corrupt data
// 		n.keys = make([]K, 0)
// 		n.values = make([]V, 0)
// 		n.childPageIDs = make([]PageID, 0)
// 		return fmt.Errorf("%w: stored=0x%x, calculated=0x%x for page %d", ErrChecksumMismatch, storedChecksum, calculatedChecksum, page.GetPageID())
// 	}
// 	// --- END CRITICAL CHECKSUM VERIFICATION ---

// 	buffer := bytes.NewReader(pageData[:pageSize-checksumSize]) // Read from data *before* checksum

// 	// Read Node Header:
// 	// Flags (1 byte)
// 	var flags byte
// 	if err := binary.Read(buffer, binary.LittleEndian, &flags); err != nil {
// 		return fmt.Errorf("%w: reading flags: %v", ErrDeserialization, err)
// 	}
// 	n.isLeaf = (flags & (1 << 0)) != 0 // Check 0th bit for isLeaf

// 	// Number of keys (uint16)
// 	var numKeys uint16
// 	if err := binary.Read(buffer, binary.LittleEndian, &numKeys); err != nil {
// 		return fmt.Errorf("%w: reading numKeys: %v", ErrDeserialization, err)
// 	}
// 	n.keys = make([]K, numKeys)
// 	n.values = make([]V, numKeys)

// 	// Deserialize Keys
// 	for i := uint16(0); i < numKeys; i++ {
// 		var keyDataLen uint16
// 		if err := binary.Read(buffer, binary.LittleEndian, &keyDataLen); err != nil {
// 			return fmt.Errorf("%w: reading key length for key %d: %v", ErrDeserialization, i, err)
// 		}
// 		keyData := make([]byte, keyDataLen)
// 		if _, err := io.ReadFull(buffer, keyData); err != nil {
// 			return fmt.Errorf("%w: reading key data for key %d: %v", ErrDeserialization, i, err)
// 		}
// 		key, err := keyDeserializer(keyData)
// 		if err != nil {
// 			return fmt.Errorf("%w: deserializing key %d: %v", ErrDeserialization, i, err)
// 		}
// 		n.keys[i] = key
// 	}

// 	// Deserialize Values
// 	for i := uint16(0); i < numKeys; i++ {
// 		var valDataLen uint16
// 		if err := binary.Read(buffer, binary.LittleEndian, &valDataLen); err != nil {
// 			return fmt.Errorf("%w: reading value length for value %d: %v", ErrDeserialization, i, err)
// 		}
// 		valData := make([]byte, valDataLen)
// 		if _, err := io.ReadFull(buffer, valData); err != nil {
// 			return fmt.Errorf("%w: reading value data for value %d: %v", ErrDeserialization, i, err)
// 		}
// 		val, err := valueDeserializer(valData)
// 		if err != nil {
// 			return fmt.Errorf("%w: deserializing value %d: %v", ErrDeserialization, i, err)
// 		}
// 		n.values[i] = val
// 	}

// 	// Deserialize Child Page IDs (if not a leaf)
// 	if !n.isLeaf {
// 		var numChildren uint16
// 		if err := binary.Read(buffer, binary.LittleEndian, &numChildren); err != nil {
// 			return fmt.Errorf("%w: reading numChildren: %v", ErrDeserialization, err)
// 		}
// 		n.childPageIDs = make([]PageID, numChildren)
// 		for i := uint16(0); i < numChildren; i++ {
// 			if err := binary.Read(buffer, binary.LittleEndian, &n.childPageIDs[i]); err != nil {
// 				return fmt.Errorf("%w: reading childPageID %d: %v", ErrDeserialization, i, err)
// 			}
// 		}
// 	} else {
// 		n.childPageIDs = make([]PageID, 0) // Ensure it's an empty slice for leaves
// 	}

// 	n.pageID = page.GetPageID() // Set the node's page ID from the page object
// 	log.Printf("DESER: Successfully deserialized PageID %d, isLeaf: %v, numKeys: %d", n.pageID, n.isLeaf, len(n.keys))
// 	return nil
// }

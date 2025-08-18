package indexedwritesservice

import (
	"context"
	"fmt"
	"log"
	"sync"

	pb "github.com/sushant-115/gojodb/api/proto"
	"github.com/sushant-115/gojodb/core/indexmanager"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	// Use insecure for local testing
)

// IndexedWriteService implements the gRPC IndexedWriteService.
type IndexedWriteService struct {
	pb.UnimplementedIndexedWriteServiceServer
	nodeID        string
	slotID        uint32
	indexManagers map[string]indexmanager.IndexManager // indexType -> IndexManager
	writeMutex    sync.Mutex                           // Protects write operations (and LSN updates)
}

// NewIndexedWriteService creates a new IndexedWriteService.
func NewIndexedWriteService(nodeID string, slotID uint32, indexManagers map[string]indexmanager.IndexManager) *IndexedWriteService {
	return &IndexedWriteService{
		nodeID:        nodeID,
		slotID:        slotID,
		indexManagers: indexManagers,
	}
}

// Put handles a single key-value put operation.
func (s *IndexedWriteService) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	s.writeMutex.Lock()
	defer s.writeMutex.Unlock()

	// Operations go to B-tree (primary K/V store) and potentially others.
	// In a real system, WAL write happens first, then apply to in-memory/disk structures.
	// For now, directly apply to B-tree, and then potentially other indexes.

	// 1. Apply to B-tree
	if err := s.indexManagers["btree"].Put(req.Key, req.Value); err != nil {
		log.Printf("Node %s, Slot %d: Failed to put key %s to btree: %v", s.nodeID, s.slotID, req.Key, err)
		return &pb.PutResponse{Success: false, Message: err.Error()}, status.Errorf(codes.Internal, "put failed (btree): %v", err)
	}

	// // 2. Optionally, apply to inverted index if value contains text content (assuming JSON document)
	// var docContent struct {
	// 	Text string `json:"text"` // Example field for text content
	// 	ID   string `json:"id"`   // Example field for document ID
	// }
	// if err := json.Unmarshal(req.Value, &docContent); err == nil && docContent.Text != "" && docContent.ID != "" {
	// 	if invIdxMgr, ok := s.indexManagers["inverted"].(*indexmanager.InvertedIndexManager); ok {
	// 		err := invIdxMgr.AddDocument(docContent.ID, docContent.Text)
	// 		if err != nil {
	// 			log.Printf("Node %s, Slot %d: Warning: Failed to add document %s to inverted index: %v", s.nodeID, s.slotID, docContent.ID, err)
	// 			// Decide if this is a critical error or just a warning based on your design
	// 		}
	// 	}
	// }

	// // 3. Optionally, apply to spatial index if value contains spatial data (assuming JSON document)
	// var spatialData struct {
	// 	ID string  `json:"id"` // Example field for spatial object ID
	// 	X1 float64 `json:"x1"`
	// 	Y1 float64 `json:"y1"`
	// 	X2 float64 `json:"x2"`
	// 	Y2 float64 `json:"y2"`
	// }
	// if err := json.Unmarshal(req.Value, &spatialData); err == nil && spatialData.ID != "" {
	// 	if spatialIdxMgr, ok := s.indexManagers["spatial"].(*indexmanager.SpatialIndexManager); ok {
	// 		rect := spatial.Rect{
	// 			MinX: spatialData.X1, MinY: spatialData.Y1, MaxX: spatialData.X2, MaxY: spatialData.Y2,
	// 		}
	// 		err := spatialIdxMgr.InsertSpatial(rect, spatialData.ID)
	// 		if err != nil {
	// 			log.Printf("Node %s, Slot %d: Warning: Failed to insert spatial data %s to spatial index: %v", s.nodeID, s.slotID, spatialData.ID, err)
	// 		}
	// 	}
	// }

	log.Printf("Node %s, Slot %d: Put key=%s (applied to multiple indexes)", s.nodeID, s.slotID, req.Key)
	return &pb.PutResponse{Success: true, Message: "Key-value pair put successfully"}, nil
}

// Delete handles a single key delete operation.
func (s *IndexedWriteService) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	s.writeMutex.Lock()
	defer s.writeMutex.Unlock()

	// 1. Delete from B-tree
	if err := s.indexManagers["btree"].Delete(req.Key); err != nil {
		log.Printf("Node %s, Slot %d: Failed to delete key %s from btree: %v", s.nodeID, s.slotID, req.Key, err)
		return &pb.DeleteResponse{Success: false, Message: err.Error()}, status.Errorf(codes.Internal, "delete failed (btree): %v", err)
	}

	// 2. Optionally, delete from inverted index (assuming key is docID or associated with docID)
	// You might need to fetch the original value to get the docID if it's not the key
	// For simplicity, assume key is directly the docID for inverted/spatial indices
	if _, ok := s.indexManagers["inverted"].(*indexmanager.InvertedIndexManager); ok {
		// This requires a DeleteDocument method on InvertedIndex
		// if err := invIdxMgr.Delete(req.Key); err != nil { // Assuming key is docID
		// 	log.Printf("Node %s, Slot %d: Warning: Failed to delete document %s from inverted index: %v", s.nodeID, s.slotID, req.Key, err)
		// }
	}

	// 3. Optionally, delete from spatial index
	if spatialIdxMgr, ok := s.indexManagers["spatial"].(*indexmanager.SpatialIndexManager); ok {
		if err := spatialIdxMgr.DeleteSpatial(req.Key); err != nil { // Assuming key is dataID
			log.Printf("Node %s, Slot %d: Warning: Failed to delete spatial data %s from spatial index: %v", s.nodeID, s.slotID, req.Key, err)
		}
	}

	log.Printf("Node %s, Slot %d: Deleted key=%s (applied to multiple indexes)", s.nodeID, s.slotID, req.Key)
	return &pb.DeleteResponse{Success: true, Message: "Key-value pair deleted successfully"}, nil
}

// BulkPut handles multiple key-value put operations.
func (s *IndexedWriteService) BulkPut(ctx context.Context, req *pb.BulkPutRequest) (*pb.BulkPutResponse, error) {
	s.writeMutex.Lock()
	defer s.writeMutex.Unlock()

	for _, entry := range req.Entries {
		// Call the single Put method internally to reuse logic and ensure WAL/index updates
		// This also ensures each Put is logged (if WAL is implemented).
		_, err := s.Put(ctx, &pb.PutRequest{Key: entry.Key, Value: entry.Value})
		if err != nil {
			log.Printf("Node %s, Slot %d: Failed to bulk put entry %s: %v", s.nodeID, s.slotID, entry.Key, err)
			return &pb.BulkPutResponse{Success: false, Message: fmt.Sprintf("Bulk put failed for key %s: %v", entry.Key, err)}, status.Errorf(codes.Internal, "bulk put failed")
		}
	}
	log.Printf("Node %s, Slot %d: Bulk put %d entries completed.", s.nodeID, s.slotID, len(req.Entries))
	return &pb.BulkPutResponse{Success: true, Message: "Bulk put completed successfully"}, nil
}

// BulkDelete handles multiple key delete operations.
func (s *IndexedWriteService) BulkDelete(ctx context.Context, req *pb.BulkDeleteRequest) (*pb.BulkDeleteResponse, error) {
	s.writeMutex.Lock()
	defer s.writeMutex.Unlock()

	for _, key := range req.Keys {
		// Call the single Delete method internally
		_, err := s.Delete(ctx, &pb.DeleteRequest{Key: key})
		if err != nil {
			log.Printf("Node %s, Slot %d: Failed to bulk delete key %s: %v", s.nodeID, s.slotID, key, err)
			return &pb.BulkDeleteResponse{Success: false, Message: fmt.Sprintf("Bulk delete failed for key %s: %v", key, err)}, status.Errorf(codes.Internal, "bulk delete failed")
		}
	}
	log.Printf("Node %s, Slot %d: Bulk deleted %d keys completed.", s.nodeID, s.slotID, len(req.Keys))
	return &pb.BulkDeleteResponse{Success: true, Message: "Bulk delete completed successfully"}, nil
}

package indexedreadsservice

import (
	"context"
	"log"

	pb "github.com/sushant-115/gojodb/api/proto"
	"github.com/sushant-115/gojodb/core/indexmanager"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IndexedReadService implements the gRPC IndexedReadService.
type IndexedReadService struct {
	pb.UnimplementedIndexedReadServiceServer
	nodeID        string
	slotID        uint32
	indexManagers map[string]indexmanager.IndexManager
}

// NewIndexedReadService creates a new IndexedReadService.
func NewIndexedReadService(nodeID string, slotID uint32, indexManagers map[string]indexmanager.IndexManager) *IndexedReadService {
	return &IndexedReadService{
		nodeID:        nodeID,
		slotID:        slotID,
		indexManagers: indexManagers,
	}
}

// Get handles a single key-value get operation.
func (s *IndexedReadService) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	// Always get from the primary K/V store (B-tree)
	val, found := s.indexManagers["btree"].Get(req.Key)
	log.Printf("Node %s, Slot %d: Get key=%s, found=%t", s.nodeID, s.slotID, req.Key, found)
	return &pb.GetResponse{Value: val, Found: found}, nil
}

// GetRange handles a range query.
func (s *IndexedReadService) GetRange(ctx context.Context, req *pb.GetRangeRequest) (*pb.GetRangeResponse, error) {
	// Always get from the primary K/V store (B-tree)
	entries, err := s.indexManagers["btree"].GetRange(req.StartKey, req.EndKey, req.Limit)
	if err != nil {
		log.Printf("Node %s, Slot %d: Failed to get range for start_key %s: %v", s.nodeID, s.slotID, req.StartKey, err)
		return nil, status.Errorf(codes.Internal, "get range failed: %v", err)
	}
	log.Printf("Node %s, Slot %d: GetRange start=%s, end=%s, limit=%d, returned %d entries", s.nodeID, s.slotID, req.StartKey, req.EndKey, req.Limit, len(entries))
	return &pb.GetRangeResponse{Entries: entries}, nil
}

// TextSearch handles a text search query.
func (s *IndexedReadService) TextSearch(ctx context.Context, req *pb.TextSearchRequest) (*pb.TextSearchResponse, error) {
	// Delegate to the inverted index manager
	if invIdxMgr, ok := s.indexManagers["inverted"].(*indexmanager.InvertedIndexManager); ok {
		results, err := invIdxMgr.TextSearch(req.Query, req.IndexName, req.Limit)
		if err != nil {
			log.Printf("Node %s, Slot %d: Failed text search for query %s: %v", s.nodeID, s.slotID, req.Query, err)
			return nil, status.Errorf(codes.Internal, "text search failed: %v", err)
		}
		log.Printf("Node %s, Slot %d: TextSearch query=%s, returned %d results", s.nodeID, s.slotID, req.Query, len(results))
		return &pb.TextSearchResponse{Results: results}, nil
	}
	return nil, status.Errorf(codes.Unimplemented, "TextSearch not implemented or inverted index manager not found")
}

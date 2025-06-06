package indexedreadsservice

// import (
// 	"context"
// 	"encoding/json" // For converting BTree string data back to Document
// 	"fmt"

// 	pb "github.com/sushant-115/gojodb/api/proto" // Adjust import path
// 	"github.com/sushant-115/gojodb/core/indexing/btree"
// 	"github.com/sushant-115/gojodb/core/indexing/inverted_index"
// 	"github.com/sushant-115/gojodb/core/indexing/spatial"
// 	"go.uber.org/zap"
// 	"google.golang.org/protobuf/types/known/structpb"
// )

// // IndexedReadServer implements the IndexedReadService gRPC service.
// type IndexedReadServer struct {
// 	pb.UnimplementedIndexedReadServiceServer
// 	BTreeStore    *btree.BTree[string, string]
// 	InvertedIndex *inverted_index.InvertedIndex
// 	SpatialIndex  *spatial.SpatialIndexManager
// 	Logger        *zap.Logger
// }

// // NewIndexedReadServer creates a new IndexedReadServer.
// func NewIndexedReadServer(
// 	btreeStore *btree.BTree[string, string],
// 	invertedIdx *inverted_index.InvertedIndex,
// 	spatialIdx *spatial.SpatialIndexManager,
// 	logger *zap.Logger,
// ) *IndexedReadServer {
// 	return &IndexedReadServer{
// 		BTreeStore:    btreeStore,
// 		InvertedIndex: invertedIdx,
// 		SpatialIndex:  spatialIdx,
// 		Logger:        logger.Named("indexed_read_service"),
// 	}
// }

// // GetDocument retrieves a document by its ID.
// func (s *IndexedReadServer) GetDocument(ctx context.Context, req *pb.GetDocumentRequest) (*pb.GetDocumentResponse, error) {
// 	s.Logger.Info("GetDocument request received", zap.String("docID", req.DocumentId))
// 	if req.DocumentId == "" {
// 		return &pb.GetDocumentResponse{Found: false}, fmt.Errorf("document ID is required")
// 	}

// 	// Retrieve from BTree (assuming value is JSON string of the document data)
// 	valueStr, found, err := s.BTreeStore.Search(req.DocumentId)
// 	if !found || err != nil {
// 		s.Logger.Info("Document not found in BTree", zap.String("docID", req.DocumentId))
// 		return &pb.GetDocumentResponse{Found: false}, nil
// 	}

// 	// Deserialize JSON string back to structpb.Struct
// 	docData := &structpb.Struct{}
// 	err = json.Unmarshal([]byte(valueStr), docData) // structpb.Struct has UnmarshalJSON method
// 	if err != nil {
// 		s.Logger.Error("Failed to unmarshal document data from BTree", zap.String("docID", req.DocumentId), zap.Error(err))
// 		return &pb.GetDocumentResponse{Found: false}, fmt.Errorf("failed to parse document data: %v", err)
// 	}

// 	return &pb.GetDocumentResponse{
// 		Found: true,
// 		Document: &pb.Document{
// 			Id:   req.DocumentId,
// 			Data: docData,
// 			// Metadata would need to be stored separately or alongside if needed
// 		},
// 	}, nil
// }

// // SearchInvertedIndex performs a search using the inverted index.
// func (s *IndexedReadServer) SearchInvertedIndex(ctx context.Context, req *pb.SearchInvertedIndexRequest) (*pb.SearchResponse, error) {
// 	s.Logger.Info("SearchInvertedIndex request received", zap.String("query", req.Query))
// 	if req.Query == "" {
// 		return nil, fmt.Errorf("search query is required")
// 	}

// 	// Perform search on InvertedIndex
// 	// The Search method in InvertedIndex needs to be adapted to handle complex queries,
// 	// pagination (limit/offset), and potentially field-specific searches.
// 	// For now, assuming a simple Search that returns a list of document IDs.
// 	docIDs, err := s.InvertedIndex.Search(req.Query) // This needs refinement based on actual InvertedIndex.Search capabilities
// 	if err != nil {
// 		s.Logger.Error("Inverted index search failed", zap.String("query", req.Query), zap.Error(err))
// 		return nil, fmt.Errorf("inverted index search failed: %v", err)
// 	}

// 	// TODO: Implement pagination (limit/offset) for docIDs before fetching
// 	// Paginate docIDs
// 	paginatedDocIDs := docIDs
// 	if req.Offset > 0 && int(req.Offset) < len(docIDs) {
// 		paginatedDocIDs = paginatedDocIDs[req.Offset:]
// 	}
// 	if req.Limit > 0 && int(req.Limit) < len(paginatedDocIDs) {
// 		paginatedDocIDs = paginatedDocIDs[:req.Limit]
// 	}

// 	// Fetch full documents from BTree based on IDs
// 	var documents []*pb.Document
// 	for _, docID := range paginatedDocIDs {
// 		// This could be optimized (e.g., batch Get from BTree if BTree supports it)
// 		docResp, err := s.GetDocument(ctx, &pb.GetDocumentRequest{DocumentId: docID})
// 		if err == nil && docResp.Found {
// 			documents = append(documents, docResp.Document)
// 		} else {
// 			s.Logger.Warn("Document ID from inverted index search not found in BTree or error fetching", zap.String("docID", docID), zap.Error(err))
// 		}
// 	}

// 	return &pb.SearchResponse{
// 		Documents: documents,
// 		TotalHits: int64(len(docIDs)), // Total potential hits before pagination
// 	}, nil
// }

// // SearchSpatialIndex performs a search using the spatial index.
// func (s *IndexedReadServer) SearchSpatialIndex(ctx context.Context, req *pb.SearchSpatialIndexRequest) (*pb.SearchResponse, error) {
// 	s.Logger.Info("SearchSpatialIndex request received", zap.Any("queryShape", req.QueryShape), zap.Any("queryType", req.QueryType))
// 	if req.QueryShape == nil {
// 		return nil, fmt.Errorf("query shape is required")
// 	}
// 	if req.QueryShape.TopLeft == nil || req.QueryShape.BottomRight == nil {
// 		return nil, fmt.Errorf("query shape definition is incomplete")
// 	}

// 	// Convert pb.GeoShape to bounds for SpatialIndex.SearchIntersect
// 	// Assuming SpatialIndex.SearchIntersect takes minX, minY, maxX, maxY
// 	minX := req.QueryShape.TopLeft.Longitude
// 	minY := req.QueryShape.BottomRight.Latitude // Typical for geo: min Lat is bottom
// 	maxX := req.QueryShape.BottomRight.Longitude
// 	maxY := req.QueryShape.TopLeft.Latitude // Typical for geo: max Lat is top

// 	// Ensure minX <= maxX and minY <= maxY (can be tricky with geo wrap-around, not handled here)
// 	if minX > maxX {
// 		minX, maxX = maxX, minX
// 	}
// 	if minY > maxY {
// 		minY, maxY = maxY, minY
// 	}

// 	var results []interface{} // SpatialIndex.SearchIntersect returns []interface{}
// 	var err error

// 	// The SpatialIndex.SearchIntersect is for intersection.
// 	// TODO: SpatialIndex needs methods for CONTAINS, WITHIN queries if SpatialQueryType is used.
// 	// For now, all query types will use SearchIntersect as a placeholder.
// 	switch req.QueryType {
// 	case pb.SpatialQueryType_INTERSECTS:
// 		rect := spatial.Rect{MinX: minX, MinY: minY, MaxX: maxX, MaxY: maxY}
// 		spatialData, _ := s.SpatialIndex.Query(rect)
// 		results = append(results, spatialData)
// 	case pb.SpatialQueryType_CONTAINS:
// 		s.Logger.Warn("SpatialQueryType_CONTAINS not fully implemented, using INTERSECTS logic.")
// 		rect := spatial.Rect{MinX: minX, MinY: minY, MaxX: maxX, MaxY: maxY}
// 		spatialData, _ := s.SpatialIndex.Query(rect)
// 		results = append(results, spatialData)
// 	case pb.SpatialQueryType_WITHIN:
// 		s.Logger.Warn("SpatialQueryType_WITHIN not fully implemented, using INTERSECTS logic.")
// 		rect := spatial.Rect{MinX: minX, MinY: minY, MaxX: maxX, MaxY: maxY}
// 		spatialData, _ := s.SpatialIndex.Query(rect)
// 		results = append(results, spatialData)
// 	default:
// 		return nil, fmt.Errorf("unsupported spatial query type: %s", req.QueryType)
// 	}

// 	if err != nil { // This err check is currently dead code as SearchIntersect doesn't return error
// 		s.Logger.Error("Spatial index search failed", zap.Error(err))
// 		return nil, fmt.Errorf("spatial index search failed: %v", err)
// 	}

// 	// The results from SpatialIndex.SearchIntersect are []interface{}, where each item
// 	// is expected to be the 'obj' passed during Insert, which should be map[string]interface{}
// 	// containing at least an "id" field for the document ID.
// 	var docIDs []string
// 	for _, res := range results {
// 		if dataMap, ok := res.(map[string]interface{}); ok {
// 			if idVal, idOk := dataMap["id"].(string); idOk {
// 				docIDs = append(docIDs, idVal)
// 			} else {
// 				s.Logger.Warn("Spatial index result item missing 'id' string attribute", zap.Any("item", res))
// 			}
// 		} else {
// 			s.Logger.Warn("Spatial index result item not of expected type map[string]interface{}", zap.Any("item", res))
// 		}
// 	}

// 	// TODO: Implement pagination (limit/offset) for docIDs before fetching.
// 	paginatedDocIDs := docIDs
// 	if req.Offset > 0 && int(req.Offset) < len(docIDs) {
// 		paginatedDocIDs = paginatedDocIDs[req.Offset:]
// 	}
// 	if req.Limit > 0 && int(req.Limit) < len(paginatedDocIDs) {
// 		paginatedDocIDs = paginatedDocIDs[:req.Limit]
// 	}

// 	var documents []*pb.Document
// 	for _, docID := range paginatedDocIDs {
// 		docResp, errGet := s.GetDocument(ctx, &pb.GetDocumentRequest{DocumentId: docID})
// 		if errGet == nil && docResp.Found {
// 			documents = append(documents, docResp.Document)
// 		} else {
// 			s.Logger.Warn("DocID from spatial search not found/error fetching", zap.String("docID", docID), zap.Error(errGet))
// 		}
// 	}

// 	return &pb.SearchResponse{
// 		Documents: documents,
// 		TotalHits: int64(len(docIDs)),
// 	}, nil
// }

// // RangeScan performs a B-Tree range scan.
// func (s *IndexedReadServer) RangeScan(req *pb.RangeScanRequest, stream pb.IndexedReadService_RangeScanServer) error {
// 	s.Logger.Info("RangeScan request received", zap.String("startKey", req.StartKey), zap.String("endKey", req.EndKey), zap.Int32("limit", req.Limit))

// 	// Use BTree's iterator for range scan.
// 	// This assumes BTree has an iterator that can provide key-value pairs.
// 	// For simplicity, if BTree.RangeScan(start, end) returns []KeyValuePair:
// 	// items, err := s.BTreeStore.RangeScan(req.StartKey, req.EndKey, int(req.Limit))
// 	// if err != nil {
// 	//  s.Logger.Error("BTree range scan failed", zap.Error(err))
// 	//  return fmt.Errorf("BTree range scan failed: %v", err)
// 	// }
// 	// for _, item := range items {
// 	//  docData := &structpb.Struct{}
// 	//  if err := json.Unmarshal([]byte(item.Value), docData); err != nil {
// 	//      s.Logger.Error("Failed to unmarshal document data from BTree during range scan", zap.String("key", item.Key), zap.Error(err))
// 	//      continue // Skip corrupted item
// 	//  }
// 	//  if err := stream.Send(&pb.Document{Id: item.Key, Data: docData}); err != nil {
// 	//      return err
// 	//  }
// 	// }
// 	// The above is a placeholder. A true streaming implementation from BTree iterator is better.

// 	s.Logger.Warn("RangeScan is a placeholder and needs actual BTree iterator integration for streaming.")
// 	// Simulate streaming a few dummy documents for now if BTree.RangeScan isn't ready
// 	for i := 0; i < 2 && (req.Limit == 0 || i < int(req.Limit)); i++ {
// 		dummyID := fmt.Sprintf("%s_item%d", req.StartKey, i+1)
// 		if req.EndKey != "" && dummyID >= req.EndKey {
// 			break
// 		}
// 		data, _ := structpb.NewStruct(map[string]interface{}{"message": "dummy item for range scan", "id": dummyID, "index": i})
// 		doc := &pb.Document{Id: dummyID, Data: data}
// 		if err := stream.Send(doc); err != nil {
// 			s.Logger.Error("Error sending document in RangeScan stream", zap.Error(err))
// 			return err
// 		}
// 	}

// 	return nil
// }

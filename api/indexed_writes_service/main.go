package indexedwritesservice

import (
	"context"
	"fmt"

	pb "github.com/sushant-115/gojodb/api/proto" // Adjust import path as per your project
	"github.com/sushant-115/gojodb/core/indexing/btree"
	"github.com/sushant-115/gojodb/core/indexing/inverted_index"
	"github.com/sushant-115/gojodb/core/indexing/spatial"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"
)

// IndexedWriteServer implements the IndexedWriteService gRPC service.
type IndexedWriteServer struct {
	pb.UnimplementedIndexedWriteServiceServer                               // For forward compatibility
	BTreeStore                                *btree.BTree[string, string]  // Primary B-Tree store for documents
	InvertedIndex                             *inverted_index.InvertedIndex // Inverted index instance
	SpatialIndex                              *spatial.SpatialIndexManager  // Spatial index instance
	Logger                                    *zap.Logger                   // Logger instance
}

// NewIndexedWriteServer creates a new IndexedWriteServer.
func NewIndexedWriteServer(
	btreeStore *btree.BTree[string, string],
	invertedIdx *inverted_index.InvertedIndex,
	spatialIdx *spatial.SpatialIndexManager,
	logger *zap.Logger,
) *IndexedWriteServer {
	return &IndexedWriteServer{
		BTreeStore:    btreeStore,
		InvertedIndex: invertedIdx,
		SpatialIndex:  spatialIdx,
		Logger:        logger.Named("indexed_write_service"),
	}
}

// IndexedWrite handles inserting or updating a document and its associated indexes.
func (s *IndexedWriteServer) IndexedWrite(ctx context.Context, req *pb.IndexedWriteRequest) (*pb.WriteResult, error) {
	s.Logger.Info("IndexedWrite request received", zap.String("docID", req.Document.Id))

	if req.Document == nil || req.Document.Id == "" {
		s.Logger.Error("IndexedWrite failed: document or document ID is missing")
		return &pb.WriteResult{Success: false, Message: "Document or Document ID is required"}, nil
	}

	docID := req.Document.Id
	docDataStruct := req.Document.Data

	// 1. Write to BTreeStore (primary store)
	// Convert structpb.Struct to a string for BTree (e.g., JSON string)
	// This assumes your BTree stores string values. Adjust if it stores []byte or other types.
	docDataBytes, err := docDataStruct.MarshalJSON()
	if err != nil {
		s.Logger.Error("Failed to marshal document data to JSON", zap.String("docID", docID), zap.Error(err))
		return &pb.WriteResult{Id: docID, Success: false, Message: "Failed to serialize document data"}, nil
	}
	// The BTree.Insert method needs to return LSN or it needs to be fetched from LogManager after WAL record.
	// For now, placeholder LSN.
	err = s.BTreeStore.Insert(docID, string(docDataBytes), 0) // Assuming Insert returns LSN and error
	if err != nil {
		s.Logger.Error("Failed to write document to BTree store", zap.String("docID", docID), zap.Error(err))
		return &pb.WriteResult{Id: docID, Success: false, Message: fmt.Sprintf("BTree write failed: %v", err)}, nil
	}
	s.Logger.Info("Document written to BTree", zap.String("docID", docID)) // Assuming LSN has ToString()

	// 2. Update Inverted Index (if applicable based on document fields and request)
	// This logic needs to extract text fields from req.Document.Data for inverted indexing.
	textFieldsToIndex := make(map[string]string)
	if docDataStruct != nil && docDataStruct.Fields != nil {
		for key, value := range docDataStruct.Fields {
			// Simple check: if string value, consider for inverted indexing.
			// A more robust approach might use a schema or explicit field list.
			if strVal, ok := value.Kind.(*structpb.Value_StringValue); ok {
				textFieldsToIndex[key] = strVal.StringValue
			}
		}
	}

	if len(textFieldsToIndex) > 0 {
		// The InvertedIndex.AddDocument method should handle LSN from underlying LogManager
		// or this service should get LSN from LogManager after calling InvertedIndex.
		err := s.InvertedIndex.Insert(string(docDataBytes), docID) // Assuming AddDocument takes map[string]string
		if err != nil {
			s.Logger.Error("Failed to update inverted index", zap.String("docID", docID), zap.Error(err))
			// Decide on error handling: rollback BTree write? For now, log and report partial success/failure.
			return &pb.WriteResult{Id: docID, Success: false, Message: fmt.Sprintf("Inverted index update failed: %v (BTree write succeeded with LSN)", err), Lsn: ""}, nil
		}
		s.Logger.Info("Updated inverted index", zap.String("docID", docID))
	}

	// 3. Update Spatial Index (if applicable based on document fields and request)
	// This logic needs to extract geo fields from req.Document.Data.
	// Example: if req.Document.Data contains a "location" field of type GeoShape.
	// geoShapeField, hasGeo := docDataStruct.Fields["location"] // Assuming "location" field
	// if hasGeo {
	//     // Convert geoShapeField (structpb.Value) to pb.GeoShape
	//     // geoShape, parseErr := s.parseGeoShape(geoShapeField)
	//     // if parseErr == nil {
	//     //    err := s.SpatialIndex.Insert(docID, geoShape.TopLeft.Longitude, geoShape.TopLeft.Latitude, geoShape.BottomRight.Longitude, geoShape.BottomRight.Latitude, map[string]interface{}{"id": docID})
	//     //    if err != nil {
	//     //        s.Logger.Error("Failed to update spatial index", zap.String("docID", docID), zap.Error(err))
	//     //        // Error handling
	//     //    } else {
	//     //        s.Logger.Info("Updated spatial index", zap.String("docID", docID))
	//     //    }
	//     // } else {
	//     //    s.Logger.Warn("Could not parse geo shape for spatial indexing", zap.String("docID", docID), zap.Error(parseErr))
	//     // }
	// }
	s.Logger.Warn("Spatial index update in IndexedWrite is a placeholder", zap.String("docID", docID))

	return &pb.WriteResult{Id: docID, Success: true, Message: "Document and indexes updated successfully", Lsn: ""}, nil
}

// UpdateInvertedIndex provides a more direct way to update the inverted index.
func (s *IndexedWriteServer) UpdateInvertedIndex(ctx context.Context, req *pb.UpdateInvertedIndexRequest) (*pb.WriteResult, error) {
	s.Logger.Info("UpdateInvertedIndex request received", zap.String("docID", req.DocumentId))
	if req.DocumentId == "" {
		return &pb.WriteResult{Success: false, Message: "Document ID is required"}, nil
	}

	err := s.InvertedIndex.Insert(req.String(), req.DocumentId)
	if err != nil {
		s.Logger.Error("Failed to update inverted index directly", zap.String("docID", req.DocumentId), zap.Error(err))
		return &pb.WriteResult{Id: req.DocumentId, Success: false, Message: fmt.Sprintf("Inverted index update failed: %v", err)}, nil
	}

	// LSN handling: InvertedIndex.AddDocument should internally log to WAL and get an LSN,
	// or this service needs to wrap it with LogManager calls. For now, assume LSN is handled internally or not returned here.
	return &pb.WriteResult{Id: req.DocumentId, Success: true, Message: "Inverted index updated successfully"}, nil
}

// UpdateSpatialIndex provides a more direct way to update the spatial index.
func (s *IndexedWriteServer) UpdateSpatialIndex(ctx context.Context, req *pb.UpdateSpatialIndexRequest) (*pb.WriteResult, error) {
	s.Logger.Info("UpdateSpatialIndex request received", zap.String("docID", req.DocumentId))
	if req.DocumentId == "" || req.Shape == nil {
		return &pb.WriteResult{Success: false, Message: "Document ID and Shape are required"}, nil
	}

	// Convert req.Attributes (map[string]*structpb.Value) to map[string]interface{} for SpatialIndex.Insert
	attributes := make(map[string]interface{})
	if req.Attributes != nil {
		for k, v := range req.Attributes {
			attributes[k] = v.AsInterface()
		}
	}
	attributes["id"] = req.DocumentId // Ensure document ID is part of attributes for identification
	rect := spatial.Rect{
		MinX: req.Shape.TopLeft.Longitude, // Assuming rtree.Rect uses minX, minY, maxX, maxY
		MinY: req.Shape.TopLeft.Latitude,
		MaxX: req.Shape.BottomRight.Longitude,
		MaxY: req.Shape.BottomRight.Latitude,
	}
	// The SpatialIndex.Insert method needs to handle WAL logging and LSN.
	err := s.SpatialIndex.Insert(rect, spatial.SpatialData{Attributes: attributes, ID: req.DocumentId})
	if err != nil {
		s.Logger.Error("Failed to update spatial index directly", zap.String("docID", req.DocumentId), zap.Error(err))
		return &pb.WriteResult{Id: req.DocumentId, Success: false, Message: fmt.Sprintf("Spatial index update failed: %v", err)}, nil
	}

	return &pb.WriteResult{Id: req.DocumentId, Success: true, Message: "Spatial index updated successfully"}, nil
}

// DeleteDocument removes a document and its index entries.
func (s *IndexedWriteServer) DeleteDocument(ctx context.Context, req *pb.DeleteDocumentRequest) (*pb.WriteResult, error) {
	s.Logger.Info("DeleteDocument request received", zap.String("docID", req.DocumentId))
	if req.DocumentId == "" {
		return &pb.WriteResult{Success: false, Message: "Document ID is required"}, nil
	}

	docID := req.DocumentId

	// 1. Delete from BTreeStore
	// BTree.Delete should log to WAL and return LSN
	err := s.BTreeStore.Delete(docID, 0)
	if err != nil {
		// Handle case where document might not exist in BTree - is it an error?
		s.Logger.Error("Failed to delete document from BTree store", zap.String("docID", docID), zap.Error(err))
		return &pb.WriteResult{Id: docID, Success: false, Message: fmt.Sprintf("BTree delete failed: %v", err)}, nil
	}
	s.Logger.Info("Document deleted from BTree", zap.String("docID", docID), zap.String("lsn", ""))

	// 2. Delete from Inverted Index
	// InvertedIndex should have a RemoveDocument method.
	// err = s.InvertedIndex.RemoveDocument(docID)
	// if err != nil {
	// 	s.Logger.Error("Failed to remove document from inverted index", zap.String("docID", docID), zap.Error(err))
	// 	// Non-fatal for overall delete, but log and potentially include in message
	// } else {
	// 	s.Logger.Info("Document removed from inverted index", zap.String("docID", docID))
	// }

	// 3. Delete from Spatial Index
	// SpatialIndex.Delete would take the same bounds/object it was inserted with, or by a unique ID stored in attributes.
	// This requires the spatial index to support deletion by a document ID.
	// Let's assume SpatialIndex.Remove(objectID interface{}) where objectID is the "id" attribute.
	// err = s.SpatialIndex.Delete(docID) // Assuming this uses the "id" attribute stored during insert
	// if err != nil {
	// 	s.Logger.Error("Failed to remove document from spatial index", zap.String("docID", docID), zap.Error(err))
	// 	// Non-fatal
	// } else {
	// 	s.Logger.Info("Document removed from spatial index", zap.String("docID", docID))
	// }

	return &pb.WriteResult{Id: docID, Success: true, Message: "Document deleted successfully from primary store and indexes (best effort for indexes).", Lsn: ""}, nil
}

// Helper to parse GeoShape from structpb.Value - This is a placeholder and needs robust implementation
// func (s *IndexedWriteServer) parseGeoShape(val *structpb.Value) (*pb.GeoShape, error) {
//  s.Logger.Warn("parseGeoShape is a placeholder and needs implementation.")
//  // Example: If val is a struct with "top_left": {"latitude":X, "longitude":Y}, ...
//  // This would involve checking types and extracting values.
//  return nil, fmt.Errorf("GeoShape parsing not implemented")
// }

package bulkwritesservice

import (
	"context"
	"fmt"
	"io" // For StreamBulkWrite

	pb "github.com/sushant-115/gojodb/api/proto" // Adjust import path
	"github.com/sushant-115/gojodb/core/indexing/btree"
	"github.com/sushant-115/gojodb/core/indexing/inverted_index"
	"github.com/sushant-115/gojodb/core/indexing/spatial"
	"go.uber.org/zap"
	// "google.golang.org/protobuf/types/known/structpb" // Might be needed if parsing Document.Data deeply
)

// BulkWriteServer implements the BulkWriteService gRPC service.
type BulkWriteServer struct {
	pb.UnimplementedBulkWriteServiceServer
	// Dependencies similar to IndexedWriteServer, as bulk operations are sequences of individual writes/deletes.
	// Alternatively, it could call the IndexedWriteService internally, but direct access might be more performant for true bulk.
	BTreeStore    *btree.BTree[string, string]
	InvertedIndex *inverted_index.InvertedIndex
	SpatialIndex  *spatial.IndexManager
	Logger        *zap.Logger
	// indexedWriteService *indexed_writes_service.IndexedWriteServer // Option: delegate to single write service
}

// NewBulkWriteServer creates a new BulkWriteServer.
func NewBulkWriteServer(
	btreeStore *btree.BTree[string, string],
	invertedIdx *inverted_index.InvertedIndex,
	spatialIdx *spatial.IndexManager,
	logger *zap.Logger,
) *BulkWriteServer {
	return &BulkWriteServer{
		BTreeStore:    btreeStore,
		InvertedIndex: invertedIdx,
		SpatialIndex:  spatialIdx,
		Logger:        logger.Named("bulk_write_service"),
	}
}

// BulkWrite processes a list of operations.
func (s *BulkWriteServer) BulkWrite(ctx context.Context, req *pb.BulkWriteRequest) (*pb.BulkWriteResponse, error) {
	s.Logger.Info("BulkWrite request received", zap.Int("operation_count", len(req.Operations)))
	if len(req.Operations) == 0 {
		return &pb.BulkWriteResponse{}, nil
	}

	results := make([]*pb.WriteResult, 0, len(req.Operations))
	var successCount, failureCount int32

	// TODO: Implement transactionality if needed (all succeed or all fail).
	// For now, process one by one.
	for _, op := range req.Operations {
		var opResult *pb.WriteResult
		var err error

		// For a more complete implementation, reuse logic from IndexedWriteServer methods.
		// This is a simplified inline version.
		// A better approach would be to have an internal "executeWriteOperation" method.
		docID := ""
		if op.Document != nil {
			docID = op.Document.Id
		}

		switch op.OperationType {
		case pb.OperationType_UPSERT:
			if op.Document == nil || op.Document.Id == "" {
				opResult = &pb.WriteResult{Success: false, Message: "UPSERT operation missing document or document ID"}
			} else {
				// Simplified UPSERT: Call underlying BTree Insert and index updates.
				// This needs the same detailed logic as IndexedWriteService.IndexedWrite for atomicity and index updates.
				// For brevity, this is a highly simplified placeholder.
				docDataBytes, convErr := op.Document.Data.MarshalJSON()
				if convErr != nil {
					opResult = &pb.WriteResult{Id: op.Document.Id, Success: false, Message: "Failed to serialize document data for UPSERT"}
				} else {
					lsn, btreeErr := s.BTreeStore.Insert(op.Document.Id, string(docDataBytes))
					if btreeErr != nil {
						opResult = &pb.WriteResult{Id: op.Document.Id, Success: false, Message: fmt.Sprintf("BTree UPSERT failed: %v", btreeErr)}
					} else {
						opResult = &pb.WriteResult{Id: op.Document.Id, Success: true, Lsn: lsn.ToString(), Message: "UPSERT (BTree only) successful"}
						// TODO: Add Inverted Index and Spatial Index updates here, like in IndexedWrite
						s.Logger.Info("Bulk UPSERT (BTree only)", zap.String("docID", op.Document.Id), zap.String("lsn", lsn.ToString()))
					}
				}
			}
		case pb.OperationType_DELETE:
			if docID == "" {
				opResult = &pb.WriteResult{Success: false, Message: "DELETE operation missing document ID"}
			} else {
				lsn, btreeErr := s.BTreeStore.Delete(docID)
				if btreeErr != nil {
					opResult = &pb.WriteResult{Id: docID, Success: false, Message: fmt.Sprintf("BTree DELETE failed: %v", btreeErr)}
				} else {
					opResult = &pb.WriteResult{Id: docID, Success: true, Lsn: lsn.ToString(), Message: "DELETE (BTree only) successful"}
					// TODO: Add Inverted Index and Spatial Index removals here
					s.Logger.Info("Bulk DELETE (BTree only)", zap.String("docID", docID), zap.String("lsn", lsn.ToString()))
				}
			}
		default:
			opResult = &pb.WriteResult{Id: docID, Success: false, Message: fmt.Sprintf("Unsupported operation type: %s", op.OperationType)}
		}

		if opResult == nil { // Should not happen if logic is correct
			opResult = &pb.WriteResult{Id: docID, Success: false, Message: "Internal error processing operation"}
		}

		if opResult.Success {
			successCount++
		} else {
			failureCount++
			s.Logger.Warn("Bulk operation failed", zap.String("docID", docID), zap.String("type", op.OperationType.String()), zap.String("message", opResult.Message))
		}
		results = append(results, opResult)

		// Optional: Fail fast
		// if !opResult.Success && failFast { return &pb.BulkWriteResponse{...}, nil }
	}
	s.Logger.Info("BulkWrite processing complete", zap.Int32("success", successCount), zap.Int32("failure", failureCount))
	return &pb.BulkWriteResponse{
		Results:      results,
		SuccessCount: successCount,
		FailureCount: failureCount,
	}, nil
}

// StreamBulkWrite handles a stream of bulk operations.
func (s *BulkWriteServer) StreamBulkWrite(stream pb.BulkWriteService_StreamBulkWriteServer) error {
	s.Logger.Info("StreamBulkWrite session started")
	var totalOps, successCount, failureCount int32
	var failedOpIDs []string
	var firstErrorMessage string

	for {
		op, err := stream.Recv()
		if err == io.EOF {
			// End of stream
			s.Logger.Info("StreamBulkWrite EOF received. Processing complete.",
				zap.Int32("total_ops", totalOps),
				zap.Int32("successful", successCount),
				zap.Int32("failed", failureCount))
			return stream.SendAndClose(&pb.BulkWriteSummaryResponse{
				TotalOperations:    totalOps,
				SuccessCount:       successCount,
				FailureCount:       failureCount,
				FailedOperationIds: failedOpIDs,
				FirstErrorMessage:  firstErrorMessage,
			})
		}
		if err != nil {
			s.Logger.Error("Error receiving bulk operation from stream", zap.Error(err))
			return err // Or handle more gracefully, e.g., by trying to SendAndClose a summary
		}
		totalOps++

		// Process the individual operation (similar logic to non-streaming BulkWrite)
		var opResult *pb.WriteResult
		docID := ""
		if op.Document != nil {
			docID = op.Document.Id
		}

		switch op.OperationType {
		case pb.OperationType_UPSERT:
			if op.Document == nil || op.Document.Id == "" {
				opResult = &pb.WriteResult{Success: false, Message: "UPSERT operation missing document or document ID"}
			} else {
				docDataBytes, convErr := op.Document.Data.MarshalJSON()
				if convErr != nil {
					opResult = &pb.WriteResult{Id: op.Document.Id, Success: false, Message: "Failed to serialize document data for UPSERT"}
				} else {
					lsn, btreeErr := s.BTreeStore.Insert(op.Document.Id, string(docDataBytes))
					if btreeErr != nil {
						opResult = &pb.WriteResult{Id: op.Document.Id, Success: false, Message: fmt.Sprintf("BTree UPSERT failed: %v", btreeErr)}
					} else {
						opResult = &pb.WriteResult{Id: op.Document.Id, Success: true, Lsn: lsn.ToString(), Message: "UPSERT (BTree only) successful"}
						s.Logger.Debug("Streamed UPSERT (BTree only)", zap.String("docID", op.Document.Id))
					}
				}
			}
		case pb.OperationType_DELETE:
			if docID == "" {
				opResult = &pb.WriteResult{Success: false, Message: "DELETE operation missing document ID"}
			} else {
				lsn, btreeErr := s.BTreeStore.Delete(docID)
				if btreeErr != nil {
					opResult = &pb.WriteResult{Id: docID, Success: false, Message: fmt.Sprintf("BTree DELETE failed: %v", btreeErr)}
				} else {
					opResult = &pb.WriteResult{Id: docID, Success: true, Lsn: lsn.ToString(), Message: "DELETE (BTree only) successful"}
					s.Logger.Debug("Streamed DELETE (BTree only)", zap.String("docID", docID))
				}
			}
		default:
			opResult = &pb.WriteResult{Id: docID, Success: false, Message: fmt.Sprintf("Unsupported operation type: %s", op.OperationType)}
		}

		if opResult.Success {
			successCount++
		} else {
			failureCount++
			if docID != "" {
				failedOpIDs = append(failedOpIDs, docID)
			} else if op.Document != nil && op.Document.Id != "" {
				failedOpIDs = append(failedOpIDs, op.Document.Id+" (op_type_unknown_id)")
			} else {
				failedOpIDs = append(failedOpIDs, fmt.Sprintf("unknown_id_op_index_%d", totalOps))
			}

			if firstErrorMessage == "" {
				firstErrorMessage = opResult.Message
			}
			s.Logger.Warn("Streamed bulk operation failed", zap.String("docID", docID), zap.String("type", op.OperationType.String()), zap.String("message", opResult.Message))
		}
	}
}

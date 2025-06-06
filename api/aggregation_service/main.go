package aggregationservice

// import (
// 	"context"
// 	"fmt"

// 	// "sort" // May be needed for implementing $sort or other stages

// 	"github.com/sushant-115/gojodb/core/indexing/btree"
// 	"github.com/sushant-115/gojodb/core/indexing/inverted_index"

// 	// "github.com/sushant-115/gojodb/core/indexing/spatial" // May be needed if aggregations involve spatial data
// 	pb "github.com/sushant-115/gojodb/api/proto" // Adjust import path
// 	"go.uber.org/zap"
// 	"google.golang.org/protobuf/types/known/structpb"
// )

// // AggregationServer implements the AggregationService gRPC service.
// type AggregationServer struct {
// 	pb.UnimplementedGatewayServiceServer
// 	BTreeStore    *btree.BTree[string, string]  // Primary data source
// 	InvertedIndex *inverted_index.InvertedIndex // For $match stages using text search
// 	Logger        *zap.Logger
// }

// // NewAggregationServer creates a new AggregationServer.
// func NewAggregationServer(
// 	btreeStore *btree.BTree[string, string],
// 	invertedIdx *inverted_index.InvertedIndex,
// 	logger *zap.Logger,
// ) *AggregationServer {
// 	return &AggregationServer{
// 		BTreeStore:    btreeStore,
// 		InvertedIndex: invertedIdx,
// 		Logger:        logger.Named("aggregation_service"),
// 	}
// }

// // Aggregate performs an aggregation query based on a pipeline of stages.
// func (s *AggregationServer) Aggregate(ctx context.Context, req *pb.GetRangeRequest) (*pb.AggregationResponse, error) {
// 	s.Logger.Info("Aggregate request received", zap.String("collection", req.CollectionName), zap.Int("num_stages", len(req.Stages)))

// 	if req.CollectionName == "" { // Assuming aggregations target named collections/tables
// 		// For now, GojoDB seems to be a key-value store with auxiliary indexes.
// 		// Aggregations usually operate on a collection of documents.
// 		// We might need to define how "collections" map to BTree data or use a specific prefix.
// 		// For this example, let's assume all documents in BTree are part of a default collection.
// 		s.Logger.Warn("CollectionName is empty; aggregation will conceptually process all documents in BTree.")
// 	}
// 	if len(req.Stages) == 0 {
// 		return &pb.AggregationResponse{Success: false, Message: "Aggregation pipeline requires at least one stage"}, nil
// 	}

// 	// Initial dataset: all documents from BTree (this is inefficient for large datasets without a $match first)
// 	// In a real system, the first stage (e.g., $match) would optimize data loading.
// 	var currentResults []*structpb.Struct

// 	// Placeholder: Iterating BTree. This is NOT scalable for aggregation.
// 	// Aggregations typically need efficient ways to scan and filter data.
// 	// This part needs significant optimization, possibly by pushing down some operations to storage/index layers.
// 	// For now, a conceptual full scan if no initial $match leveraging an index.
// 	s.Logger.Warn("Aggregation pipeline is starting with a conceptual full BTree scan. This is inefficient and needs optimization (e.g., index-backed $match).")

// 	// Conceptual: Load all data if no initial filtering through an index.
// 	// This is a MAJOR simplification. Real aggregations are much more complex.
// 	// allKeys := s.BTreeStore.AllKeys() // Assuming BTree can provide all keys
// 	// for _, key := range allKeys {
// 	//  valStr, found := s.BTreeStore.Search(key)
// 	//  if found {
// 	//      docData := &structpb.Struct{}
// 	//      if err := json.Unmarshal([]byte(valStr), docData); err == nil {
// 	//          // Attach ID to the document for processing if needed
// 	//          if docData.Fields == nil { docData.Fields = make(map[string]*structpb.Value) }
// 	//          docData.Fields["_id"] = structpb.NewStringValue(key)
// 	//          currentResults = append(currentResults, docData)
// 	//      }
// 	//  }
// 	// }
// 	// Due to BTree not having AllKeys, this conceptual full scan is hard to implement simply here.
// 	// We will proceed assuming `currentResults` is populated by an initial stage or a limited scan.
// 	// For a true PoC, one might iterate a few known keys or require an initial $match.
// 	// Let's assume for now that the pipeline starts with an empty dataset or gets it from an efficient first stage.

// 	// Process each stage in the pipeline
// 	var err error
// 	for i, stage := range req.Stages {
// 		s.Logger.Debug("Processing aggregation stage", zap.Int("stage_index", i), zap.Any("stage_type", stage.GetStageType()))
// 		switch s := stage.StageType.(type) {
// 		case *pb.AggregationPipelineStage_Match:
// 			currentResults, err = s.applyMatchStage(currentResults, s.Match)
// 		case *pb.AggregationPipelineStage_Group:
// 			currentResults, err = s.applyGroupStage(currentResults, s.Group)
// 		case *pb.AggregationPipelineStage_Sort:
// 			currentResults, err = s.applySortStage(currentResults, s.Sort)
// 		case *pb.AggregationPipelineStage_Project:
// 			currentResults, err = s.applyProjectStage(currentResults, s.Project)
// 		case *pb.AggregationPipelineStage_Limit:
// 			currentResults, err = s.applyLimitStage(currentResults, s.Limit)
// 		case *pb.AggregationPipelineStage_Skip:
// 			currentResults, err = s.applySkipStage(currentResults, s.Skip)
// 		case *pb.AggregationPipelineStage_Count:
// 			currentResults, err = s.applyCountStage(currentResults, s.Count)
// 		default:
// 			err = fmt.Errorf("unsupported aggregation stage type: %T", s)
// 		}
// 		if err != nil {
// 			s.Logger.Error("Error applying aggregation stage", zap.Error(err), zap.Int("stage_index", i))
// 			return &pb.AggregationResponse{Success: false, Message: fmt.Sprintf("Error in stage %d: %v", i, err)}, nil
// 		}
// 		if currentResults == nil { // Can happen if a stage results in no documents
// 			currentResults = []*structpb.Struct{}
// 		}
// 		s.Logger.Debug("Stage processed", zap.Int("stage_index", i), zap.Int("documents_after_stage", len(currentResults)))
// 	}

// 	return &pb.AggregationResponse{Results: currentResults, Success: true}, nil
// }

// // --- Placeholder implementations for aggregation stages ---
// // These are highly simplified and need to be made robust and performant.

// func (s *AggregationServer) applyMatchStage(input []*structpb.Struct, stage *pb.MatchStage) ([]*structpb.Struct, error) {
// 	s.Logger.Debug("Applying $match stage", zap.Any("filter", stage.Filter))
// 	if len(input) == 0 && stage.Filter == nil { // If input is already empty and no filter, nothing to do
// 		return input, nil
// 	}
// 	if stage.Filter == nil || len(stage.Filter) == 0 { // No filter, pass all input
// 		return input, nil
// 	}

// 	// This is a very basic match. A real $match needs to handle various operators ($eq, $gt, $lt, $in, $and, $or, etc.)
// 	// and potentially leverage indexes (e.g., if matching on an indexed field).
// 	// If input is empty and this is the first stage, this is where inverted index search could happen for text fields.
// 	// For now, simple equality check on existing `input` documents.

// 	var output []*structpb.Struct
// 	for _, doc := range input {
// 		match := true
// 		for filterKey, filterValue := range stage.Filter {
// 			docValue, ok := doc.Fields[filterKey]
// 			if !ok || !protoValuesEqual(docValue, filterValue) { // protoValuesEqual needs to be implemented
// 				match = false
// 				break
// 			}
// 		}
// 		if match {
// 			output = append(output, doc)
// 		}
// 	}
// 	s.Logger.Warn("$match stage is using a very simplified equality filter. Advanced operators and index usage are not implemented.")
// 	return output, nil
// }

// // protoValuesEqual compares two structpb.Value instances. Needs careful implementation for all types.
// func protoValuesEqual(v1, v2 *structpb.Value) bool {
// 	if v1 == nil && v2 == nil {
// 		return true
// 	}
// 	if v1 == nil || v2 == nil {
// 		return false
// 	}

// 	switch k1 := v1.Kind.(type) {
// 	case *structpb.Value_NullValue:
// 		_, ok := v2.Kind.(*structpb.Value_NullValue)
// 		return ok
// 	case *structpb.Value_NumberValue:
// 		k2, ok := v2.Kind.(*structpb.Value_NumberValue)
// 		return ok && k1.NumberValue == k2.NumberValue
// 	case *structpb.Value_StringValue:
// 		k2, ok := v2.Kind.(*structpb.Value_StringValue)
// 		return ok && k1.StringValue == k2.StringValue
// 	case *structpb.Value_BoolValue:
// 		k2, ok := v2.Kind.(*structpb.Value_BoolValue)
// 		return ok && k1.BoolValue == k2.BoolValue
// 	// TODO: Implement for StructValue and ListValue (recursive comparison)
// 	default:
// 		return false // Unsupported type for simple equality
// 	}
// }

// func (s *AggregationServer) applyGroupStage(input []*structpb.Struct, stage *pb.GroupStage) ([]*structpb.Struct, error) {
// 	s.Logger.Debug("Applying $group stage", zap.String("groupBy", stage.GroupByField), zap.Any("accumulators", stage.Accumulators))
// 	s.Logger.Warn("$group stage is a placeholder and not fully implemented. It requires complex grouping and accumulation logic.")
// 	// This is a complex stage. Requires:
// 	// 1. Grouping documents by stage.GroupByField.
// 	// 2. For each group, applying accumulators ($sum, $avg, $count, etc.).
// 	// Example: if stage.GroupByField is "$category", all docs with same category value are grouped.
// 	// Then for each group, if an accumulator is {"totalSales": {type: SUM, field: "$price"}}, sum prices in that group.
// 	// The output documents would have fields for the group key and each accumulator result.

// 	// Highly simplified: If no group-by field, treat as single group for global accumulation (e.g. total count)
// 	if stage.GroupByField == "" && len(stage.Accumulators) > 0 {
// 		// Example: just counting all input documents if an accumulator is $count
// 		for outField, acc := range stage.Accumulators {
// 			if acc.Type == pb.Accumulator_COUNT {
// 				countResult, _ := structpb.NewStruct(map[string]interface{}{
// 					outField: float64(len(input)),
// 				})
// 				return []*structpb.Struct{countResult}, nil
// 			}
// 		}
// 	}

// 	return input, fmt.Errorf("$group stage not fully implemented") // Return input as is, or error
// }

// func (s *AggregationServer) applySortStage(input []*structpb.Struct, stage *pb.SortStage) ([]*structpb.Struct, error) {
// 	s.Logger.Debug("Applying $sort stage", zap.Any("fields", stage.Fields))
// 	s.Logger.Warn("$sort stage is a placeholder and not fully implemented. Requires multi-field sorting logic.")
// 	// Needs to sort `input` documents based on multiple fields and directions.
// 	// `sort.Slice` from Go standard library can be used with a custom comparison function.
// 	return input, fmt.Errorf("$sort stage not fully implemented")
// }

// func (s *AggregationServer) applyProjectStage(input []*structpb.Struct, stage *pb.ProjectStage) ([]*structpb.Struct, error) {
// 	s.Logger.Debug("Applying $project stage", zap.Any("fields", stage.Fields))
// 	s.Logger.Warn("$project stage is a placeholder. Requires field selection/exclusion/renaming logic.")
// 	// For each document in `input`, create a new document including/excluding/renaming fields as per stage.Fields.
// 	// MongoDB's $project is quite powerful. This would be a simplified version.
// 	var output []*structpb.Struct
// 	for _, doc := range input {
// 		newDocFields := make(map[string]*structpb.Value)
// 		hasExplicitIDRule := false
// 		idIncluded := true // _id is included by default

// 		if stage.Fields != nil {
// 			for fieldName, rule := range stage.Fields {
// 				if fieldName == "_id" {
// 					hasExplicitIDRule = true
// 					if rule == 0 {
// 						idIncluded = false
// 					}
// 				}
// 				if rule == 1 { // Include
// 					if val, ok := doc.Fields[fieldName]; ok {
// 						newDocFields[fieldName] = val
// 					}
// 				}
// 				// Rule == 0 (exclude) is handled by not adding, except for _id
// 			}
// 		}
// 		// Handle _id default inclusion / explicit exclusion
// 		if idIncluded || (hasExplicitIDRule && stage.Fields["_id"] == 1) {
// 			if idVal, ok := doc.Fields["_id"]; ok {
// 				newDocFields["_id"] = idVal
// 			} else {
// 				// If original doc didn't have _id but it's implicitly/explicitly included, this logic might need refinement
// 				// or assume _id always exists if being projected.
// 			}
// 		}
// 		if !idIncluded && hasExplicitIDRule && stage.Fields["_id"] == 0 {
// 			delete(newDocFields, "_id")
// 		}

// 		output = append(output, &structpb.Struct{Fields: newDocFields})
// 	}
// 	return output, nil
// }

// func (s *AggregationServer) applyLimitStage(input []*structpb.Struct, stage *pb.LimitStage) ([]*structpb.Struct, error) {
// 	s.Logger.Debug("Applying $limit stage", zap.Int32("count", stage.Count))
// 	if stage.Count <= 0 {
// 		return input, fmt.Errorf("limit count must be positive")
// 	}
// 	if int(stage.Count) < len(input) {
// 		return input[:stage.Count], nil
// 	}
// 	return input, nil
// }

// func (s *AggregationServer) applySkipStage(input []*structpb.Struct, stage *pb.SkipStage) ([]*structpb.Struct, error) {
// 	s.Logger.Debug("Applying $skip stage", zap.Int32("count", stage.Count))
// 	if stage.Count < 0 { // Should not happen with proto validation, but good to check
// 		return input, fmt.Errorf("skip count cannot be negative")
// 	}
// 	if stage.Count == 0 {
// 		return input, nil
// 	}
// 	if int(stage.Count) < len(input) {
// 		return input[stage.Count:], nil
// 	}
// 	return []*structpb.Struct{}, nil // Skip all
// }

// func (s *AggregationServer) applyCountStage(input []*structpb.Struct, stage *pb.CountStage) ([]*structpb.Struct, error) {
// 	s.Logger.Debug("Applying $count stage", zap.String("outputField", stage.OutputFieldName))
// 	if stage.OutputFieldName == "" {
// 		return nil, fmt.Errorf("count stage requires an output field name")
// 	}
// 	countResult, _ := structpb.NewStruct(map[string]interface{}{
// 		stage.OutputFieldName: float64(len(input)), // structpb.Value represents numbers as float64
// 	})
// 	return []*structpb.Struct{countResult}, nil
// }

package indexedreadsservice

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/sushant-115/gojodb/api/proto"
	"github.com/sushant-115/gojodb/core/indexmanager"
	internaltelemetry "github.com/sushant-115/gojodb/internal/telemetry"
	"github.com/sushant-115/gojodb/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	serviceName = "gojodb_server"
)

// IndexedReadService implements the gRPC IndexedReadService.
type IndexedReadService struct {
	pb.UnimplementedIndexedReadServiceServer
	nodeID        string
	slotID        uint32
	indexManagers map[string]indexmanager.IndexManager
	tracer        trace.Tracer
	metrics       *internaltelemetry.GrpcGatewayMetrics
}

// NewIndexedReadService creates a new IndexedReadService.
func NewIndexedReadService(nodeID string, slotID uint32, indexManagers map[string]indexmanager.IndexManager, tel *telemetry.Telemetry) *IndexedReadService {
	grpcMetrics, err := internaltelemetry.NewGrpcGatewayMetrics(tel.Meter)
	if err != nil {
		fmt.Printf("failed to create gRPC metrics: %w", err)
	}
	return &IndexedReadService{
		nodeID:        nodeID,
		slotID:        slotID,
		indexManagers: indexManagers,
		tracer:        tel.Tracer,
		metrics:       grpcMetrics,
	}
}

// Get handles a single key-value get operation.
func (s *IndexedReadService) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	metricCtx, span, startTime := s.StartMetricsAndTrace(ctx, "Get")
	var statusCode otelcodes.Code = otelcodes.Ok
	defer func() {
		s.EndMetricsAndTrace(metricCtx, span, startTime, "Get", statusCode)
	}()

	// Always get from the primary K/V store (B-tree)
	val, found := s.indexManagers["btree"].Get(metricCtx, req.Key)
	log.Printf("Node %s, Slot %d: Get key=%s, found=%t", s.nodeID, s.slotID, req.Key, found)
	return &pb.GetResponse{Value: val, Found: found}, nil
}

// GetRange handles a range query.
func (s *IndexedReadService) GetRange(ctx context.Context, req *pb.GetRangeRequest) (*pb.GetRangeResponse, error) {
	metricCtx, span, startTime := s.StartMetricsAndTrace(ctx, "GetRange")
	var statusCode otelcodes.Code = otelcodes.Ok
	defer func() {
		s.EndMetricsAndTrace(metricCtx, span, startTime, "GetRange", statusCode)
	}()
	// Always get from the primary K/V store (B-tree)
	entries, err := s.indexManagers["btree"].GetRange(metricCtx, req.StartKey, req.EndKey, req.Limit)
	if err != nil {
		statusCode = otelcodes.Error
		log.Printf("Node %s, Slot %d: Failed to get range for start_key %s: %v", s.nodeID, s.slotID, req.StartKey, err)
		return nil, status.Errorf(codes.Internal, "get range failed: %v", err)
	}
	log.Printf("Node %s, Slot %d: GetRange start=%s, end=%s, limit=%d, returned %d entries", s.nodeID, s.slotID, req.StartKey, req.EndKey, req.Limit, len(entries))
	return &pb.GetRangeResponse{Entries: entries}, nil
}

// TextSearch handles a text search query.
func (s *IndexedReadService) TextSearch(ctx context.Context, req *pb.TextSearchRequest) (*pb.TextSearchResponse, error) {
	metricCtx, span, startTime := s.StartMetricsAndTrace(ctx, "TextSearch")
	var statusCode otelcodes.Code = otelcodes.Ok
	defer func() {
		s.EndMetricsAndTrace(metricCtx, span, startTime, "TextSearch", statusCode)
	}()
	// Delegate to the inverted index manager
	if invIdxMgr, ok := s.indexManagers["inverted"].(*indexmanager.InvertedIndexManager); ok {
		results, err := invIdxMgr.TextSearch(metricCtx, req.Query, req.IndexName, req.Limit)
		if err != nil {
			statusCode = otelcodes.Error
			log.Printf("Node %s, Slot %d: Failed text search for query %s: %v", s.nodeID, s.slotID, req.Query, err)
			return nil, status.Errorf(codes.Internal, "text search failed: %v", err)
		}
		log.Printf("Node %s, Slot %d: TextSearch query=%s, returned %d results", s.nodeID, s.slotID, req.Query, len(results))
		return &pb.TextSearchResponse{Results: results}, nil
	}
	statusCode = otelcodes.Error
	return nil, status.Errorf(codes.Unimplemented, "TextSearch not implemented or inverted index manager not found")
}

// StartMetricsAndTrace begins the telemetry recording for a gRPC method.
// It returns a new context, the trace span, and the start time.
func (s *IndexedReadService) StartMetricsAndTrace(ctx context.Context, fullMethodName string) (context.Context, trace.Span, time.Time) {
	startTime := time.Now()

	// Increment active RPCs and started counter
	s.metrics.ActiveRpcsUpDownCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("grpc.service", serviceName),
		attribute.String("grpc.method", fullMethodName),
	))
	s.metrics.RpcsStartedCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("grpc.service", serviceName),
		attribute.String("grpc.method", fullMethodName),
	))

	// Start a new trace span
	ctx, span := s.tracer.Start(ctx, fullMethodName, trace.WithAttributes(
		attribute.String("grpc.service", serviceName),
		attribute.String("grpc.method", fullMethodName),
	))

	return ctx, span, startTime
}

// EndMetricsAndTrace completes the telemetry recording for a gRPC method.
func (s *IndexedReadService) EndMetricsAndTrace(ctx context.Context, span trace.Span, startTime time.Time, fullMethodName string, statusCode otelcodes.Code) {
	latency := time.Since(startTime).Milliseconds()

	// Set span status based on the final gRPC code
	if statusCode != otelcodes.Ok {
		span.SetStatus(otelcodes.Error, statusCode.String())
	} else {
		span.SetStatus(otelcodes.Ok, "Success")
	}
	span.End()

	// Decrement active RPCs
	s.metrics.ActiveRpcsUpDownCounter.Add(ctx, -1, metric.WithAttributes(
		attribute.String("grpc.service", serviceName),
		attribute.String("grpc.method", fullMethodName),
	))

	// Define attributes for the completed RPC.
	metricAttributes := attribute.NewSet(
		attribute.String("grpc.service", serviceName),
		attribute.String("grpc.method", fullMethodName),
		attribute.String("grpc.code", statusCode.String()),
	)

	// Record latency and increment handled counter
	s.metrics.RpcLatencyHistogram.Record(ctx, latency, metric.WithAttributeSet(metricAttributes))
	s.metrics.RpcsHandledCounter.Add(ctx, 1, metric.WithAttributeSet(metricAttributes))
}

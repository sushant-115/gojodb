package indexmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/sushant-115/gojodb/api/proto"
	"github.com/sushant-115/gojodb/core/indexing/inverted_index"
	"github.com/sushant-115/gojodb/core/indexing/spatial"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
	internaltelemetry "github.com/sushant-115/gojodb/internal/telemetry"
	"github.com/sushant-115/gojodb/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// ===============================================
// InvertedIndexManager: Implementation for Inverted Index
// ===============================================

type InvertedIndexManager struct {
	mu        sync.RWMutex
	index     *inverted_index.InvertedIndex
	latestLSN uint64
	// For snapshotting (in-memory dummy)
	snapshotData map[string]map[string][]string // snapshotID -> map[term]list[docID]
	tracer       trace.Tracer
	metrics      *internaltelemetry.GrpcGatewayMetrics
	serviceName  string
}

func NewInvertedIndexManager(index *inverted_index.InvertedIndex, tel *telemetry.Telemetry) *InvertedIndexManager {
	grpcMetrics, err := internaltelemetry.NewGrpcGatewayMetrics(tel.Meter)
	if err != nil {
		fmt.Printf("failed to create gRPC metrics: %w", err)
	}
	return &InvertedIndexManager{
		index:        index,
		snapshotData: make(map[string]map[string][]string),
		tracer:       tel.Tracer,
		metrics:      grpcMetrics,
		serviceName:  "inverted_indexmanager",
	}
}

func (m *InvertedIndexManager) Name() string { return "inverted" }

// Put is not directly supported for InvertedIndexManager (use AddDocument).
func (m *InvertedIndexManager) Put(ctx context.Context, key string, value []byte) error {
	return fmt.Errorf("Put not supported on InvertedIndexManager; use AddDocument")
}

// Get is not directly supported for InvertedIndexManager.
func (m *InvertedIndexManager) Get(ctx context.Context, key string) ([]byte, bool) {
	return nil, false // Inverted index doesn't store direct key-value
}

// Delete is not directly supported for InvertedIndexManager (needs doc ID).
func (m *InvertedIndexManager) Delete(ctx context.Context, key string) error {
	// Assuming a DeleteDocument method exists
	// return m.index.DeleteDocument(key)
	return fmt.Errorf("Delete not supported on InvertedIndexManager; use DeleteDocument(docID)")
}

func (m *InvertedIndexManager) GetRange(ctx context.Context, startKey, endKey string, limit int32) ([]*pb.KeyValuePair, error) {
	return nil, fmt.Errorf("GetRange not supported on InvertedIndexManager")
}

func (m *InvertedIndexManager) TextSearch(ctx context.Context, query, indexName string, limit int32) ([]*pb.TextSearchResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	docIDs, err := m.index.Search(query)
	var results []*pb.TextSearchResult
	for _, docID := range docIDs {
		results = append(results, &pb.TextSearchResult{Key: docID, Score: 1.0}) // Dummy score
		if limit > 0 && len(results) >= int(limit) {
			break
		}
	}
	return results, err
}

func (m *InvertedIndexManager) AddDocument(ctx context.Context, docID string, content string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.index.Insert(content, docID)
	if err == nil {
		m.latestLSN++
	}
	return err
}

// InsertSpatial is not applicable for inverted index.
func (m *InvertedIndexManager) InsertSpatial(ctx context.Context, rect spatial.Rect, dataID string) error {
	return fmt.Errorf("InsertSpatial not supported on InvertedIndexManager")
}

// DeleteSpatial is not applicable for inverted index.
func (m *InvertedIndexManager) DeleteSpatial(ctx context.Context, dataID string) error {
	return fmt.Errorf("DeleteSpatial not supported on InvertedIndexManager")
}

// PrepareSnapshot for InvertedIndexManager (dummy in-memory serialization).
func (m *InvertedIndexManager) PrepareSnapshot(ctx context.Context) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// Assuming InvertedIndex has a method to get its internal state (e.g., term -> []docID mapping)
	// indexState := m.index.GetIndexState() // Dummy method call
	// snapshotBytes, err := json.Marshal(struct {
	// 	Data map[string][]string `json:"data"` // Simplified representation of inverted index state
	// 	LSN  uint64              `json:"lsn"`
	// }{
	// 	Data: indexState,
	// 	LSN:  m.latestLSN,
	// })
	// if err != nil {
	// 	return "", fmt.Errorf("failed to marshal inverted index snapshot: %v", err)
	// }
	snapshotID := fmt.Sprintf("inverted-snapshot-%d", time.Now().UnixNano())
	// m.snapshotData[snapshotID] = indexState // Store simplified state
	log.Printf("InvertedIndexManager: Prepared snapshot %s with LSN %d", snapshotID, m.latestLSN)
	return snapshotID, nil
}

// StreamSnapshot for InvertedIndexManager.
func (m *InvertedIndexManager) StreamSnapshot(ctx context.Context, snapshotID string, chunkChan chan []byte) error {
	defer close(chunkChan)
	m.mu.RLock()
	indexState, ok := m.snapshotData[snapshotID]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("inverted index snapshot ID %s not found", snapshotID)
	}

	snapshotBytes, err := json.Marshal(indexState) // Re-marshal for streaming
	if err != nil {
		return fmt.Errorf("failed to marshal inverted index snapshot for streaming: %v", err)
	}

	chunkSize := 4 * 1024 // 4KB chunks
	for i := 0; i < len(snapshotBytes); i += chunkSize {
		end := i + chunkSize
		if end > len(snapshotBytes) {
			end = len(snapshotBytes)
		}
		chunk := snapshotBytes[i:end]
		select {
		case chunkChan <- chunk:
			// Chunk sent
		case <-time.After(5 * time.Second):
			return fmt.Errorf("timeout sending inverted index snapshot chunk")
		}
	}
	log.Printf("InvertedIndexManager: Streamed snapshot %s successfully", snapshotID)
	return nil
}

// ApplySnapshot for InvertedIndexManager.
func (m *InvertedIndexManager) ApplySnapshot(ctx context.Context, snapshotID string, chunkChan <-chan []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var receivedBytes []byte
	for chunk := range chunkChan {
		receivedBytes = append(receivedBytes, chunk...)
	}

	var snapshotContent map[string][]string // Simplified snapshot content
	if err := json.Unmarshal(receivedBytes, &snapshotContent); err != nil {
		return fmt.Errorf("failed to unmarshal received inverted index snapshot: %v", err)
	}

	// Rebuild the inverted index from the snapshot data
	// m.index = inverted_index.NewInvertedIndex() // Clear existing
	// // Assuming InvertedIndex has a method to load state
	// m.index.LoadIndexState(snapshotContent) // Dummy method call
	m.latestLSN = 0 // LSN would be part of snapshot or determined after replay
	log.Printf("InvertedIndexManager: Applied snapshot %s", snapshotID)
	return nil
}

func (m *InvertedIndexManager) GetLatestLSN() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.latestLSN
}

// ApplyLogRecord for InvertedIndexManager.
func (m *InvertedIndexManager) ApplyLogRecord(ctx context.Context, entry *wal.LogRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if entry.LogType != wal.LogTypeInvertedIndex {
		return fmt.Errorf("InvertedIndexManager cannot apply log entry for index type %v", entry.LogType)
	}
	switch entry.Type {
	// case "ADD_DOC":
	// 	err := m.index.AddDocument(entry.DocID, entry.Content)
	// 	if err == nil {
	// 		m.latestLSN++
	// 	}
	// 	return err
	case wal.LogRecordTypeInsertKey: // Assuming you add a DeleteDocument method to inverted_index
		// 	// err := m.index.DeleteDocument(entry.DocID)
		// 	// if err == nil {
		// 	// 	m.latestLSN++
		// 	// }
		// 	// return err
		return fmt.Errorf("DeleteDocument not yet implemented for InvertedIndexManager")
	default:
		return fmt.Errorf("unsupported log entry type for InvertedIndexManager: %v", entry.Type)
	}
}

// StartMetricsAndTrace begins the telemetry recording for a gRPC method.
// It returns a new context, the trace span, and the start time.
func (s *InvertedIndexManager) StartMetricsAndTrace(ctx context.Context, fullMethodName string) (context.Context, trace.Span, time.Time) {
	startTime := time.Now()

	// Increment active RPCs and started counter
	s.metrics.ActiveRpcsUpDownCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("grpc.service", s.serviceName),
		attribute.String("grpc.method", fullMethodName),
	))
	s.metrics.RpcsStartedCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("grpc.service", s.serviceName),
		attribute.String("grpc.method", fullMethodName),
	))

	// Start a new trace span
	ctx, span := s.tracer.Start(ctx, fullMethodName, trace.WithAttributes(
		attribute.String("grpc.service", s.serviceName),
		attribute.String("grpc.method", fullMethodName),
	))

	return ctx, span, startTime
}

// EndMetricsAndTrace completes the telemetry recording for a gRPC method.
func (s *InvertedIndexManager) EndMetricsAndTrace(ctx context.Context, span trace.Span, startTime time.Time, fullMethodName string, statusCode otelcodes.Code) {
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
		attribute.String("grpc.service", s.serviceName),
		attribute.String("grpc.method", fullMethodName),
	))

	// Define attributes for the completed RPC.
	metricAttributes := attribute.NewSet(
		attribute.String("grpc.service", s.serviceName),
		attribute.String("grpc.method", fullMethodName),
		attribute.String("grpc.code", statusCode.String()),
	)

	// Record latency and increment handled counter
	s.metrics.RpcLatencyHistogram.Record(ctx, latency, metric.WithAttributeSet(metricAttributes))
	s.metrics.RpcsHandledCounter.Add(ctx, 1, metric.WithAttributeSet(metricAttributes))
}
